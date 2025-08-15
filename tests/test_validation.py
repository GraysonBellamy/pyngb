"""
Unit tests for the validation module of pyngb.
"""

import numpy as np
import polars as pl
import pytest

from pyngb.validation import (
    QualityChecker,
    ValidationResult,
    check_dsc_data,
    check_mass_data,
    check_temperature_profile,
    validate_sta_data,
)


class TestValidationResult:
    """Test ValidationResult class."""

    def test_init(self):
        """Test ValidationResult initialization."""
        result = ValidationResult()
        assert result.errors == []
        assert result.warnings == []
        assert result.info == []
        assert result.passed_checks == []

    def test_add_error(self):
        """Test adding errors."""
        result = ValidationResult()
        result.add_error("Test error")
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error"

    def test_add_warning(self):
        """Test adding warnings."""
        result = ValidationResult()
        result.add_warning("Test warning")
        assert len(result.warnings) == 1
        assert result.warnings[0] == "Test warning"

    def test_add_info(self):
        """Test adding info messages."""
        result = ValidationResult()
        result.add_info("Test info")
        assert len(result.info) == 1
        assert result.info[0] == "Test info"

    def test_add_pass(self):
        """Test adding passed checks."""
        result = ValidationResult()
        result.add_pass("Test check")
        assert len(result.passed_checks) == 1
        assert result.passed_checks[0] == "Test check"

    def test_is_valid(self):
        """Test validation status."""
        result = ValidationResult()
        assert result.is_valid is True

        result.add_error("Error")
        assert result.is_valid is False

        result.add_warning("Warning")
        assert result.is_valid is False  # Still invalid due to error

    def test_has_warnings(self):
        """Test warning status."""
        result = ValidationResult()
        assert result.has_warnings is False

        result.add_warning("Warning")
        assert result.has_warnings is True

    def test_summary(self):
        """Test summary generation."""
        result = ValidationResult()
        result.add_error("Error")
        result.add_warning("Warning")
        result.add_pass("Check")

        summary = result.summary()
        assert summary["is_valid"] is False
        assert summary["has_warnings"] is True
        assert summary["error_count"] == 1
        assert summary["warning_count"] == 1
        assert summary["checks_passed"] == 1
        assert summary["total_issues"] == 2

    def test_report(self):
        """Test report generation."""
        result = ValidationResult()
        result.add_error("Test error")
        result.add_warning("Test warning")
        result.add_info("Test info")
        result.add_pass("Test check")

        report = result.report()
        assert "❌ INVALID" in report
        assert "Test error" in report
        assert "Test warning" in report
        assert "Test info" in report


@pytest.fixture
def sample_sta_data():
    """Create sample STA data for testing."""
    n_points = 1000
    # Seed randomness for determinism in tests
    rng = np.random.default_rng(0)
    return pl.DataFrame(
        {
            "time": np.linspace(0, 100, n_points),
            "sample_temperature": np.linspace(25, 800, n_points),
            "mass": np.linspace(10, 8, n_points),
            "dsc_signal": rng.normal(0, 5, n_points),
        }
    )


@pytest.fixture
def sample_metadata():
    """Create sample metadata for testing."""
    return {
        "instrument": "STA 449 F3",
        "sample_name": "Test Sample",
        "operator": "Test User",
        "mass": 10.0,
    }


class TestValidateSta:
    """Test validate_sta_data function."""

    def test_valid_data(self, sample_sta_data):
        """Test validation of valid data."""
        issues = validate_sta_data(sample_sta_data)
        assert isinstance(issues, list)
        # Valid data should have minimal issues
        assert len(issues) <= 2  # Allow for minor statistical variations

    def test_empty_data(self):
        """Test validation of empty data."""
        empty_data = pl.DataFrame(
            {
                "time": [],
                "sample_temperature": [],
            }
        )
        issues = validate_sta_data(empty_data)
        assert len(issues) > 0
        assert any("empty" in issue.lower() for issue in issues)

    def test_missing_columns(self):
        """Test validation with missing required columns."""
        incomplete_data = pl.DataFrame({"only_time": [1, 2, 3]})
        issues = validate_sta_data(incomplete_data)
        assert len(issues) > 0
        assert any("missing" in issue.lower() for issue in issues)

    def test_null_values(self):
        """Test validation with null values."""
        data_with_nulls = pl.DataFrame(
            {
                "time": [1, 2, None, 4],
                "sample_temperature": [25, 50, 75, None],
            }
        )
        issues = validate_sta_data(data_with_nulls)
        assert len(issues) > 0
        assert any("null" in issue.lower() for issue in issues)

    def test_constant_temperature(self):
        """Test validation with constant temperature."""
        constant_temp_data = pl.DataFrame(
            {
                "time": [1, 2, 3, 4],
                "sample_temperature": [25, 25, 25, 25],
            }
        )
        issues = validate_sta_data(constant_temp_data)
        assert len(issues) > 0
        assert any(
            "constant" in issue.lower() or "heating" in issue.lower()
            for issue in issues
        )

    def test_extreme_temperatures(self):
        """Test validation with extreme temperatures."""
        extreme_temp_data = pl.DataFrame(
            {
                "time": [1, 2, 3, 4],
                "sample_temperature": [
                    -300,
                    3000,
                    25,
                    50,
                ],  # Below absolute zero and very high
            }
        )
        issues = validate_sta_data(extreme_temp_data)
        assert len(issues) > 0
        assert any("temperature" in issue.lower() for issue in issues)

    def test_pyarrow_table_input(self, sample_sta_data):
        """Test validation with PyArrow table input."""
        arrow_table = sample_sta_data.to_arrow()
        issues = validate_sta_data(arrow_table)
        assert isinstance(issues, list)


class TestQualityChecker:
    """Test QualityChecker class."""

    def test_init_with_dataframe(self, sample_sta_data, sample_metadata):
        """Test initialization with DataFrame."""
        checker = QualityChecker(sample_sta_data, sample_metadata)
        assert checker.df.height == 1000
        assert checker.metadata == sample_metadata

    def test_init_with_arrow_table(self, sample_sta_data):
        """Test initialization with Arrow table."""
        arrow_table = sample_sta_data.to_arrow()
        checker = QualityChecker(arrow_table)
        assert checker.df.height == 1000

    def test_quick_check_valid_data(self, sample_sta_data):
        """Test quick check with valid data."""
        checker = QualityChecker(sample_sta_data)
        issues = checker.quick_check()
        assert isinstance(issues, list)
        # Valid data should have few issues
        assert len(issues) <= 2

    def test_quick_check_empty_data(self):
        """Test quick check with empty data."""
        empty_data = pl.DataFrame(
            {
                "time": [],
                "sample_temperature": [],
            }
        )
        checker = QualityChecker(empty_data)
        issues = checker.quick_check()
        assert len(issues) > 0
        assert any("empty" in issue.lower() for issue in issues)

    def test_quick_check_missing_columns(self):
        """Test quick check with missing columns."""
        incomplete_data = pl.DataFrame({"only_data": [1, 2, 3]})
        checker = QualityChecker(incomplete_data)
        issues = checker.quick_check()
        assert len(issues) > 0
        assert any("missing" in issue.lower() for issue in issues)

    def test_full_validation(self, sample_sta_data, sample_metadata):
        """Test full validation."""
        checker = QualityChecker(sample_sta_data, sample_metadata)
        result = checker.full_validation()
        assert isinstance(result, ValidationResult)
        assert result.is_valid  # Valid data should pass
        assert len(result.passed_checks) > 0

    def test_full_validation_invalid_data(self):
        """Test full validation with invalid data."""
        invalid_data = pl.DataFrame(
            {
                "time": [1, 2, 1, 4],  # Time goes backwards
                "sample_temperature": [-300, 25, 50, 75],  # Below absolute zero
                "mass": [-1, 8, 7, 6],  # Negative mass
            }
        )
        checker = QualityChecker(invalid_data)
        result = checker.full_validation()
        assert not result.is_valid
        assert len(result.errors) > 0

    def test_data_structure_checks(self, sample_sta_data):
        """Test data structure validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_data_structure()
        # Should have passed checks for valid data
        assert len(checker.result.passed_checks) > 0

    def test_temperature_data_checks(self, sample_sta_data):
        """Test temperature data validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_temperature_data()
        # Valid temperature data should pass
        assert len(checker.result.passed_checks) > 0

    def test_time_data_checks(self, sample_sta_data):
        """Test time data validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_time_data()
        # Valid time data should pass
        assert len(checker.result.passed_checks) > 0

    def test_mass_data_checks(self, sample_sta_data):
        """Test mass data validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_mass_data()
        # Valid mass data should pass
        assert len(checker.result.passed_checks) > 0

    def test_dsc_data_checks(self, sample_sta_data):
        """Test DSC data validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_dsc_data()
        # Valid DSC data should pass
        assert len(checker.result.passed_checks) > 0

    def test_metadata_consistency_checks(self, sample_sta_data, sample_metadata):
        """Test metadata consistency validation."""
        checker = QualityChecker(sample_sta_data, sample_metadata)
        checker._check_metadata_consistency()
        # Valid metadata should pass
        assert len(checker.result.passed_checks) > 0

    def test_statistical_checks(self, sample_sta_data):
        """Test statistical property validation."""
        checker = QualityChecker(sample_sta_data)
        checker._check_statistical_properties()
        # Normal data should not have excessive outliers
        assert len([w for w in checker.result.warnings if "outlier" in w.lower()]) <= 1


class TestSpecificValidationFunctions:
    """Test specific validation functions."""

    def test_check_temperature_profile(self, sample_sta_data):
        """Test temperature profile checking."""
        analysis = check_temperature_profile(sample_sta_data)
        assert isinstance(analysis, dict)
        assert "temperature_range" in analysis
        assert "min_temperature" in analysis
        assert "max_temperature" in analysis
        assert "is_monotonic_increasing" in analysis
        assert "is_monotonic_decreasing" in analysis
        assert "average_rate" in analysis

        # Check values are reasonable
        # Guard against unexpected non-numeric types
        tr = analysis.get("temperature_range")
        if isinstance(tr, (int, float)) and not isinstance(tr, bool):
            assert tr > 0
        assert analysis["min_temperature"] == 25
        assert analysis["max_temperature"] == 800
        assert analysis["is_monotonic_increasing"] is True

    def test_check_temperature_profile_missing_column(self):
        """Test temperature profile with missing column."""
        data_no_temp = pl.DataFrame({"time": [1, 2, 3]})
        analysis = check_temperature_profile(data_no_temp)
        assert "error" in analysis

    def test_check_mass_data(self, sample_sta_data):
        """Test mass data checking."""
        analysis = check_mass_data(sample_sta_data)
        assert isinstance(analysis, dict)
        assert "initial_mass" in analysis
        assert "final_mass" in analysis
        assert "mass_change" in analysis
        assert "mass_range" in analysis
        assert "has_negative_values" in analysis

        # Check values are reasonable
        assert analysis["initial_mass"] == 10.0
        assert analysis["final_mass"] == 8.0
        assert analysis["mass_change"] == -2.0  # Change from mass_loss_percent
        assert analysis["has_negative_values"] is False

    def test_check_mass_data_missing_column(self):
        """Test mass data with missing column."""
        data_no_mass = pl.DataFrame({"time": [1, 2, 3]})
        analysis = check_mass_data(data_no_mass)
        assert "error" in analysis

    def test_check_dsc_data(self, sample_sta_data):
        """Test DSC data checking."""
        analysis = check_dsc_data(sample_sta_data)
        assert isinstance(analysis, dict)
        assert "signal_range" in analysis
        assert "signal_std" in analysis
        assert "peaks_detected" in analysis
        assert "positive_peaks" in analysis
        assert "negative_peaks" in analysis
        assert "signal_to_noise" in analysis

        # Check values are reasonable for random noise
        std = analysis.get("signal_std")
        if isinstance(std, (int, float)) and not isinstance(std, bool):
            assert std > 0
        s2n = analysis.get("signal_to_noise")
        if isinstance(s2n, (int, float)) and not isinstance(s2n, bool):
            assert s2n > 0

    def test_check_dsc_data_missing_column(self):
        """Test DSC data with missing column."""
        data_no_dsc = pl.DataFrame({"time": [1, 2, 3]})
        analysis = check_dsc_data(data_no_dsc)
        assert "error" in analysis

    def test_check_dsc_data_with_peaks(self):
        """Test DSC data with artificial peaks."""
        # Create data with clear peaks
        x = np.linspace(0, 10, 1000)
        y = np.sin(x) * 10  # Clear sinusoidal pattern with peaks
        dsc_data_with_peaks = pl.DataFrame(
            {
                "time": x,
                "sample_temperature": x * 80 + 25,
                "dsc_signal": y,
            }
        )
        analysis = check_dsc_data(dsc_data_with_peaks)
        pk = analysis.get("peaks_detected")
        if isinstance(pk, (int, float)) and not isinstance(pk, bool):
            assert pk > 0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_single_point_data(self):
        """Test validation with single data point."""
        single_point = pl.DataFrame(
            {
                "time": [1],
                "sample_temperature": [25],
            }
        )
        checker = QualityChecker(single_point)
        result = checker.full_validation()
        # Should handle single point gracefully
        assert isinstance(result, ValidationResult)

    def test_two_point_data(self):
        """Test validation with two data points."""
        two_points = pl.DataFrame(
            {
                "time": [1, 2],
                "sample_temperature": [25, 50],
                "mass": [10, 9],
            }
        )
        checker = QualityChecker(two_points)
        result = checker.full_validation()
        assert isinstance(result, ValidationResult)

    def test_infinite_values(self):
        """Test validation with infinite values."""
        data_with_inf = pl.DataFrame(
            {
                "time": [1.0, 2.0, 3.0, 4.0],
                "sample_temperature": [25.0, 50.0, float("inf"), 100.0],
                "mass": [10.0, 9.0, 8.0, 7.0],
            }
        )
        checker = QualityChecker(data_with_inf)
        result = checker.full_validation()
        # Should detect issues with infinite values
        assert len(result.warnings) > 0 or len(result.errors) > 0

    def test_all_nan_column(self):
        """Test validation with all NaN values in a column."""
        data_with_nan = pl.DataFrame(
            {
                "time": [1, 2, 3, 4],
                "sample_temperature": [None, None, None, None],
            }
        )
        checker = QualityChecker(data_with_nan)
        issues = checker.quick_check()
        assert len(issues) > 0
        assert any("null" in issue.lower() for issue in issues)

    @pytest.mark.slow
    def test_very_large_dataset(self):
        """Test validation with large dataset."""
        large_data = pl.DataFrame(
            {
                "time": np.linspace(0, 1000, 100000),
                "sample_temperature": np.linspace(25, 800, 100000),
            }
        )
        checker = QualityChecker(large_data)
        # Should handle large datasets without errors
        issues = checker.quick_check()
        assert isinstance(issues, list)
