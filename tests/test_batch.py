"""
Integration tests for batch processing functionality.

Tests the batch processing module along with its integration with validation.
"""

import tempfile

import numpy as np
import polars as pl
import pytest

from pynetzsch.batch import BatchProcessor
from pynetzsch.validation import QualityChecker, ValidationResult, validate_sta_data


@pytest.fixture
def sample_sta_data():
    """Create sample STA data for testing."""
    n_points = 100
    return pl.DataFrame(
        {
            "time": np.linspace(0, 100, n_points),
            "temperature": np.linspace(25, 800, n_points),
            "sample_mass": np.linspace(10, 8, n_points),
            "dsc": np.random.normal(0, 2, n_points),
        }
    )


class TestValidationModule:
    """Test validation module."""

    def test_validate_sta_data_valid(self, sample_sta_data):
        """Test validation of valid STA data."""
        issues = validate_sta_data(sample_sta_data)
        assert isinstance(issues, list)
        assert len(issues) == 0  # Valid data should have no issues

    def test_validate_sta_data_invalid(self):
        """Test validation of problematic STA data."""
        invalid_data = pl.DataFrame(
            {
                "time": [1, 2, 1, 4],  # Time goes backwards
                "temperature": [-300, 25, 50, 75],  # Below absolute zero
            }
        )
        issues = validate_sta_data(invalid_data)
        assert len(issues) > 0

    def test_quality_checker_basic(self, sample_sta_data):
        """Test basic QualityChecker functionality."""
        checker = QualityChecker(sample_sta_data)
        result = checker.full_validation()
        assert isinstance(result, ValidationResult)
        assert result.is_valid

    def test_validation_result_methods(self):
        """Test ValidationResult methods."""
        result = ValidationResult()
        result.add_error("Test error")
        result.add_warning("Test warning")
        result.add_pass("Test pass")

        assert not result.is_valid
        assert result.has_warnings
        summary = result.summary()
        assert summary["error_count"] == 1
        assert summary["warning_count"] == 1

    def test_empty_data_validation(self):
        """Test validation of empty data."""
        empty_data = pl.DataFrame({"time": [], "temperature": []})
        issues = validate_sta_data(empty_data)
        assert len(issues) > 0


class TestBatchProcessor:
    """Test batch processing module."""

    def test_batch_processor_init(self):
        """Test BatchProcessor initialization."""
        processor = BatchProcessor()
        assert processor.max_workers is None  # Default is None
        assert processor.verbose is True  # Default is True

    def test_batch_processor_custom(self):
        """Test BatchProcessor with custom parameters."""
        processor = BatchProcessor(max_workers=2, verbose=True)
        assert processor.max_workers == 2
        assert processor.verbose is True

    def test_empty_file_processing(self):
        """Test processing empty file list."""
        processor = BatchProcessor()
        with tempfile.TemporaryDirectory() as temp_dir:
            results = processor.process_files([], output_dir=temp_dir)
            assert len(results) == 0

    def test_nonexistent_directory(self):
        """Test processing non-existent directory."""
        processor = BatchProcessor()
        with pytest.raises(FileNotFoundError):
            processor.process_directory("/nonexistent/directory")


class TestSpecialScenarios:
    """Test special data quality scenarios."""

    def test_constant_temperature(self):
        """Test constant temperature detection."""
        constant_temp = pl.DataFrame(
            {
                "time": [1, 2, 3, 4],
                "temperature": [25, 25, 25, 25],
            }
        )
        issues = validate_sta_data(constant_temp)
        assert any(
            "constant" in issue.lower() or "heating" in issue.lower()
            for issue in issues
        )

    def test_null_values(self):
        """Test null value detection."""
        data_with_nulls = pl.DataFrame(
            {
                "time": [1, 2, None, 4],
                "temperature": [25, 50, 75, None],
            }
        )
        issues = validate_sta_data(data_with_nulls)
        assert any("null" in issue.lower() for issue in issues)

    def test_arrow_table_support(self, sample_sta_data):
        """Test PyArrow table support."""
        arrow_table = sample_sta_data.to_arrow()
        issues = validate_sta_data(arrow_table)
        assert isinstance(issues, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
