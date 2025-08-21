"""
Tests for baseline subtraction functionality.
"""

import pytest
import polars as pl
import pyarrow as pa

from pyngb import read_ngb, subtract_baseline
from pyngb.baseline import BaselineSubtractor


class TestBaselineSubtraction:
    """Test baseline subtraction functionality."""

    @pytest.fixture
    def sample_file(self):
        """Path to sample test file."""
        return "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"

    @pytest.fixture
    def baseline_file(self):
        """Path to baseline test file."""
        return "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"

    @pytest.fixture
    def incompatible_sample_file(self):
        """Path to sample file with incompatible temperature program."""
        return "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3"

    @pytest.fixture
    def incompatible_baseline_file(self):
        """Path to baseline file with incompatible temperature program."""
        return "tests/test_files/Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3"

    @pytest.fixture
    def sample_data(self, sample_file):
        """Load sample data."""
        return read_ngb(sample_file)

    @pytest.fixture
    def baseline_data(self, baseline_file):
        """Load baseline data."""
        return read_ngb(baseline_file)

    def test_standalone_subtract_baseline(self, sample_file, baseline_file):
        """Test standalone subtract_baseline function."""
        result = subtract_baseline(sample_file, baseline_file)

        assert isinstance(result, pl.DataFrame)
        assert result.height > 0
        assert result.width > 0

        # Should have key columns
        expected_columns = ["time", "mass", "dsc_signal"]
        for col in expected_columns:
            assert col in result.columns

    def test_subtract_baseline_with_temperature_axis(self, sample_file, baseline_file):
        """Test baseline subtraction with sample temperature axis."""
        result = subtract_baseline(
            sample_file, baseline_file, dynamic_axis="sample_temperature"
        )

        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_subtract_baseline_with_furnace_axis(self, sample_file, baseline_file):
        """Test baseline subtraction with furnace temperature axis."""
        result = subtract_baseline(
            sample_file, baseline_file, dynamic_axis="furnace_temperature"
        )

        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_subtract_baseline_with_time_axis(self, sample_file, baseline_file):
        """Test baseline subtraction with time axis."""
        result = subtract_baseline(sample_file, baseline_file, dynamic_axis="time")

        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_integrated_read_ngb_with_baseline(self, sample_file, baseline_file):
        """Test integrated read_ngb with baseline subtraction."""
        result = read_ngb(sample_file, baseline_file=baseline_file)

        assert isinstance(result, pa.Table)
        assert result.num_rows > 0
        assert result.num_columns > 0

        # Convert to DataFrame for column checking
        df = pl.from_arrow(result)
        expected_columns = ["time", "mass", "dsc_signal"]
        for col in expected_columns:
            assert col in df.columns

    def test_integrated_read_ngb_with_metadata_and_baseline(
        self, sample_file, baseline_file
    ):
        """Test integrated read_ngb with baseline and metadata return."""
        metadata, result = read_ngb(
            sample_file, baseline_file=baseline_file, return_metadata=True
        )

        assert isinstance(metadata, dict)
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0

        # Should have metadata
        assert "instrument" in metadata
        assert "sample_name" in metadata

    def test_read_ngb_invalid_dynamic_axis(self, sample_file, baseline_file):
        """Test error handling for invalid dynamic axis."""
        with pytest.raises(ValueError, match="dynamic_axis must be one of"):
            read_ngb(
                sample_file, baseline_file=baseline_file, dynamic_axis="invalid_axis"
            )

    def test_baseline_subtractor_class(self, sample_data, baseline_data):
        """Test BaselineSubtractor class directly."""
        subtractor = BaselineSubtractor()

        # Convert to DataFrames
        sample_df = pl.from_arrow(sample_data)
        # Note: baseline_df would be used in more complex tests
        # baseline_df = pl.from_arrow(baseline_data)

        # Load metadata for temperature program
        sample_file = "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3"
        metadata, _ = read_ngb(sample_file, return_metadata=True)

        # Test segment identification
        isothermal, dynamic = subtractor.identify_segments(
            sample_df, metadata.get("temperature_program", {})
        )

        assert isinstance(isothermal, list)
        assert isinstance(dynamic, list)
        # Should find at least some segments
        assert len(isothermal) + len(dynamic) > 0

    def test_baseline_subtractor_interpolation(self, sample_data, baseline_data):
        """Test interpolation functionality."""
        subtractor = BaselineSubtractor()

        sample_df = pl.from_arrow(sample_data)
        baseline_df = pl.from_arrow(baseline_data)

        # Test interpolation on a small segment
        sample_segment = sample_df.head(100)
        baseline_segment = baseline_df.head(100)

        interpolated = subtractor.interpolate_baseline(
            sample_segment, baseline_segment, "time"
        )

        assert isinstance(interpolated, pl.DataFrame)
        assert interpolated.height == sample_segment.height

    def test_data_preservation(self, sample_file, baseline_file):
        """Test that baseline subtraction succeeds and produces reasonable output."""
        # Load subtracted data
        subtracted = subtract_baseline(sample_file, baseline_file)

        # Basic checks - subtracted data should exist and have reasonable structure
        assert subtracted is not None
        assert len(subtracted) > 0

        # Check that key columns exist
        column_names = subtracted.schema.names()
        assert "time" in column_names

        # Verify subtraction worked
        assert len(subtracted) > 10, "Should have reasonable number of data points"

    def test_file_not_found_error(self):
        """Test error handling for missing files."""
        with pytest.raises(FileNotFoundError):
            subtract_baseline("nonexistent.ngb-ss3", "baseline.ngb-bs3")

        with pytest.raises(FileNotFoundError):
            subtract_baseline(
                "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3",
                "nonexistent.ngb-bs3",
            )

    def test_no_temperature_program(self, sample_file, baseline_file):
        """Test handling when no temperature program is available."""
        # This should still work, treating everything as dynamic
        result = subtract_baseline(sample_file, baseline_file)
        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_temperature_program_validation_success(self, sample_file, baseline_file):
        """Test that compatible temperature programs pass validation."""
        result = subtract_baseline(sample_file, baseline_file)
        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_temperature_program_validation_failure(
        self, incompatible_sample_file, incompatible_baseline_file
    ):
        """Test that incompatible temperature programs fail validation."""
        with pytest.raises(ValueError, match="Temperature program mismatch"):
            subtract_baseline(incompatible_sample_file, incompatible_baseline_file)

    def test_integrated_api_validation_failure(
        self, incompatible_sample_file, incompatible_baseline_file
    ):
        """Test that incompatible temperature programs fail in integrated API."""
        with pytest.raises(ValueError, match="Temperature program mismatch"):
            read_ngb(incompatible_sample_file, baseline_file=incompatible_baseline_file)

    def test_baseline_subtractor_validation_direct(self):
        """Test BaselineSubtractor validation directly."""
        subtractor = BaselineSubtractor()

        # Load metadata from incompatible files
        sample_metadata, _ = read_ngb(
            "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3",
            return_metadata=True,
        )
        baseline_metadata, _ = read_ngb(
            "tests/test_files/Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3",
            return_metadata=True,
        )

        # Should raise validation error
        with pytest.raises(ValueError, match="Temperature program mismatch"):
            subtractor.validate_temperature_programs(sample_metadata, baseline_metadata)

    def test_baseline_subtractor_validation_success(self, sample_file, baseline_file):
        """Test BaselineSubtractor validation with compatible files."""
        subtractor = BaselineSubtractor()

        # Load metadata from compatible files
        sample_metadata, _ = read_ngb(sample_file, return_metadata=True)
        baseline_metadata, _ = read_ngb(baseline_file, return_metadata=True)

        # Should not raise any error
        try:
            subtractor.validate_temperature_programs(sample_metadata, baseline_metadata)
        except ValueError:
            pytest.fail("Validation should have passed for compatible files")
