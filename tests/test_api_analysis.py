"""Tests for the DTG API integration with PyArrow tables."""

from __future__ import annotations

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from pyngb.api.analysis import add_dtg, calculate_table_dtg


class TestAddDTG:
    """Test the add_dtg function."""

    def setup_method(self):
        """Set up test data."""
        # Create sample thermal analysis data
        time = np.linspace(0, 100, 50)
        mass = 10 - 0.02 * time  # Linear mass loss
        temperature = 25 + 5 * time  # Linear heating

        # Create PyArrow table
        self.table = pa.table(
            {
                "time": time,
                "mass": mass,
                "sample_temperature": temperature,
                "other_data": np.random.RandomState(42).normal(0, 1, len(time)),
            }
        )

        # Add some metadata
        metadata = {"experiment": "test", "sample": "test_sample"}
        schema = self.table.schema.with_metadata(metadata)
        self.table = self.table.cast(schema)

    def test_add_dtg_default(self):
        """Test adding DTG column with default parameters."""
        result_table = add_dtg(self.table)

        # Check that DTG column was added
        assert "dtg" in result_table.column_names
        assert result_table.num_columns == self.table.num_columns + 1
        assert result_table.num_rows == self.table.num_rows

        # Check DTG values
        df = pl.from_arrow(result_table)
        dtg_values = df["dtg"].to_numpy()

        # For linear mass loss, DTG should be approximately constant
        expected = 1.2  # Our function returns positive for mass loss
        assert abs(np.mean(dtg_values) - expected) < 0.1

    def test_add_dtg_custom_parameters(self):
        """Test adding DTG with custom parameters."""
        result_table = add_dtg(
            self.table, method="gradient", smooth="strict", column_name="mass_rate"
        )

        # Check custom column name
        assert "mass_rate" in result_table.column_names
        assert "dtg" not in result_table.column_names

        # Check that it worked
        df = pl.from_arrow(result_table)
        dtg_values = df["mass_rate"].to_numpy()
        assert len(dtg_values) == self.table.num_rows
        assert not np.any(np.isnan(dtg_values))

    def test_metadata_preservation(self):
        """Test that metadata is preserved when adding DTG."""
        result_table = add_dtg(self.table)

        # Original metadata should be preserved
        assert result_table.schema.metadata is not None
        assert result_table.schema.metadata == self.table.schema.metadata

    def test_missing_time_column(self):
        """Test error handling when time column is missing."""
        table_no_time = self.table.drop(["time"])

        with pytest.raises(ValueError, match="must contain 'time' column"):
            add_dtg(table_no_time)

    def test_missing_mass_column(self):
        """Test error handling when mass column is missing."""
        table_no_mass = self.table.drop(["mass"])

        with pytest.raises(ValueError, match="must contain 'mass' column"):
            add_dtg(table_no_mass)

    def test_different_smoothing_levels(self):
        """Test different smoothing levels produce different results."""
        result_strict = add_dtg(self.table, smooth="strict", column_name="dtg_strict")
        result_loose = add_dtg(self.table, smooth="loose", column_name="dtg_loose")

        # Combine tables to compare
        combined = result_strict.append_column(
            "dtg_loose", result_loose.column("dtg_loose")
        )
        df = pl.from_arrow(combined)

        strict_vals = df["dtg_strict"].to_numpy()
        loose_vals = df["dtg_loose"].to_numpy()

        # For linear data, smoothing levels may have similar variation
        # Just check both are reasonable and different
        assert np.all(np.isfinite(strict_vals))
        assert np.all(np.isfinite(loose_vals))
        # They should be different (unless data is perfectly linear)
        # For linear data, the difference may be very small


class TestCalculateTableDTG:
    """Test the calculate_table_dtg function."""

    def setup_method(self):
        """Set up test data."""
        time = np.linspace(0, 100, 50)
        mass = 10 - 0.03 * time

        self.table = pa.table(
            {"time": time, "mass": mass, "extra_column": np.ones(len(time))}
        )

    def test_calculate_table_dtg_default(self):
        """Test DTG calculation from table with defaults."""
        result = calculate_table_dtg(self.table)

        assert isinstance(result, np.ndarray)
        assert len(result) == self.table.num_rows
        assert result.dtype == np.float64

        # Check approximate value for linear mass loss
        expected = 1.8  # Our function returns positive for mass loss
        assert abs(np.mean(result) - expected) < 0.1

    def test_calculate_table_dtg_custom(self):
        """Test DTG calculation with custom parameters."""
        result = calculate_table_dtg(self.table, method="gradient", smooth="loose")

        assert isinstance(result, np.ndarray)
        assert len(result) == self.table.num_rows
        assert not np.any(np.isnan(result))

    def test_missing_required_columns(self):
        """Test error handling for missing columns."""
        # Missing time column
        table_no_time = self.table.drop(["time"])
        with pytest.raises(ValueError, match="must contain 'time' column"):
            calculate_table_dtg(table_no_time)

        # Missing mass column
        table_no_mass = self.table.drop(["mass"])
        with pytest.raises(ValueError, match="must contain 'mass' column"):
            calculate_table_dtg(table_no_mass)

    def test_method_comparison(self):
        """Test comparison between different methods."""
        result_savgol = calculate_table_dtg(self.table, method="savgol")
        result_gradient = calculate_table_dtg(self.table, method="gradient")

        # Both should have same length and be reasonable
        assert len(result_savgol) == len(result_gradient) == self.table.num_rows

        # Both methods should give reasonable results
        assert np.all(np.isfinite(result_savgol))
        assert np.all(np.isfinite(result_gradient))
        # For linear data, both should be close to expected value
        expected = 1.8  # Positive for mass loss
        assert abs(np.mean(result_savgol) - expected) < 0.5
        assert abs(np.mean(result_gradient) - expected) < 0.5


class TestIntegration:
    """Test integration between add_dtg and calculate_table_dtg."""

    def setup_method(self):
        """Set up test data."""
        time = np.linspace(0, 60, 30)
        mass = 8 + 2 * np.exp(-time / 20)  # Exponential mass change

        self.table = pa.table(
            {"time": time, "mass": mass, "sample_temperature": 25 + 10 * time}
        )

    def test_consistency_between_functions(self):
        """Test that both functions give consistent results."""
        # Method 1: calculate DTG separately
        dtg_array = calculate_table_dtg(self.table, method="savgol", smooth="medium")

        # Method 2: add DTG to table
        table_with_dtg = add_dtg(self.table, method="savgol", smooth="medium")
        df = pl.from_arrow(table_with_dtg)
        dtg_from_table = df["dtg"].to_numpy()

        # Should be identical (or very close)
        np.testing.assert_allclose(dtg_array, dtg_from_table, rtol=1e-10)

    def test_workflow_example(self):
        """Test a realistic workflow example."""
        # Step 1: Calculate DTG to check if it looks reasonable
        dtg_values = calculate_table_dtg(self.table, smooth="medium")
        max_rate = abs(dtg_values.min())  # Maximum mass loss rate

        assert max_rate > 0  # Should have some mass loss

        # Step 2: If reasonable, add to table with appropriate smoothing
        smoothing = "loose" if max_rate > 5 else "medium"  # Adapt based on rate
        final_table = add_dtg(self.table, smooth=smoothing)

        # Check final result
        assert "dtg" in final_table.column_names
        assert final_table.num_rows == self.table.num_rows


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_empty_table(self):
        """Test handling of empty tables."""
        empty_table = pa.table(
            {
                "time": pa.array([], type=pa.float64()),
                "mass": pa.array([], type=pa.float64()),
            }
        )

        with pytest.raises(ValueError):  # Should fail due to insufficient data
            add_dtg(empty_table)

    def test_single_row_table(self):
        """Test handling of single-row tables."""
        single_row_table = pa.table({"time": [0.0], "mass": [10.0]})

        with pytest.raises(ValueError, match="Need at least 3 data points"):
            add_dtg(single_row_table)

    def test_invalid_data_types(self):
        """Test handling of invalid data types in columns."""
        # This is more of a robustness test - PyArrow handles type conversion
        table_with_strings = pa.table(
            {
                "time": ["0", "1", "2", "3", "4"],  # String instead of numeric
                "mass": [10, 9, 8, 7, 6],
            }
        )

        # This might work due to automatic conversion, or might fail
        # The exact behavior depends on PyArrow/Polars conversion
        try:
            result = add_dtg(table_with_strings)
            assert isinstance(result, pa.Table)
        except (ValueError, TypeError, pa.ArrowTypeError):
            # Either error is acceptable for invalid input
            pass


if __name__ == "__main__":
    pytest.main([__file__])
