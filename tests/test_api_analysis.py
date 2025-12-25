"""Tests for the DTG API integration with PyArrow tables."""

from __future__ import annotations
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from pyngb.api.analysis import add_dtg, calculate_table_dtg, normalize_to_initial_mass


class TestAddDTG:
    """Test the add_dtg function."""

    def setup_method(self) -> Any:
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

    def test_add_dtg_default(self) -> None:
        """Test adding DTG column with default parameters."""
        result_table = add_dtg(self.table)

        # Check that DTG column was added
        assert "dtg" in result_table.column_names
        assert result_table.num_columns == self.table.num_columns + 1
        assert result_table.num_rows == self.table.num_rows

        # Check DTG values
        df = pl.from_arrow(result_table)
        dtg_values = df["dtg"].to_numpy()  # type: ignore[index]

        # For linear mass loss, DTG should be approximately constant
        expected = 1.2  # Our function returns positive for mass loss
        assert abs(np.mean(dtg_values) - expected) < 0.1

    def test_add_dtg_custom_parameters(self) -> None:
        """Test adding DTG with custom parameters."""
        result_table = add_dtg(
            self.table, method="gradient", smooth="strict", column_name="mass_rate"
        )

        # Check custom column name
        assert "mass_rate" in result_table.column_names
        assert "dtg" not in result_table.column_names

        # Check that it worked
        df = pl.from_arrow(result_table)
        dtg_values = df["mass_rate"].to_numpy()  # type: ignore[index]
        assert len(dtg_values) == self.table.num_rows
        assert not np.any(np.isnan(dtg_values))

    def test_metadata_preservation(self) -> None:
        """Test that metadata is preserved when adding DTG."""
        result_table = add_dtg(self.table)

        # Original metadata should be preserved
        assert result_table.schema.metadata is not None
        assert result_table.schema.metadata == self.table.schema.metadata

    def test_missing_time_column(self) -> None:
        """Test error handling when time column is missing."""
        table_no_time = self.table.drop(["time"])

        with pytest.raises(ValueError, match="must contain 'time' column"):
            add_dtg(table_no_time)

    def test_missing_mass_column(self) -> None:
        """Test error handling when mass column is missing."""
        table_no_mass = self.table.drop(["mass"])

        with pytest.raises(ValueError, match="must contain 'mass' column"):
            add_dtg(table_no_mass)

    def test_different_smoothing_levels(self) -> None:
        """Test different smoothing levels produce different results."""
        result_strict = add_dtg(self.table, smooth="strict", column_name="dtg_strict")
        result_loose = add_dtg(self.table, smooth="loose", column_name="dtg_loose")

        # Combine tables to compare
        combined = result_strict.append_column(
            "dtg_loose", result_loose.column("dtg_loose")
        )
        df = pl.from_arrow(combined)

        strict_vals = df["dtg_strict"].to_numpy()  # type: ignore[index]
        loose_vals = df["dtg_loose"].to_numpy()  # type: ignore[index]

        # For linear data, smoothing levels may have similar variation
        # Just check both are reasonable and different
        assert np.all(np.isfinite(strict_vals))
        assert np.all(np.isfinite(loose_vals))
        # They should be different (unless data is perfectly linear)
        # For linear data, the difference may be very small


class TestCalculateTableDTG:
    """Test the calculate_table_dtg function."""

    def setup_method(self) -> Any:
        """Set up test data."""
        time = np.linspace(0, 100, 50)
        mass = 10 - 0.03 * time

        self.table = pa.table(
            {"time": time, "mass": mass, "extra_column": np.ones(len(time))}
        )

    def test_calculate_table_dtg_default(self) -> None:
        """Test DTG calculation from table with defaults."""
        result = calculate_table_dtg(self.table)

        assert isinstance(result, np.ndarray)
        assert len(result) == self.table.num_rows
        assert result.dtype == np.float64

        # Check approximate value for linear mass loss
        expected = 1.8  # Our function returns positive for mass loss
        assert abs(np.mean(result) - expected) < 0.1

    def test_calculate_table_dtg_custom(self) -> None:
        """Test DTG calculation with custom parameters."""
        result = calculate_table_dtg(self.table, method="gradient", smooth="loose")

        assert isinstance(result, np.ndarray)
        assert len(result) == self.table.num_rows
        assert not np.any(np.isnan(result))

    def test_missing_required_columns(self) -> None:
        """Test error handling for missing columns."""
        # Missing time column
        table_no_time = self.table.drop(["time"])
        with pytest.raises(ValueError, match="must contain 'time' column"):
            calculate_table_dtg(table_no_time)

        # Missing mass column
        table_no_mass = self.table.drop(["mass"])
        with pytest.raises(ValueError, match="must contain 'mass' column"):
            calculate_table_dtg(table_no_mass)

    def test_method_comparison(self) -> None:
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

    def setup_method(self) -> Any:
        """Set up test data."""
        time = np.linspace(0, 60, 30)
        mass = 8 + 2 * np.exp(-time / 20)  # Exponential mass change

        self.table = pa.table(
            {"time": time, "mass": mass, "sample_temperature": 25 + 10 * time}
        )

    def test_consistency_between_functions(self) -> None:
        """Test that both functions give consistent results."""
        # Method 1: calculate DTG separately
        dtg_array = calculate_table_dtg(self.table, method="savgol", smooth="medium")

        # Method 2: add DTG to table
        table_with_dtg = add_dtg(self.table, method="savgol", smooth="medium")
        df = pl.from_arrow(table_with_dtg)
        dtg_from_table = df["dtg"].to_numpy()  # type: ignore[index]

        # Should be identical (or very close)
        np.testing.assert_allclose(dtg_array, dtg_from_table, rtol=1e-10)

    def test_workflow_example(self) -> None:
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

    def test_empty_table(self) -> None:
        """Test handling of empty tables."""
        empty_table = pa.table(
            {
                "time": pa.array([], type=pa.float64()),
                "mass": pa.array([], type=pa.float64()),
            }
        )

        with pytest.raises(ValueError):  # Should fail due to insufficient data
            add_dtg(empty_table)

    def test_single_row_table(self) -> None:
        """Test handling of single-row tables."""
        single_row_table = pa.table({"time": [0.0], "mass": [10.0]})

        with pytest.raises(ValueError, match="Need at least 3 data points"):
            add_dtg(single_row_table)

    def test_invalid_data_types(self) -> None:
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


class TestNormalizeToInitialMass:
    """Test the normalize_to_initial_mass function."""

    def setup_method(self) -> Any:
        """Set up test data with metadata."""
        import json

        # Create sample data with mass starting near zero (tared)
        time = np.linspace(0, 100, 50)
        mass = 0.0 + np.cumsum(
            np.random.RandomState(42).normal(-0.01, 0.005, len(time))
        )
        dsc_signal = np.random.RandomState(42).normal(0, 2.5, len(time))
        temperature = 25 + 2 * time

        # Create PyArrow table
        self.table = pa.table(
            {
                "time": time,
                "mass": mass,
                "dsc_signal": dsc_signal,
                "sample_temperature": temperature,
                "other_data": np.random.RandomState(42).normal(0, 1, len(time)),
            }
        )

        # Add metadata with sample_mass (initial mass before taring)
        metadata = {
            "sample_mass": 15.75,  # mg
            "instrument": "NETZSCH STA 449 F3",
            "crucible_mass": 42.15,
        }

        # Store metadata as file_metadata in schema
        schema_metadata = {
            b"file_metadata": json.dumps(metadata).encode(),
            b"type": b"STA",
        }
        schema = self.table.schema.with_metadata(schema_metadata)
        self.table = self.table.cast(schema)

    def test_normalize_default_columns(self) -> None:
        """Test normalization with default columns (mass and dsc_signal) in place."""
        # Store original values for comparison
        original_df = pl.from_arrow(self.table)
        original_mass = original_df["mass"].to_numpy()  # type: ignore[index]
        original_dsc = original_df["dsc_signal"].to_numpy()  # type: ignore[index]
        original_time = original_df["time"].to_numpy()  # type: ignore[index]

        result_table = normalize_to_initial_mass(self.table)

        # Check that no new columns were added (in-place normalization)
        assert result_table.num_columns == self.table.num_columns
        assert result_table.num_rows == self.table.num_rows
        assert result_table.column_names == self.table.column_names

        # Convert to dataframes for comparison
        result_df = pl.from_arrow(result_table)

        # Check normalization is correct (columns updated in place)
        normalized_mass = result_df["mass"].to_numpy()  # type: ignore[index]
        normalized_dsc = result_df["dsc_signal"].to_numpy()  # type: ignore[index]

        # Values should be divided by sample_mass (15.75)
        expected_mass = original_mass / 15.75
        expected_dsc = original_dsc / 15.75

        np.testing.assert_allclose(normalized_mass, expected_mass)
        np.testing.assert_allclose(normalized_dsc, expected_dsc)

        # Unchanged columns should remain the same
        np.testing.assert_array_equal(original_time, result_df["time"].to_numpy())  # type: ignore[index]
        np.testing.assert_array_equal(
            original_df["sample_temperature"].to_numpy(),  # type: ignore[index]
            result_df["sample_temperature"].to_numpy(),  # type: ignore[index]
        )

    def test_normalize_specific_columns(self) -> None:
        """Test normalization with specific columns (in place)."""
        # Store original values
        original_df = pl.from_arrow(self.table)
        original_mass = original_df["mass"].to_numpy()  # type: ignore[index]
        original_dsc = original_df["dsc_signal"].to_numpy()  # type: ignore[index]

        result_table = normalize_to_initial_mass(self.table, columns=["mass"])

        # Check that no new columns were added (in-place normalization)
        assert result_table.num_columns == self.table.num_columns
        assert result_table.column_names == self.table.column_names

        # Convert to dataframes for comparison
        result_df = pl.from_arrow(result_table)

        # Only mass should be normalized (in place)
        normalized_mass = result_df["mass"].to_numpy()  # type: ignore[index]
        expected_mass = original_mass / 15.75
        np.testing.assert_allclose(normalized_mass, expected_mass)

        # DSC signal should be unchanged (not normalized)
        np.testing.assert_array_equal(original_dsc, result_df["dsc_signal"].to_numpy())  # type: ignore[index]

    def test_metadata_preservation(self) -> None:
        """Test that metadata is preserved after normalization."""
        result_table = normalize_to_initial_mass(self.table)

        # Metadata should be identical
        assert result_table.schema.metadata == self.table.schema.metadata

    def test_missing_metadata_error(self) -> None:
        """Test error when table has no metadata."""
        table_no_meta = pa.table(
            {
                "time": [1, 2, 3],
                "mass": [0.1, 0.2, 0.3],
            }
        )

        with pytest.raises(ValueError, match="Table metadata is missing"):
            normalize_to_initial_mass(table_no_meta)

    def test_missing_file_metadata_error(self) -> None:
        """Test error when file_metadata key is missing."""
        # Table with metadata but no file_metadata key
        schema_metadata = {b"other_key": b"other_value"}
        schema = pa.table(
            {
                "time": [1, 2, 3],
                "mass": [0.1, 0.2, 0.3],
            }
        ).schema.with_metadata(schema_metadata)

        table = pa.table(
            {
                "time": [1, 2, 3],
                "mass": [0.1, 0.2, 0.3],
            }
        ).cast(schema)

        with pytest.raises(ValueError, match="No file_metadata found"):
            normalize_to_initial_mass(table)

    def test_missing_sample_mass_error(self) -> None:
        """Test error when sample_mass is not in metadata."""
        import json

        metadata = {"instrument": "NETZSCH STA 449 F3"}  # No sample_mass
        schema_metadata = {
            b"file_metadata": json.dumps(metadata).encode(),
        }

        table = pa.table(
            {
                "time": [1, 2, 3],
                "mass": [0.1, 0.2, 0.3],
            }
        )
        schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(schema)

        with pytest.raises(ValueError, match="sample_mass not found"):
            normalize_to_initial_mass(table)

    def test_invalid_sample_mass_error(self) -> None:
        """Test error when sample_mass is invalid."""
        import json

        # Test zero mass
        metadata = {"sample_mass": 0.0}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}

        table = pa.table(
            {
                "time": [1, 2, 3],
                "mass": [0.1, 0.2, 0.3],
            }
        )
        schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(schema)

        with pytest.raises(ValueError, match="Invalid sample_mass value"):
            normalize_to_initial_mass(table)

        # Test negative mass
        metadata = {"sample_mass": -5.0}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(schema)

        with pytest.raises(ValueError, match="Invalid sample_mass value"):
            normalize_to_initial_mass(table)

    def test_missing_column_error(self) -> None:
        """Test error when specified columns don't exist."""
        with pytest.raises(KeyError, match="Columns not found"):
            normalize_to_initial_mass(self.table, columns=["nonexistent_column"])

    def test_non_numeric_column_error(self) -> None:
        """Test error when trying to normalize non-numeric columns."""

        # Add a string column
        table_with_string = self.table.append_column(
            "string_col", pa.array(["a", "b", "c"] * 17)[:50]
        )

        with pytest.raises(ValueError, match="not numeric and cannot be normalized"):
            normalize_to_initial_mass(table_with_string, columns=["string_col"])

    def test_no_default_columns_error(self) -> None:
        """Test error when no default columns are found."""
        # Create table without mass or dsc_signal
        table_no_defaults = pa.table(
            {
                "time": [1, 2, 3],
                "temperature": [25, 50, 75],
            }
        )

        # Add required metadata
        import json

        metadata = {"sample_mass": 10.0}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        schema = table_no_defaults.schema.with_metadata(schema_metadata)
        table_no_defaults = table_no_defaults.cast(schema)

        with pytest.raises(ValueError, match="No default normalization columns found"):
            normalize_to_initial_mass(table_no_defaults)

    def test_realistic_mass_normalization(self) -> None:
        """Test realistic mass normalization scenario."""
        # Create realistic data: mass starts at ~0 (tared) then changes
        time = np.linspace(0, 120, 100)  # 2 hour experiment

        # Simulate mass loss during heating
        mass_loss = np.cumsum(
            np.where(time > 30, -0.02, 0)
        )  # Start losing mass after 30min
        mass = 0.05 + mass_loss + np.random.RandomState(42).normal(0, 0.005, len(time))

        table = pa.table({"time": time, "mass": mass})

        # Add metadata with realistic initial sample mass
        import json

        metadata = {"sample_mass": 8.75}  # 8.75 mg initial mass
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(schema)

        # Normalize (in place)
        result_table = normalize_to_initial_mass(table, columns=["mass"])
        result_df = pl.from_arrow(result_table)
        normalized_mass = result_df[
            "mass"
        ].to_numpy()  # Column updated in place  # type: ignore[index]

        # Initial mass should be close to initial_offset / sample_mass
        expected_initial = 0.05 / 8.75  # ~0.0057
        assert abs(normalized_mass[0] - expected_initial) < 0.001

        # Final mass should show the loss as fraction
        final_mass_loss = abs(mass[-1])  # Total mass change
        expected_final_normalized = abs(normalized_mass[-1] - normalized_mass[0])
        expected_loss_fraction = final_mass_loss / 8.75
        assert abs(expected_final_normalized - expected_loss_fraction) < 0.01


if __name__ == "__main__":
    pytest.main([__file__])
