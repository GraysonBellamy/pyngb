"""Tests for DSC calibration functionality."""

import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from pyngb import read_ngb
from pyngb.api.analysis import apply_dsc_calibration, normalize_to_initial_mass
from pyngb.api.metadata import get_column_units, get_processing_history
from pyngb.util import set_column_metadata, get_column_metadata


class TestDSCCalibration:
    """Test DSC calibration functionality."""

    def test_calibration_formula_correctness(self) -> None:
        """Test that the calibration formula is implemented correctly."""
        # Create test data
        test_data = {
            "sample_temperature": [100.0, 200.0, 300.0, 400.0, 500.0],
            "dsc_signal": [1.0, 2.0, 3.0, 4.0, 5.0],  # µV
        }

        table = pa.table(test_data)

        # Create mock calibration constants
        cal_constants = {
            "p0": 25.0,  # Reference temperature
            "p1": 100.0,  # Temperature scale
            "p2": 1.0,  # Base calibration factor
            "p3": 0.1,  # Linear term
            "p4": 0.01,  # Quadratic term
            "p5": 0.001,  # Cubic term
        }

        # Create metadata with calibration constants
        metadata = {"calibration_constants": cal_constants}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        # Set up default column metadata
        table = set_column_metadata(
            table,
            "dsc_signal",
            {
                "units": "µV",
                "processing_history": ["raw"],
                "source": "measurement",
                "baseline_subtracted": False,
                "calibration_applied": False,
            },
        )
        table = set_column_metadata(
            table,
            "sample_temperature",
            {"units": "°C", "processing_history": ["raw"], "source": "measurement"},
        )

        # Apply calibration
        calibrated_table = apply_dsc_calibration(table)

        # Extract calibrated values using Polars
        df = pl.from_arrow(calibrated_table)
        calibrated_values = df["dsc_signal"].to_numpy()  # type: ignore[index]

        # Verify that values have changed and are reasonable
        assert not np.array_equal(test_data["dsc_signal"], calibrated_values)
        assert all(v > 0 for v in calibrated_values)

        # Check units
        assert get_column_units(calibrated_table, "dsc_signal") == "mW"

        # Check processing history
        history = get_processing_history(calibrated_table, "dsc_signal")
        assert "calibration_applied" in history

        # Check calibration flag
        metadata = get_column_metadata(calibrated_table, "dsc_signal")
        assert metadata["calibration_applied"] is True

    def test_missing_calibration_constants(self) -> None:
        """Test error handling when calibration constants are missing."""
        test_data = {"sample_temperature": [100.0, 200.0], "dsc_signal": [1.0, 2.0]}
        table = pa.table(test_data)

        # No metadata
        with pytest.raises(ValueError, match="Table metadata is missing"):
            apply_dsc_calibration(table)

        # Empty metadata
        schema_metadata = {b"file_metadata": json.dumps({}).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        with pytest.raises(ValueError, match="calibration_constants not found"):
            apply_dsc_calibration(table)

        # Incomplete calibration constants
        incomplete_constants = {"p0": 1.0, "p1": 1.0}  # Missing p2-p5
        metadata = {"calibration_constants": incomplete_constants}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        with pytest.raises(KeyError, match="Missing calibration constants"):
            apply_dsc_calibration(table)

    def test_missing_required_columns(self) -> None:
        """Test error handling when required columns are missing."""
        # Table without temperature
        table_no_temp = pa.table({"dsc_signal": [1.0, 2.0]})
        with pytest.raises(ValueError, match="must contain 'sample_temperature'"):
            apply_dsc_calibration(table_no_temp)

        # Table without DSC signal
        table_no_dsc = pa.table({"sample_temperature": [100.0, 200.0]})
        with pytest.raises(ValueError, match="must contain 'dsc_signal'"):
            apply_dsc_calibration(table_no_dsc)

    def test_double_calibration_prevention(self) -> None:
        """Test that calibration cannot be applied twice."""
        test_data = {"sample_temperature": [100.0, 200.0], "dsc_signal": [1.0, 2.0]}
        table = pa.table(test_data)

        # Set up metadata and apply first calibration
        cal_constants = {f"p{i}": 1.0 for i in range(6)}
        metadata = {"calibration_constants": cal_constants}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        table = set_column_metadata(
            table,
            "dsc_signal",
            {
                "units": "µV",
                "processing_history": ["raw"],
                "source": "measurement",
                "calibration_applied": False,
            },
        )
        table = set_column_metadata(
            table,
            "sample_temperature",
            {"units": "°C", "processing_history": ["raw"], "source": "measurement"},
        )

        calibrated_table = apply_dsc_calibration(table)

        # Try to apply calibration again - should fail
        with pytest.raises(ValueError, match="Calibration has already been applied"):
            apply_dsc_calibration(calibrated_table)

    def test_custom_column_names(self) -> None:
        """Test calibration with custom column names."""
        test_data = {"furnace_temperature": [100.0, 200.0], "heat_flow": [1.0, 2.0]}
        table = pa.table(test_data)

        cal_constants = {f"p{i}": 1.0 for i in range(6)}
        metadata = {"calibration_constants": cal_constants}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        table = set_column_metadata(
            table,
            "heat_flow",
            {
                "units": "µV",
                "processing_history": ["raw"],
                "source": "measurement",
                "calibration_applied": False,
            },
        )
        table = set_column_metadata(
            table,
            "furnace_temperature",
            {"units": "°C", "processing_history": ["raw"], "source": "measurement"},
        )

        calibrated_table = apply_dsc_calibration(
            table, temperature_column="furnace_temperature", dsc_column="heat_flow"
        )

        assert get_column_units(calibrated_table, "heat_flow") == "mW"

    def test_calibration_with_normalization_order_independence(self) -> None:
        """Test that calibration and normalization work in any order."""
        # Create test data
        test_data = {
            "sample_temperature": [100.0, 200.0, 300.0],
            "dsc_signal": [1.0, 2.0, 3.0],
        }
        table = pa.table(test_data)

        # Add required metadata
        cal_constants = {f"p{i}": 1.0 for i in range(6)}
        metadata = {"calibration_constants": cal_constants, "sample_mass": 10.0}
        schema_metadata = {b"file_metadata": json.dumps(metadata).encode()}
        new_schema = table.schema.with_metadata(schema_metadata)
        table = table.cast(new_schema)

        # Set up column metadata
        table = set_column_metadata(
            table,
            "dsc_signal",
            {
                "units": "µV",
                "processing_history": ["raw"],
                "source": "measurement",
                "baseline_subtracted": False,
                "calibration_applied": False,
            },
        )
        table = set_column_metadata(
            table,
            "sample_temperature",
            {"units": "°C", "processing_history": ["raw"], "source": "measurement"},
        )

        # Test 1: Calibration first, then normalization
        table1 = apply_dsc_calibration(table)
        table1 = normalize_to_initial_mass(table1, columns=["dsc_signal"])

        units1 = get_column_units(table1, "dsc_signal")
        history1 = get_processing_history(table1, "dsc_signal")

        # Test 2: Normalization first, then calibration
        table2 = normalize_to_initial_mass(table, columns=["dsc_signal"])
        table2 = apply_dsc_calibration(table2)

        units2 = get_column_units(table2, "dsc_signal")
        history2 = get_processing_history(table2, "dsc_signal")

        # Both should result in proper units and history
        assert units1 == "mW/mg"
        assert units2 == "mW/mg"
        assert "calibration_applied" in history1
        assert "normalized" in history1
        assert "calibration_applied" in history2
        assert "normalized" in history2

    @pytest.mark.skipif(
        not Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3").exists(),
        reason="Test file not available",
    )
    def test_calibration_with_real_file(self) -> None:
        """Test calibration with a real NGB file."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")

        # Load data
        table = read_ngb(str(test_file))

        # Check initial state
        initial_units = get_column_units(table, "dsc_signal")
        assert initial_units == "µV"

        initial_metadata = get_column_metadata(table, "dsc_signal")
        assert initial_metadata["calibration_applied"] is False

        # Apply calibration
        calibrated_table = apply_dsc_calibration(table)

        # Verify changes
        calibrated_units = get_column_units(calibrated_table, "dsc_signal")
        assert calibrated_units == "mW"

        calibrated_metadata = get_column_metadata(calibrated_table, "dsc_signal")
        assert calibrated_metadata["calibration_applied"] is True

        history = get_processing_history(calibrated_table, "dsc_signal")
        assert "calibration_applied" in history

        # Verify data has changed using Polars
        original_df = pl.from_arrow(table)
        calibrated_df = pl.from_arrow(calibrated_table)
        original_data = original_df["dsc_signal"].to_numpy()  # type: ignore[index]
        calibrated_data = calibrated_df["dsc_signal"].to_numpy()  # type: ignore[index]
        assert not np.array_equal(original_data, calibrated_data)
