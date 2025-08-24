"""Tests for column metadata functionality."""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from pyngb.api.metadata import (
    add_column_processing_step,
    get_column_baseline_status,
    get_column_source,
    get_column_units,
    get_processing_history,
    inspect_column_metadata,
    is_column_baseline_correctable,
    mark_baseline_corrected,
    set_column_source,
    set_column_units,
)
from pyngb.util import (
    add_processing_step,
    get_baseline_status,
    get_column_metadata,
    initialize_table_column_metadata,
    is_baseline_correctable,
    set_column_metadata,
    set_default_column_metadata,
    update_column_metadata,
)


class TestUtilityFunctions:
    """Test utility functions for column metadata."""

    def setup_method(self):
        """Set up test data."""
        self.table = pa.table(
            {
                "time": np.linspace(0, 100, 50),
                "mass": 10 - 0.02 * np.linspace(0, 100, 50),
                "sample_temperature": 25 + 5 * np.linspace(0, 100, 50),
                "dsc_signal": np.random.RandomState(42).normal(0, 1, 50),
                "purge_flow_1": np.full(50, 50.0),
            }
        )

    def test_set_get_column_metadata(self):
        """Test setting and getting column metadata."""
        metadata = {
            "units": "mg",
            "processing_history": ["raw"],
            "source": "measurement",
            "baseline_subtracted": False,
        }

        # Set metadata
        updated_table = set_column_metadata(self.table, "mass", metadata)

        # Get all metadata
        retrieved = get_column_metadata(updated_table, "mass")
        assert retrieved == metadata

        # Get specific key
        units = get_column_metadata(updated_table, "mass", "units")
        assert units == "mg"

        # Get non-existent key
        missing = get_column_metadata(updated_table, "mass", "nonexistent")
        assert missing is None

    def test_update_column_metadata(self):
        """Test updating specific fields in column metadata."""
        # Set initial metadata
        initial_metadata = {"units": "mg", "source": "measurement"}
        table_with_meta = set_column_metadata(self.table, "mass", initial_metadata)

        # Update with new fields
        updates = {
            "processing_history": ["raw", "filtered"],
            "baseline_subtracted": False,
        }
        updated_table = update_column_metadata(table_with_meta, "mass", updates)

        # Check that both old and new fields are present
        final_metadata = get_column_metadata(updated_table, "mass")
        assert final_metadata["units"] == "mg"  # Original field preserved
        assert final_metadata["source"] == "measurement"  # Original field preserved
        assert final_metadata["processing_history"] == [
            "raw",
            "filtered",
        ]  # New field added
        assert final_metadata["baseline_subtracted"] is False  # New field added

    def test_add_processing_step(self):
        """Test adding processing steps to history."""
        # Set initial metadata with processing history
        initial_metadata = {"processing_history": ["raw"]}
        table_with_meta = set_column_metadata(self.table, "mass", initial_metadata)

        # Add processing step
        updated_table = add_processing_step(table_with_meta, "mass", "smoothed")

        # Check history was updated
        history = get_column_metadata(updated_table, "mass", "processing_history")
        assert history == ["raw", "smoothed"]

        # Add same step again (should not duplicate)
        updated_table2 = add_processing_step(updated_table, "mass", "smoothed")
        history2 = get_column_metadata(updated_table2, "mass", "processing_history")
        assert history2 == ["raw", "smoothed"]  # No duplicate

    def test_baseline_status_functions(self):
        """Test baseline status checking functions."""
        # Test is_baseline_correctable
        assert is_baseline_correctable("mass") is True
        assert is_baseline_correctable("dsc_signal") is True
        assert is_baseline_correctable("time") is False
        assert is_baseline_correctable("purge_flow_1") is False

        # Set metadata for baseline-correctable column
        metadata = {"baseline_subtracted": True}
        table_with_meta = set_column_metadata(self.table, "mass", metadata)

        # Test get_baseline_status
        status = get_baseline_status(table_with_meta, "mass")
        assert status is True

        # Test for non-baseline-correctable column
        status_time = get_baseline_status(table_with_meta, "time")
        assert status_time is None  # Not applicable

    def test_set_default_column_metadata(self):
        """Test setting default metadata for known column types."""
        # Test known column type
        updated_table = set_default_column_metadata(self.table, "mass")
        metadata = get_column_metadata(updated_table, "mass")

        assert metadata["units"] == "mg"
        assert metadata["processing_history"] == ["raw"]
        assert metadata["source"] == "measurement"
        assert metadata["baseline_subtracted"] is False

        # Test unknown column type
        table_with_custom = self.table.append_column(
            "custom_column", pa.array(np.ones(50))
        )
        updated_table2 = set_default_column_metadata(table_with_custom, "custom_column")
        metadata2 = get_column_metadata(updated_table2, "custom_column")

        assert metadata2["units"] == "unknown"
        assert metadata2["processing_history"] == ["raw"]
        assert metadata2["source"] == "unknown"
        assert (
            "baseline_subtracted" not in metadata2
        )  # Not applicable for unknown columns

    def test_initialize_table_column_metadata(self):
        """Test initializing metadata for all columns in a table."""
        updated_table = initialize_table_column_metadata(self.table)

        # Check that all columns got default metadata
        for column in self.table.column_names:
            metadata = get_column_metadata(updated_table, column)
            assert metadata is not None
            assert "units" in metadata
            assert "processing_history" in metadata
            assert "source" in metadata

            # Check baseline_subtracted only for applicable columns
            if is_baseline_correctable(column):
                assert "baseline_subtracted" in metadata
            else:
                assert "baseline_subtracted" not in metadata

    def test_error_handling(self):
        """Test error handling for invalid inputs."""
        # Test non-existent column
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            set_column_metadata(self.table, "nonexistent", {})

        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            get_column_metadata(self.table, "nonexistent")

        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            update_column_metadata(self.table, "nonexistent", {})


class TestAPIFunctions:
    """Test high-level API functions for column metadata."""

    def setup_method(self):
        """Set up test data with some metadata."""
        self.table = pa.table(
            {
                "time": np.linspace(0, 100, 30),
                "mass": 10 - 0.02 * np.linspace(0, 100, 30),
                "dsc_signal": np.random.RandomState(42).normal(0, 1, 30),
                "purge_flow_1": np.full(30, 50.0),
            }
        )

        # Initialize with default metadata
        self.table = initialize_table_column_metadata(self.table)

    def test_units_functions(self):
        """Test units setting and getting functions."""
        # Get default units
        units = get_column_units(self.table, "mass")
        assert units == "mg"

        # Set new units
        updated_table = set_column_units(self.table, "mass", "g")
        new_units = get_column_units(updated_table, "mass")
        assert new_units == "g"

    def test_baseline_functions(self):
        """Test baseline correction functions."""
        # Check initial status
        status = get_column_baseline_status(self.table, "mass")
        assert status is False

        # Mark as baseline corrected
        updated_table = mark_baseline_corrected(self.table, "mass")
        new_status = get_column_baseline_status(updated_table, "mass")
        assert new_status is True

        # Check processing history was updated
        history = get_processing_history(updated_table, "mass")
        assert "baseline_corrected" in history

        # Test with multiple columns
        updated_table2 = mark_baseline_corrected(self.table, ["mass", "dsc_signal"])
        mass_status = get_column_baseline_status(updated_table2, "mass")
        dsc_status = get_column_baseline_status(updated_table2, "dsc_signal")
        assert mass_status is True
        assert dsc_status is True

        # Test with non-correctable column (should be ignored)
        updated_table3 = mark_baseline_corrected(self.table, ["mass", "time"])
        time_status = get_column_baseline_status(updated_table3, "time")
        assert time_status is None  # Still None, not applicable

    def test_processing_history_functions(self):
        """Test processing history functions."""
        # Get initial history
        history = get_processing_history(self.table, "mass")
        assert history == ["raw"]

        # Add processing step
        updated_table = add_column_processing_step(self.table, "mass", "smoothed")
        new_history = get_processing_history(updated_table, "mass")
        assert new_history == ["raw", "smoothed"]

    def test_source_functions(self):
        """Test source setting and getting functions."""
        # Get default source
        source = get_column_source(self.table, "mass")
        assert source == "measurement"

        # Set new source
        updated_table = set_column_source(self.table, "mass", "calculated")
        new_source = get_column_source(updated_table, "mass")
        assert new_source == "calculated"

    def test_inspection_function(self):
        """Test metadata inspection function."""
        metadata = inspect_column_metadata(self.table, "mass")

        # Check that all expected fields are present
        assert "units" in metadata
        assert "processing_history" in metadata
        assert "source" in metadata
        assert "baseline_subtracted" in metadata

        # Check values
        assert metadata["units"] == "mg"
        assert metadata["processing_history"] == ["raw"]
        assert metadata["source"] == "measurement"
        assert metadata["baseline_subtracted"] is False

    def test_baseline_correctable_function(self):
        """Test baseline correctability checking function."""
        assert is_column_baseline_correctable("mass") is True
        assert is_column_baseline_correctable("dsc_signal") is True
        assert is_column_baseline_correctable("time") is False
        assert is_column_baseline_correctable("purge_flow_1") is False


class TestIntegrationWithAnalysis:
    """Test integration with existing analysis functions."""

    def setup_method(self):
        """Set up test data."""
        time = np.linspace(0, 100, 50)
        mass = 10 - 0.02 * time
        temperature = 25 + 5 * time

        self.table = pa.table(
            {
                "time": time,
                "mass": mass,
                "sample_temperature": temperature,
            }
        )

        # Initialize metadata
        self.table = initialize_table_column_metadata(self.table)

    def test_add_dtg_with_metadata(self):
        """Test that add_dtg sets appropriate metadata."""
        from pyngb.api.analysis import add_dtg

        # Add DTG
        table_with_dtg = add_dtg(self.table)

        # Check DTG column metadata
        dtg_metadata = inspect_column_metadata(table_with_dtg, "dtg")
        assert dtg_metadata["units"] == "mg/min"
        assert dtg_metadata["processing_history"] == ["calculated"]
        assert dtg_metadata["source"] == "derived"
        assert (
            "baseline_subtracted" not in dtg_metadata
        )  # DTG doesn't support baseline correction

    def test_normalize_with_metadata(self):
        """Test that normalize_to_initial_mass preserves and updates metadata."""
        from pyngb.api.analysis import normalize_to_initial_mass
        import json

        # Add metadata with sample_mass
        metadata = {"sample_mass": 15.75}
        schema_metadata = {
            b"file_metadata": json.dumps(metadata).encode(),
            b"type": b"STA",
        }
        schema = self.table.schema.with_metadata(schema_metadata)
        table_with_file_meta = self.table.cast(schema)

        # Mark mass as baseline corrected
        table_baseline_corrected = mark_baseline_corrected(table_with_file_meta, "mass")

        # Normalize
        normalized_table = normalize_to_initial_mass(
            table_baseline_corrected, columns=["mass"]
        )

        # Check mass column metadata (updated in place)
        norm_metadata = inspect_column_metadata(normalized_table, "mass")
        assert norm_metadata["units"] == "mg/mg"  # Units updated
        assert (
            "normalized" in norm_metadata["processing_history"]
        )  # Processing step added
        assert (
            norm_metadata["source"] == "measurement"
        )  # Source preserved (not changed to derived)
        assert norm_metadata["baseline_subtracted"] is True  # Baseline status preserved


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_metadata(self):
        """Test handling of columns with no metadata."""
        table = pa.table({"data": [1, 2, 3]})

        # Get metadata from column with no metadata
        metadata = get_column_metadata(table, "data")
        assert metadata == {}

        # Get specific key from column with no metadata
        units = get_column_metadata(table, "data", "units")
        assert units is None

        # Get baseline status from column with no metadata
        status = get_baseline_status(table, "data")
        assert status is None

    def test_malformed_metadata(self):
        """Test handling of malformed metadata."""
        import pyarrow as pa

        # Create table with malformed metadata (non-JSON bytes)
        schema = pa.schema(
            [pa.field("data", pa.int64(), metadata={b"units": b"invalid\xff\xfe"})]
        )
        table = pa.table({"data": [1, 2, 3]}, schema=schema)

        # Should handle gracefully
        metadata = get_column_metadata(table, "data")
        # Should decode as string fallback or skip the malformed entry
        assert isinstance(metadata, dict)

    def test_repeated_initialization(self):
        """Test that repeated initialization doesn't overwrite existing metadata."""
        table = pa.table({"mass": [1, 2, 3]})

        # First initialization
        table1 = initialize_table_column_metadata(table)
        original_units = get_column_units(table1, "mass")
        assert original_units == "mg"

        # Modify metadata
        table2 = set_column_units(table1, "mass", "g")
        modified_units = get_column_units(table2, "mass")
        assert modified_units == "g"

        # Second initialization should not overwrite
        table3 = initialize_table_column_metadata(table2)
        final_units = get_column_units(table3, "mass")
        assert final_units == "g"  # Should remain modified value


if __name__ == "__main__":
    pytest.main([__file__])
