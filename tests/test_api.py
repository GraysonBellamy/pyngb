"""
Tests for the pyngb API module.

This module tests the public API functions including read_ngb.
"""

import json
import zipfile
from pathlib import Path
from unittest.mock import patch
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from pyngb.api.loaders import read_ngb
from pyngb.api.metadata import get_column_units


class TestReadNGBData:
    """Test read_ngb function."""

    def test_read_ngb_basic(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test basic read_ngb functionality."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file)

        assert isinstance(result, pa.Table)
        # Should have embedded metadata
        assert b"file_metadata" in result.schema.metadata
        assert b"type" in result.schema.metadata

    def test_read_ngb_file_not_found(self) -> None:
        """Test read_ngb with non-existent file."""
        with pytest.raises(FileNotFoundError):
            read_ngb("non_existent_file.ngb-ss3")

    def test_read_ngb_adds_file_hash(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test that read_ngb adds file hash to metadata."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file)

        # Extract metadata
        metadata_bytes = result.schema.metadata[b"file_metadata"]
        metadata = json.loads(metadata_bytes)

        assert "file_hash" in metadata
        assert "method" in metadata["file_hash"]
        assert "hash" in metadata["file_hash"]
        assert metadata["file_hash"]["method"] == "BLAKE2b"

    @patch("pyngb.api.loaders.get_hash")
    def test_read_ngb_hash_failure(
        self, mock_get_hash: Any, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test read_ngb when hash generation fails."""
        mock_get_hash.return_value = None
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file)

        # Should still work, just without hash
        assert isinstance(result, pa.Table)
        metadata_bytes = result.schema.metadata[b"file_metadata"]
        metadata = json.loads(metadata_bytes)
        assert "file_hash" not in metadata

    def test_read_ngb_return_metadata_false(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test read_ngb with return_metadata=False (default)."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file, return_metadata=False)

        assert isinstance(result, pa.Table)
        # Should have embedded metadata
        assert b"file_metadata" in result.schema.metadata

    def test_read_ngb_return_metadata_true(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test read_ngb with return_metadata=True."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, data = read_ngb(temp_file, return_metadata=True)

        assert isinstance(metadata, dict)
        assert isinstance(data, pa.Table)
        # Data should NOT have embedded metadata when returned separately
        assert (
            data.schema.metadata is None or b"file_metadata" not in data.schema.metadata
        )

    def test_read_ngb_standardizes_time_to_seconds(self) -> None:
        """NGB files store time in minutes; pyngb exposes seconds."""
        test_file = Path("tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3")

        metadata, data = read_ngb(test_file, return_metadata=True)
        df = pl.from_arrow(data)
        assert isinstance(df, pl.DataFrame)

        # Raw fixture duration is 102.5 minutes, so public data should be seconds.
        assert float(df["time"][-1]) == pytest.approx(102.5 * 60.0)

        stage_duration = sum(
            float(stage.get("time", 0.0))
            for stage in metadata.get("temperature_program", {}).values()
        )
        assert stage_duration == pytest.approx(float(df["time"][-1]))

        table = read_ngb(test_file)
        assert get_column_units(table, "time") == "s"

    def test_read_ngb_metadata_structure(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test read_ngb metadata structure."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, _data = read_ngb(temp_file, return_metadata=True)

        # Should have at least some metadata fields
        assert isinstance(metadata, dict)
        # The exact content depends on the sample file structure

    def test_read_ngb_error_handling(self) -> None:
        """Test read_ngb error handling."""
        # Create invalid file
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as f:
            f.write(b"invalid content")
            temp_path = f.name

        try:
            with pytest.raises(zipfile.BadZipFile):
                read_ngb(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)


@pytest.mark.integration
class TestIntegrationWithMockNGB:
    """Integration tests using mock NGB files."""

    def test_integration_with_mock_file(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test complete integration with mock NGB file."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        # Test default behavior
        result = read_ngb(temp_file)
        assert isinstance(result, pa.Table)

        # Test metadata return
        metadata, data = read_ngb(temp_file, return_metadata=True)
        assert isinstance(metadata, dict)
        assert isinstance(data, pa.Table)

    def test_consistency_between_modes(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test consistency between return_metadata=True/False modes."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        # Get data both ways
        table = read_ngb(temp_file, return_metadata=False)
        metadata, data = read_ngb(temp_file, return_metadata=True)

        # Data should be the same
        assert table.num_rows == data.num_rows
        assert table.num_columns == data.num_columns
        assert table.column_names == data.column_names

        # Metadata should be consistent
        embedded_metadata = json.loads(table.schema.metadata[b"file_metadata"])
        # Note: embedded metadata includes file_hash, separate metadata might not have it yet
        # So we compare the core fields
        core_fields = ["instrument", "sample_name"]
        for key in core_fields:
            if key in embedded_metadata and key in metadata:
                assert embedded_metadata[key] == metadata[key]  # type: ignore[literal-required]

    def test_polars_integration(
        self, sample_ngb_file: Any, cleanup_temp_files: Any
    ) -> None:
        """Test integration with polars DataFrame conversion."""
        import polars as pl

        temp_file = cleanup_temp_files(sample_ngb_file)

        # Test conversion from table mode
        table = read_ngb(temp_file)
        df = pl.from_arrow(table)
        assert isinstance(df, pl.DataFrame)

        # Test conversion from separate mode
        _metadata, data = read_ngb(temp_file, return_metadata=True)
        df2 = pl.from_arrow(data)
        assert isinstance(df2, pl.DataFrame)

        # Should have same shape
        assert df.height == df2.height
        assert df.width == df2.width


class TestReadNGBMetadataPaths:
    """Regression tests for DES-05: metadata initialization must not depend
    on which flag combination read_ngb was called with."""

    REAL_FILE = (
        Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"
    )

    def test_column_metadata_present_with_return_metadata(self) -> None:
        """return_metadata=True must not skip column metadata initialization."""
        if not self.REAL_FILE.exists():
            pytest.skip("Real test file not available")

        _metadata, table = read_ngb(self.REAL_FILE, return_metadata=True)
        assert get_column_units(table, "mass") == "mg"
        assert get_column_units(table, "sample_temperature") == "°C"

    def test_dynamic_axis_validated_without_baseline(self) -> None:
        """A bogus dynamic_axis is rejected up front, before any file I/O."""
        with pytest.raises(ValueError, match="dynamic_axis"):
            read_ngb("does-not-exist.ngb-ss3", dynamic_axis="bogus")
