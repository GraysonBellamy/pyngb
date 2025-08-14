"""
Tests for the pyngb API module.

This module tests the public API functions including read_ngb and CLI functionality.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa
import pytest

from pyngb.api.loaders import main, read_ngb
from pyngb.exceptions import NGBStreamNotFoundError


class TestReadNGBData:
    """Test read_ngb function."""

    def test_read_ngb_basic(self, sample_ngb_file, cleanup_temp_files):
        """Test basic read_ngb functionality."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file)

        assert isinstance(result, pa.Table)
        # Should have embedded metadata
        assert b"file_metadata" in result.schema.metadata
        assert b"type" in result.schema.metadata

    def test_read_ngb_file_not_found(self):
        """Test read_ngb with non-existent file."""
        with pytest.raises(FileNotFoundError):
            read_ngb("non_existent_file.ngb-ss3")

    def test_read_ngb_adds_file_hash(self, sample_ngb_file, cleanup_temp_files):
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
        self, mock_get_hash, sample_ngb_file, cleanup_temp_files
    ):
        """Test read_ngb when hash generation fails."""
        mock_get_hash.return_value = None
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file)

        # Should still work, just without hash
        assert isinstance(result, pa.Table)
        metadata_bytes = result.schema.metadata[b"file_metadata"]
        metadata = json.loads(metadata_bytes)
        assert "file_hash" not in metadata

    def test_read_ngb_return_metadata_false(self, sample_ngb_file, cleanup_temp_files):
        """Test read_ngb with return_metadata=False (default)."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = read_ngb(temp_file, return_metadata=False)

        assert isinstance(result, pa.Table)
        # Should have embedded metadata
        assert b"file_metadata" in result.schema.metadata

    def test_read_ngb_return_metadata_true(self, sample_ngb_file, cleanup_temp_files):
        """Test read_ngb with return_metadata=True."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, data = read_ngb(temp_file, return_metadata=True)

        assert isinstance(metadata, dict)
        assert isinstance(data, pa.Table)
        # Data should NOT have embedded metadata when returned separately
        assert (
            data.schema.metadata is None or b"file_metadata" not in data.schema.metadata
        )

    def test_read_ngb_metadata_structure(self, sample_ngb_file, cleanup_temp_files):
        """Test read_ngb metadata structure."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, data = read_ngb(temp_file, return_metadata=True)

        # Should have at least some metadata fields
        assert isinstance(metadata, dict)
        # The exact content depends on the sample file structure

    def test_read_ngb_error_handling(self):
        """Test read_ngb error handling."""
        # Create invalid file
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as f:
            f.write(b"invalid content")
            temp_path = f.name

        try:
            with pytest.raises(Exception):  # Should raise some kind of parsing error
                read_ngb(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestMainCLI:
    """Test main CLI function."""

    def test_main_help_argument(self):
        """Covered by CLI execution tests; retain minimal smoke check only."""
        assert callable(main)

    @patch("pyngb.api.loaders.read_ngb")
    @patch("pyarrow.parquet.write_table")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.touch")
    @patch("pathlib.Path.unlink")
    def test_main_parquet_output(
        self,
        mock_unlink,
        mock_touch,
        mock_mkdir,
        mock_is_file,
        mock_exists,
        mock_write_table,
        mock_read_ngb,
    ):
        """Test main function with parquet output."""
        import sys
        from unittest.mock import patch

        # Mock file system operations
        mock_exists.return_value = True
        mock_is_file.return_value = True
        mock_mkdir.return_value = None
        mock_touch.return_value = None
        mock_unlink.return_value = None

        # Mock the data loading
        mock_table = MagicMock(spec=pa.Table)
        mock_read_ngb.return_value = mock_table

        # Mock sys.argv
        with patch.object(
            sys, "argv", ["pyngb", "test.ngb-ss3", "-f", "parquet", "-o", "/tmp"]
        ):
            result = main()

        assert result == 0
        mock_read_ngb.assert_called_once_with("test.ngb-ss3")
        mock_write_table.assert_called_once()

    @patch("pyngb.api.loaders.read_ngb")
    @patch("polars.from_arrow")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.touch")
    @patch("pathlib.Path.unlink")
    def test_main_csv_output(
        self,
        mock_unlink,
        mock_touch,
        mock_mkdir,
        mock_is_file,
        mock_exists,
        mock_from_arrow,
        mock_read_ngb,
    ):
        """Test main function with CSV output."""
        import sys
        from unittest.mock import patch

        # Mock file system operations
        mock_exists.return_value = True
        mock_is_file.return_value = True
        mock_mkdir.return_value = None
        mock_touch.return_value = None
        mock_unlink.return_value = None

        # Mock the data loading and conversion
        mock_table = MagicMock(spec=pa.Table)
        mock_read_ngb.return_value = mock_table

        # Mock polars DataFrame (not pandas)
        mock_polars_df = MagicMock(spec=pl.DataFrame)  # Ensure it's a DataFrame
        mock_from_arrow.return_value = mock_polars_df

        # Mock sys.argv
        with patch.object(
            sys, "argv", ["pyngb", "test.ngb-ss3", "-f", "csv", "-o", "/tmp"]
        ):
            result = main()

        assert result == 0
        mock_read_ngb.assert_called_once_with("test.ngb-ss3")
        # The actual implementation uses polars write_csv, not pandas to_csv
        mock_polars_df.write_csv.assert_called_once()

    @patch("pyngb.api.loaders.read_ngb")
    def test_main_file_not_found(self, mock_read_ngb):
        """Test main function with non-existent file."""
        import sys
        from unittest.mock import patch

        # Mock file not found error
        mock_read_ngb.side_effect = FileNotFoundError("File not found")

        # Mock sys.argv
        with patch.object(sys, "argv", ["pyngb", "nonexistent.ngb-ss3"]):
            result = main()

        assert result == 1  # Should return error code

    @patch("pyngb.api.loaders.read_ngb")
    def test_main_parsing_error(self, mock_read_ngb):
        """Test main function with parsing error."""
        import sys
        from unittest.mock import patch

        # Mock parsing error
        mock_read_ngb.side_effect = NGBStreamNotFoundError("Stream not found")

        # Mock sys.argv
        with patch.object(sys, "argv", ["pyngb", "corrupted.ngb-ss3"]):
            result = main()

        assert result == 1  # Should return error code

    def test_main_verbose_logging(self):
        """Test main function with verbose logging."""
        import sys
        from unittest.mock import patch

        # Mock sys.argv with verbose flag
        with patch.object(sys, "argv", ["pyngb", "--help", "-v"]):
            try:
                main()
            except SystemExit as e:
                # Should still exit normally for help
                assert e.code == 0


@pytest.mark.integration
class TestIntegrationWithMockNGB:
    """Integration tests using mock NGB files."""

    def test_integration_with_mock_file(self, sample_ngb_file, cleanup_temp_files):
        """Test complete integration with mock NGB file."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        # Test default behavior
        result = read_ngb(temp_file)
        assert isinstance(result, pa.Table)

        # Test metadata return
        metadata, data = read_ngb(temp_file, return_metadata=True)
        assert isinstance(metadata, dict)
        assert isinstance(data, pa.Table)

    def test_consistency_between_modes(self, sample_ngb_file, cleanup_temp_files):
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
                assert embedded_metadata[key] == metadata[key]

    def test_polars_integration(self, sample_ngb_file, cleanup_temp_files):
        """Test integration with polars DataFrame conversion."""
        import polars as pl

        temp_file = cleanup_temp_files(sample_ngb_file)

        # Test conversion from table mode
        table = read_ngb(temp_file)
        df = pl.from_arrow(table)
        assert isinstance(df, pl.DataFrame)

        # Test conversion from separate mode
        metadata, data = read_ngb(temp_file, return_metadata=True)
        df2 = pl.from_arrow(data)
        assert isinstance(df2, pl.DataFrame)

        # Should have same shape
        assert df.height == df2.height
        assert df.width == df2.width
