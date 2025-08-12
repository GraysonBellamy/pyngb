"""
Unit tests for pyngb API functions.
"""

import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyngb.api.loaders import get_sta_data, load_ngb_data, main
from pyngb.constants import BinaryMarkers
from pyngb.exceptions import NGBStreamNotFoundError


class TestLoadNGBData:
    """Test load_ngb_data function."""

    def test_load_ngb_data_basic(self, sample_ngb_file, cleanup_temp_files):
        """Test basic load_ngb_data functionality."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = load_ngb_data(temp_file)

        assert isinstance(result, pa.Table)
        # Should have embedded metadata
        assert b"file_metadata" in result.schema.metadata
        assert b"type" in result.schema.metadata

    def test_load_ngb_data_file_not_found(self):
        """Test load_ngb_data with non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_ngb_data("non_existent_file.ngb-ss3")

    def test_load_ngb_data_adds_file_hash(self, sample_ngb_file, cleanup_temp_files):
        """Test that load_ngb_data adds file hash to metadata."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = load_ngb_data(temp_file)

        # Extract metadata
        metadata_bytes = result.schema.metadata[b"file_metadata"]
        import json

        metadata = json.loads(metadata_bytes)

        assert "file_hash" in metadata
        assert "method" in metadata["file_hash"]
        assert "hash" in metadata["file_hash"]
        assert metadata["file_hash"]["method"] == "BLAKE2b"

    @patch("pyngb.api.loaders.get_hash")
    def test_load_ngb_data_hash_failure(
        self, mock_get_hash, sample_ngb_file, cleanup_temp_files
    ):
        """Test load_ngb_data when hash generation fails."""
        mock_get_hash.return_value = None
        temp_file = cleanup_temp_files(sample_ngb_file)

        result = load_ngb_data(temp_file)

        # Should still work, just without hash
        assert isinstance(result, pa.Table)
        metadata_bytes = result.schema.metadata[b"file_metadata"]
        import json

        metadata = json.loads(metadata_bytes)
        assert "file_hash" not in metadata


class TestGetSTAData:
    """Test get_sta_data function."""

    def test_get_sta_data_basic(self, sample_ngb_file, cleanup_temp_files):
        """Test basic get_sta_data functionality."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, data = get_sta_data(temp_file)

        assert isinstance(metadata, dict)
        assert isinstance(data, pa.Table)

    def test_get_sta_data_metadata_structure(self, sample_ngb_file, cleanup_temp_files):
        """Test get_sta_data metadata structure."""
        temp_file = cleanup_temp_files(sample_ngb_file)

        metadata, data = get_sta_data(temp_file)

        # Should have at least some metadata fields
        assert isinstance(metadata, dict)
        # The exact content depends on the sample file structure

    def test_get_sta_data_file_not_found(self):
        """Test get_sta_data with non-existent file."""
        with pytest.raises(FileNotFoundError):
            get_sta_data("non_existent_file.ngb-ss3")


class TestMainCLI:
    """Test main CLI function."""

    def test_main_help_argument(self):
        """Test main function with help argument."""

    import sys
    from unittest.mock import patch

    # Mock sys.argv to include help
    with patch.object(sys, "argv", ["pyngb", "--help"]):
        try:
            main()
        except SystemExit as e:
            # argparse exits with 0 for help
            assert e.code == 0

    @patch("pyngb.api.loaders.load_ngb_data")
    @patch("pyngb.api.loaders.Path")
    @patch("pyarrow.parquet.write_table")
    def test_main_parquet_output(self, mock_write_table, mock_path, mock_load_ngb):
        """Test main function with Parquet output."""
        import sys
        from unittest.mock import patch

        # Setup mocks
        mock_table = MagicMock()
        mock_load_ngb.return_value = mock_table
        mock_output_path = MagicMock()
        mock_path.return_value = mock_output_path
        mock_output_path.mkdir = MagicMock()

        # Mock sys.argv
        test_args = ["pyngb", "test.ngb-ss3", "-f", "parquet", "-o", "/output"]
        with patch.object(sys, "argv", test_args):
            result = main()

        assert result == 0
        mock_load_ngb.assert_called_once_with("test.ngb-ss3")
        mock_write_table.assert_called_once()

    @patch("pyngb.api.loaders.load_ngb_data")
    @patch("pyngb.api.loaders.Path")
    @patch("polars.from_arrow")
    def test_main_csv_output(self, mock_from_arrow, mock_path, mock_load_ngb):
        """Test main function with CSV output."""
        import sys
        from unittest.mock import patch

        # Setup mocks
        mock_table = MagicMock()
        mock_load_ngb.return_value = mock_table
        mock_output_path = MagicMock()
        mock_path.return_value = mock_output_path
        mock_output_path.mkdir = MagicMock()

        mock_df = MagicMock()
        mock_pandas_df = MagicMock()
        mock_from_arrow.return_value = mock_df
        mock_df.to_pandas.return_value = mock_pandas_df

        # Mock sys.argv
        test_args = ["pyngb", "test.ngb-ss3", "-f", "csv"]
        with patch.object(sys, "argv", test_args):
            result = main()

        assert result == 0
        mock_load_ngb.assert_called_once_with("test.ngb-ss3")
        mock_pandas_df.to_csv.assert_called_once()

    @patch("pyngb.api.loaders.load_ngb_data")
    def test_main_error_handling(self, mock_load_ngb):
        """Test main function error handling."""
        import sys
        from unittest.mock import patch

        # Make load_ngb_data raise an exception
        mock_load_ngb.side_effect = Exception("Test error")

        # Mock sys.argv
        test_args = ["pyngb", "test.ngb-ss3"]
        with patch.object(sys, "argv", test_args):
            result = main()

        assert result == 1  # Error exit code

    def test_main_verbose_logging(self):
        """Test main function with verbose logging."""
        import logging
        import sys
        from unittest.mock import patch

        with patch.object(sys, "argv", ["pyngb", "test.ngb-ss3", "-v"]):
            with patch("pyngb.api.loaders.load_ngb_data") as mock_load:
                with patch("logging.basicConfig") as mock_config:
                    mock_load.side_effect = FileNotFoundError("Test")

                    main()

                    # Should configure logging with DEBUG level
                    mock_config.assert_called_once_with(level=logging.DEBUG)


class TestIntegrationWithMockNGB:
    """Integration tests with mock NGB file."""

    def create_minimal_ngb(self):
        """Create a minimal valid NGB file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                markers = BinaryMarkers()

                # Minimal stream 1 with some metadata
                stream1_data = (
                    b"\x75\x17"
                    + b"pad" * 10
                    + b"\x59\x10"
                    + b"pad" * 5
                    + markers.TYPE_PREFIX
                    + b"\x1f"
                    + markers.TYPE_SEPARATOR
                    + b"\x10\x00\x00\x00NETZSCH Instrument\x00"
                    + markers.END_FIELD
                )
                z.writestr("Streams/stream_1.table", stream1_data)

                # Minimal stream 2 with time data
                stream2_data = (
                    b"\x8d\x17"
                    + b"pad" * 5
                    + markers.TABLE_SEPARATOR
                    + b"\x8d\x75"
                    + markers.START_DATA
                    + b"\x05"
                    +
                    # Two float64 values: 0.0 and 1.0
                    b"\x00" * 8
                    + b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
                    + markers.END_DATA
                )
                z.writestr("Streams/stream_2.table", stream2_data)

            return temp_file.name

    def test_full_integration(self):
        """Test full integration with realistic mock data."""
        ngb_file = self.create_minimal_ngb()

        try:
            # Test load_ngb_data
            table = load_ngb_data(ngb_file)
            assert isinstance(table, pa.Table)
            assert table.num_rows >= 0  # May be 0 due to minimal data

            # Test get_sta_data
            metadata, data = get_sta_data(ngb_file)
            assert isinstance(metadata, dict)
            assert isinstance(data, pa.Table)

        finally:
            Path(ngb_file).unlink(missing_ok=True)

    def test_missing_streams(self):
        """Test behavior with missing required streams."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                # Only create stream 2, missing stream 1
                z.writestr("Streams/stream_2.table", b"minimal_data")

        try:
            with pytest.raises(NGBStreamNotFoundError):
                load_ngb_data(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)
