"""
Comprehensive tests for pyngb utility functions.
"""

import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from pyngb.util import get_hash, set_metadata


class TestSetMetadata:
    """Test set_metadata function for PyArrow table metadata handling."""

    def create_sample_table(self):
        """Create a sample PyArrow table for testing."""
        data = {
            "time": [0.0, 1.0, 2.0],
            "temperature": [25.0, 30.0, 35.0],
            "mass": [10.5, 10.4, 10.3],
        }
        return pa.table(data)

    def test_set_metadata_empty_args(self):
        """Test set_metadata with no metadata arguments."""
        table = self.create_sample_table()
        result = set_metadata(table)

        # Should return the same table
        assert result.equals(table)
        assert result.num_rows == table.num_rows
        assert result.column_names == table.column_names

    def test_set_metadata_table_level_only(self):
        """Test setting only table-level metadata."""
        table = self.create_sample_table()
        tbl_meta = {"experiment": "test_exp", "version": "1.0", "data_type": "STA"}

        result = set_metadata(table, tbl_meta=tbl_meta)

        # Check that metadata was added
        assert result.schema.metadata is not None
        assert b"experiment" in result.schema.metadata
        assert b"version" in result.schema.metadata
        assert b"data_type" in result.schema.metadata

        # Check metadata values
        assert result.schema.metadata[b"experiment"] == b"test_exp"
        assert result.schema.metadata[b"version"] == b"1.0"
        assert result.schema.metadata[b"data_type"] == b"STA"

    def test_set_metadata_column_level_only(self):
        """Test setting only column-level metadata."""
        table = self.create_sample_table()
        col_meta = {
            "time": {"unit": "seconds", "description": "Time elapsed"},
            "temperature": {"unit": "celsius", "sensor": "thermocouple"},
        }

        result = set_metadata(table, col_meta=col_meta)

        # Check time column metadata
        time_field = result.field("time")
        assert time_field.metadata is not None
        assert b"unit" in time_field.metadata
        assert b"description" in time_field.metadata
        assert time_field.metadata[b"unit"] == b"seconds"
        assert time_field.metadata[b"description"] == b"Time elapsed"

        # Check temperature column metadata
        temp_field = result.field("temperature")
        assert temp_field.metadata is not None
        assert b"unit" in temp_field.metadata
        assert b"sensor" in temp_field.metadata
        assert temp_field.metadata[b"unit"] == b"celsius"
        assert temp_field.metadata[b"sensor"] == b"thermocouple"

        # Check that mass column has no metadata
        mass_field = result.field("mass")
        assert mass_field.metadata is None or len(mass_field.metadata) == 0

    def test_set_metadata_both_levels(self):
        """Test setting both table and column level metadata."""
        table = self.create_sample_table()
        tbl_meta = {"experiment": "combined_test"}
        col_meta = {"time": {"unit": "s"}}

        result = set_metadata(table, col_meta=col_meta, tbl_meta=tbl_meta)

        # Check table metadata
        assert result.schema.metadata[b"experiment"] == b"combined_test"

        # Check column metadata
        time_field = result.field("time")
        assert time_field.metadata[b"unit"] == b"s"

    def test_set_metadata_dict_serialization(self):
        """Test that dictionaries are JSON serialized."""
        table = self.create_sample_table()
        complex_meta = {
            "nested": {"key": "value", "number": 42},
            "list": [1, 2, 3],
            "boolean": True,
        }

        result = set_metadata(table, tbl_meta=complex_meta)

        # Check that dict was JSON serialized
        nested_meta = json.loads(result.schema.metadata[b"nested"].decode())
        assert nested_meta == {"key": "value", "number": 42}

        list_meta = json.loads(result.schema.metadata[b"list"].decode())
        assert list_meta == [1, 2, 3]

        bool_meta = json.loads(result.schema.metadata[b"boolean"].decode())
        assert bool_meta is True

    def test_set_metadata_bytes_handling(self):
        """Test handling of bytes metadata values."""
        table = self.create_sample_table()
        tbl_meta = {
            "binary_data": b"some binary content",
            "text_data": "some text content",
        }

        result = set_metadata(table, tbl_meta=tbl_meta)

        # Bytes should be stored as-is
        assert result.schema.metadata[b"binary_data"] == b"some binary content"
        # Strings should be encoded
        assert result.schema.metadata[b"text_data"] == b"some text content"

    def test_set_metadata_string_encoding(self):
        """Test that strings are properly UTF-8 encoded."""
        table = self.create_sample_table()
        tbl_meta = {"unicode_text": "héllo wørld 🌍"}

        result = set_metadata(table, tbl_meta=tbl_meta)

        # Check that unicode was properly encoded
        stored_text = result.schema.metadata[b"unicode_text"].decode("utf-8")
        assert stored_text == "héllo wørld 🌍"

    def test_set_metadata_existing_metadata_preservation(self):
        """Test that existing metadata is preserved and extended."""
        # Create table with existing metadata
        schema = pa.schema(
            [
                pa.field("time", pa.float64(), metadata={"existing": "value"}),
                pa.field("temp", pa.float64()),
            ],
            metadata={"table_existing": "table_value"},
        )

        table = pa.table([[0.0, 1.0, 2.0], [25.0, 30.0, 35.0]], schema=schema)

        # Add new metadata
        col_meta = {"time": {"new_key": "new_value"}}
        tbl_meta = {"new_table_key": "new_table_value"}

        result = set_metadata(table, col_meta=col_meta, tbl_meta=tbl_meta)

        # Check that existing column metadata is preserved
        time_field = result.field("time")
        assert b"existing" in time_field.metadata
        assert b"new_key" in time_field.metadata
        assert time_field.metadata[b"existing"] == b"value"
        assert time_field.metadata[b"new_key"] == b"new_value"

        # Check that existing table metadata is preserved
        assert b"table_existing" in result.schema.metadata
        assert b"new_table_key" in result.schema.metadata
        assert result.schema.metadata[b"table_existing"] == b"table_value"
        assert result.schema.metadata[b"new_table_key"] == b"new_table_value"

    def test_set_metadata_nonexistent_column(self):
        """Test that metadata for nonexistent columns is ignored."""
        table = self.create_sample_table()
        col_meta = {"nonexistent_column": {"unit": "unknown"}}

        result = set_metadata(table, col_meta=col_meta)

        # Should not raise error and should return equivalent table
        assert result.num_rows == table.num_rows
        assert result.column_names == table.column_names

    def test_set_metadata_data_preservation(self):
        """Test that actual data is preserved during metadata operations."""
        table = self.create_sample_table()
        original_data = table.to_pydict()

        result = set_metadata(table, tbl_meta={"test": "value"})

        # Data should be identical
        assert result.to_pydict() == original_data
        assert result.num_rows == table.num_rows
        assert result.num_columns == table.num_columns

    def test_set_metadata_empty_dicts(self):
        """Test set_metadata with empty metadata dictionaries."""
        table = self.create_sample_table()

        result = set_metadata(table, col_meta={}, tbl_meta={})

        # Should return equivalent table
        assert result.equals(table)

    def test_set_metadata_column_type_preservation(self):
        """Test that column types are preserved during metadata operations."""
        table = self.create_sample_table()
        original_schema = table.schema

        result = set_metadata(table, col_meta={"time": {"unit": "s"}})

        # Schema types should be preserved
        for i, field in enumerate(original_schema):
            assert result.schema.field(i).type == field.type


class TestGetHash:
    """Test get_hash function for file hashing."""

    def test_get_hash_valid_file(self):
        """Test get_hash with a valid file."""
        # Create a temporary file with known content
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = b"test content for hashing"
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        # Calculate expected hash
        expected_hash = hashlib.blake2b(content).hexdigest()

        # Test the function
        result = get_hash(temp_file_path)

        assert result == expected_hash

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_empty_file(self):
        """Test get_hash with an empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Empty file
            temp_file.flush()
            temp_file_path = temp_file.name

        expected_hash = hashlib.blake2b(b"").hexdigest()
        result = get_hash(temp_file_path)

        assert result == expected_hash

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_large_file(self):
        """Test get_hash with a larger file."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Create larger content
            content = b"A" * 10000  # 10KB of 'A's
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        expected_hash = hashlib.blake2b(content).hexdigest()
        result = get_hash(temp_file_path)

        assert result == expected_hash

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_binary_file(self):
        """Test get_hash with binary content."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Binary content with null bytes
            content = bytes(range(256))
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        expected_hash = hashlib.blake2b(content).hexdigest()
        result = get_hash(temp_file_path)

        assert result == expected_hash

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_unicode_content(self):
        """Test get_hash with Unicode content."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Unicode content
            content = "Hello 世界 🌍".encode()
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        expected_hash = hashlib.blake2b(content).hexdigest()
        result = get_hash(temp_file_path)

        assert result == expected_hash

        # Cleanup
        Path(temp_file_path).unlink()

    @patch("pyngb.util.logger")
    def test_get_hash_file_not_found(self, mock_logger):
        """Test get_hash with non-existent file."""
        result = get_hash("nonexistent_file.txt")

        assert result is None
        mock_logger.warning.assert_called_once_with(
            "File not found while generating hash: %s", "nonexistent_file.txt"
        )

    @patch("pyngb.util.logger")
    @patch("pathlib.Path.stat")
    @patch("builtins.open", side_effect=PermissionError("Permission denied"))
    def test_get_hash_permission_error(self, mock_open, mock_stat, mock_logger):
        """Test get_hash with permission error."""
        # Mock the stat call to succeed (so we get to the open call)
        mock_stat.return_value.st_size = 1024

        result = get_hash("protected_file.txt")

        assert result is None
        mock_logger.error.assert_called_once_with(
            "Permission denied while generating hash for file: %s", "protected_file.txt"
        )

    @patch("pyngb.util.logger")
    @patch("pathlib.Path.stat")
    @patch("hashlib.blake2b", side_effect=Exception("Hash error"))
    def test_get_hash_hashing_error(self, mock_blake2b, mock_stat, mock_logger):
        """Test get_hash when hashing itself fails."""
        # Mock the stat call to succeed
        mock_stat.return_value.st_size = 1024

        with tempfile.NamedTemporaryFile() as temp_file:
            result = get_hash(temp_file.name)

            assert result is None
            # Check that error was logged with the exception object
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            assert (
                call_args[0][0]
                == "Unexpected error while generating hash for file %s: %s"
            )
            assert call_args[0][1] == temp_file.name
            assert isinstance(call_args[0][2], Exception)
            assert str(call_args[0][2]) == "Hash error"

    @patch("pyngb.util.logger")
    @patch("pathlib.Path.stat")
    def test_get_hash_io_error(self, mock_stat, mock_logger):
        """Test get_hash with I/O error during reading."""
        # Mock the stat call to succeed
        mock_stat.return_value.st_size = 1024

        with patch("builtins.open") as mock_open:
            mock_file = MagicMock()
            mock_file.read.side_effect = OSError("I/O error")
            mock_open.return_value.__enter__.return_value = mock_file

            result = get_hash("test_file.txt")

            assert result is None
            # Check that error was logged with the OS error message
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            assert call_args[0][0] == "OS error while generating hash for file %s: %s"
            assert call_args[0][1] == "test_file.txt"
            assert isinstance(call_args[0][2], IOError)
            assert str(call_args[0][2]) == "I/O error"

    def test_get_hash_deterministic(self):
        """Test that get_hash produces deterministic results."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = b"deterministic test content"
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        # Get hash multiple times
        hash1 = get_hash(temp_file_path)
        hash2 = get_hash(temp_file_path)
        hash3 = get_hash(temp_file_path)

        # Should all be the same
        assert hash1 == hash2 == hash3
        assert hash1 is not None

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_different_files_different_hashes(self):
        """Test that different files produce different hashes."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file1:
            temp_file1.write(b"content 1")
            temp_file1.flush()
            temp_file1_path = temp_file1.name

        with tempfile.NamedTemporaryFile(delete=False) as temp_file2:
            temp_file2.write(b"content 2")
            temp_file2.flush()
            temp_file2_path = temp_file2.name

        hash1 = get_hash(temp_file1_path)
        hash2 = get_hash(temp_file2_path)

        assert hash1 != hash2
        assert hash1 is not None
        assert hash2 is not None

        # Cleanup
        Path(temp_file1_path).unlink()
        Path(temp_file2_path).unlink()

    def test_get_hash_blake2b_algorithm(self):
        """Test that get_hash uses BLAKE2b algorithm specifically."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = b"test blake2b"
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        result = get_hash(temp_file_path)
        expected = hashlib.blake2b(content).hexdigest()

        assert result == expected

        # Cleanup
        Path(temp_file_path).unlink()

    def test_get_hash_file_size_limit(self):
        """Test that get_hash respects the file size limit."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Create a file that's just over 1MB
            content = b"x" * (1024 * 1024 + 1)  # 1MB + 1 byte
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        # Test with default limit (1000MB) - should work
        result = get_hash(temp_file_path)
        assert result is not None

        # Test with 1MB limit - should fail
        result = get_hash(temp_file_path, max_size_mb=1)
        assert result is None

        # Cleanup
        Path(temp_file_path).unlink()


class TestUtilIntegration:
    """Test integration between util functions."""

    def test_metadata_with_file_hash(self):
        """Test combining set_metadata with get_hash for complete workflow."""
        # Create sample file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = b"sample file content"
            temp_file.write(content)
            temp_file.flush()
            temp_file_path = temp_file.name

        # Get hash
        file_hash = get_hash(temp_file_path)

        # Create table and add metadata including hash
        table = pa.table({"data": [1, 2, 3]})
        tbl_meta = {
            "file_hash": {
                "method": "BLAKE2b",
                "hash": file_hash,
                "file": Path(temp_file_path).name,
            },
            "data_type": "test",
        }

        result = set_metadata(table, tbl_meta=tbl_meta)

        # Verify metadata is properly stored
        assert b"file_hash" in result.schema.metadata
        assert b"data_type" in result.schema.metadata

        # Verify hash metadata structure
        hash_meta = json.loads(result.schema.metadata[b"file_hash"].decode())
        assert hash_meta["method"] == "BLAKE2b"
        assert hash_meta["hash"] == file_hash
        assert hash_meta["file"] == Path(temp_file_path).name

        # Cleanup
        Path(temp_file_path).unlink()

    def test_round_trip_metadata(self):
        """Test round-trip serialization/deserialization of metadata."""
        table = pa.table({"values": [1.0, 2.0, 3.0]})

        original_meta = {
            "instrument": "Test Instrument",
            "settings": {"temperature": 25.0, "pressure": 1.0},
            "calibration": [1.0, 0.95, 0.001],
        }

        # Set metadata
        table_with_meta = set_metadata(table, tbl_meta=original_meta)

        # Extract and deserialize metadata
        extracted_meta = {}
        for key, value in table_with_meta.schema.metadata.items():
            key_str = key.decode()
            if key_str in ["instrument"]:
                extracted_meta[key_str] = value.decode()
            else:
                extracted_meta[key_str] = json.loads(value.decode())

        # Verify round-trip preservation
        assert extracted_meta["instrument"] == original_meta["instrument"]
        assert extracted_meta["settings"] == original_meta["settings"]
        assert extracted_meta["calibration"] == original_meta["calibration"]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_set_metadata_very_large_metadata(self):
        """Test set_metadata with very large metadata."""
        table = pa.table({"data": [1]})

        # Create large metadata
        large_value = "x" * 10000  # 10KB string
        tbl_meta = {"large_field": large_value}

        result = set_metadata(table, tbl_meta=tbl_meta)

        # Should handle large metadata without issues
        assert b"large_field" in result.schema.metadata
        stored_value = result.schema.metadata[b"large_field"].decode()
        assert stored_value == large_value

    def test_set_metadata_special_characters(self):
        """Test set_metadata with special characters in keys and values."""
        table = pa.table({"data": [1]})

        meta_with_special_chars = {
            "key with spaces": "value with spaces",
            "key_with_unicode_🌍": "value_with_unicode_🚀",
            "key.with.dots": "value.with.dots",
            "key-with-dashes": "value-with-dashes",
        }

        result = set_metadata(table, tbl_meta=meta_with_special_chars)

        # All keys should be properly stored
        for key, expected_value in meta_with_special_chars.items():
            key_bytes = key.encode("utf-8")
            assert key_bytes in result.schema.metadata
            assert result.schema.metadata[key_bytes].decode("utf-8") == expected_value

    def test_get_hash_path_edge_cases(self):
        """Test get_hash with various path formats."""
        with patch("pyngb.util.logger") as mock_logger:
            # Test with empty path
            result = get_hash("")
            assert result is None

            # Test with path containing special characters
            result = get_hash("file with spaces.txt")
            assert result is None

            # Test with unicode path
            result = get_hash("file_🌍.txt")
            assert result is None

            # All should result in file not found
            assert mock_logger.warning.call_count == 3

    def test_metadata_type_edge_cases(self):
        """Test metadata handling with edge case data types."""
        table = pa.table({"data": [1]})

        edge_case_meta = {
            "none_value": None,
            "zero": 0,
            "empty_string": "",
            "empty_list": [],
            "empty_dict": {},
            "float_inf": float("inf"),
            "float_nan": float("nan"),
        }

        result = set_metadata(table, tbl_meta=edge_case_meta)

        # Verify all values are properly serialized
        for key in edge_case_meta:
            key_bytes = key.encode("utf-8")
            assert key_bytes in result.schema.metadata

            # Get the stored value as a string
            stored_str = result.schema.metadata[key_bytes].decode()

            # Deserialize and check (note: inf/nan become null in JSON)
            if key == "float_inf":
                # Infinity is stored as 'Infinity' in JSON
                stored_value = json.loads(stored_str)
                assert stored_value == float("inf")
            elif key == "float_nan":
                # NaN is stored as 'NaN' in JSON
                stored_value = json.loads(stored_str)
                import math

                assert math.isnan(stored_value)
            elif key == "empty_string":
                # Empty strings are stored as-is, not JSON encoded
                assert stored_str == ""
            else:
                stored_value = json.loads(stored_str)
                assert stored_value == edge_case_meta[key]
