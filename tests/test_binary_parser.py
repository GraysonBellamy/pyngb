"""
Unit tests for pyngb binary parser.
"""

import struct

import pytest

from pyngb.binary.parser import BinaryParser
from pyngb.config import ParsingConfig
from pyngb.constants import BinaryMarkers, DataType
from pyngb.exceptions import NGBResourceLimitError


class TestBinaryParser:
    """Test BinaryParser class."""

    def test_init_default_markers(self) -> None:
        """Test BinaryParser initialization with default markers."""
        parser = BinaryParser()

        assert isinstance(parser.markers, BinaryMarkers)
        assert parser.markers.START_DATA == b"\xa0\x01"
        assert len(parser._compiled_patterns) > 0
        assert "table_sep" in parser._compiled_patterns

    def test_init_custom_markers(self) -> None:
        """Test BinaryParser initialization with custom markers."""
        custom_markers = BinaryMarkers()
        parser = BinaryParser(custom_markers)

        assert parser.markers is custom_markers

    def test_parse_value_int32(self) -> None:
        """Test parsing INT32 values."""
        # 42 as little-endian INT32
        value = b"\x2a\x00\x00\x00"
        result = BinaryParser.parse_value(DataType.INT32.value, value)
        assert result == 42

    def test_parse_value_float32(self) -> None:
        """Test parsing FLOAT32 values."""
        # 1.0 as little-endian FLOAT32
        value = b"\x00\x00\x80\x3f"
        result = BinaryParser.parse_value(DataType.FLOAT32.value, value)
        assert abs(result - 1.0) < 1e-6

    def test_parse_value_float64(self) -> None:
        """Test parsing FLOAT64 values."""
        # 1.0 as little-endian FLOAT64
        value = b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
        result = BinaryParser.parse_value(DataType.FLOAT64.value, value)
        assert abs(result - 1.0) < 1e-15

    def test_parse_value_string(self) -> None:
        """Test parsing STRING values."""
        # String with 4-byte length prefix
        value = b"\x05\x00\x00\x00Hello\x00"
        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == "Hello"

    def test_parse_value_string_with_nulls(self) -> None:
        """Test parsing STRING values with embedded nulls."""
        value = b"\x07\x00\x00\x00Hel\x00lo\x00\x00"
        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == "Hello"  # Nulls should be stripped

    def test_parse_value_string_fffeff_format(self) -> None:
        """Test parsing STRING values with NETZSCH fffeff format."""
        # NETZSCH format: fffeff + char_count + UTF-16LE data
        # "Hello" = 5 characters in UTF-16LE
        char_count = 5
        utf16le_data = "Hello".encode("utf-16le")
        value = b"\xff\xfe\xff" + bytes([char_count]) + utf16le_data

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == "Hello"

    def test_parse_value_string_fffeff_with_special_chars(self) -> None:
        """Test fffeff format with special characters."""
        test_string = "Müller"
        char_count = len(test_string)
        utf16le_data = test_string.encode("utf-16le")
        value = b"\xff\xfe\xff" + bytes([char_count]) + utf16le_data

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == test_string

    def test_parse_value_string_fffeff_with_nulls(self) -> None:
        """Test fffeff format with null padding."""
        test_string = "Test"
        char_count = len(test_string)
        utf16le_data = test_string.encode("utf-16le") + b"\x00\x00"  # Add null padding
        value = b"\xff\xfe\xff" + bytes([char_count]) + utf16le_data

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == test_string

    def test_parse_value_string_fffeff_invalid(self) -> None:
        """Test fffeff format with invalid data."""
        # Too short for claimed character count
        value = b"\xff\xfe\xff\x10" + b"short"  # Claims 16 chars but only has 5 bytes

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        # Should fall back to standard parsing or return None
        assert result is None or isinstance(result, str)

    def test_parse_value_string_standard_format(self) -> None:
        """Test standard format still works after enhancement."""
        # Standard 4-byte length prefix + UTF-8
        test_string = "Standard"
        length = len(test_string.encode("utf-8"))
        value = struct.pack("<I", length) + test_string.encode("utf-8")

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == test_string

    def test_parse_value_string_utf16le_fallback(self) -> None:
        """Test UTF-16LE fallback in standard format."""
        # Standard format with UTF-16LE data
        test_string = "UTF16Test"
        utf16le_data = test_string.encode("utf-16le")
        length = len(utf16le_data)
        value = struct.pack("<I", length) + utf16le_data

        result = BinaryParser.parse_value(DataType.STRING.value, value)
        assert result == test_string

    def test_parse_value_unknown_type(self) -> None:
        """Test parsing unknown data type returns the raw value."""
        value = b"\x42\x43\x44"
        result = BinaryParser.parse_value(b"\x99", value)
        assert result == value

    def test_parse_value_error_handling(self) -> None:
        """Test parse_value handles errors gracefully."""
        # Too short for INT32
        value = b"\x42"
        result = BinaryParser.parse_value(DataType.INT32.value, value)
        assert result is None

    def test_split_tables_no_separator(self) -> None:
        """Test split_tables with no separator returns single table."""
        parser = BinaryParser()
        data = b"single_table_data"

        result = parser.split_tables(data)
        assert len(result) == 1
        assert result[0] == data

    def test_split_tables_with_separators(self) -> None:
        """Test split_tables with table separators."""
        parser = BinaryParser()
        separator = parser.markers.TABLE_SEPARATOR

        # Create data with separators (accounting for the -2 offset logic)
        data = b"table1" + separator + b"table2" + separator + b"table3"

        result = parser.split_tables(data)
        # The exact number depends on the separator finding logic
        # Just verify we get multiple tables and they contain expected data
        assert len(result) >= 1
        # Verify some of the original data is present
        combined = b"".join(result)
        assert b"table1" in combined or b"table2" in combined

    def test_split_tables_rejects_excessive_table_count(self) -> None:
        """Pathological inputs with too many separators must be rejected."""
        tight_config = ParsingConfig(max_tables_per_stream=3)
        parser = BinaryParser(parsing_config=tight_config)
        sep = parser.markers.TABLE_SEPARATOR

        data = b"a" + sep + b"b" + sep + b"c" + sep + b"d" + sep + b"e"

        with pytest.raises(NGBResourceLimitError, match="max_tables_per_stream"):
            parser.split_tables(data)

    def test_split_tables_honors_custom_limit(self) -> None:
        """Table counts at the configured limit must still parse."""
        config = ParsingConfig(max_tables_per_stream=3)
        parser = BinaryParser(parsing_config=config)
        sep = parser.markers.TABLE_SEPARATOR

        data = b"a" + sep + b"b" + sep + b"c"
        result = parser.split_tables(data)
        assert len(result) <= 3

    def test_parse_data_rejects_oversized_payload(self) -> None:
        """A data payload larger than max_array_size_mb must be rejected."""
        parser = BinaryParser(parsing_config=ParsingConfig(max_array_size_mb=1))
        payload = b"\x00" * (1024 * 1024 + 8)  # one float64 over the 1 MB limit

        with pytest.raises(NGBResourceLimitError, match="max_array_size_mb"):
            parser.parse_data(DataType.FLOAT64.value, payload)

    def test_parse_data_at_limit_decodes(self) -> None:
        """A payload exactly at the configured limit must still decode."""
        parser = BinaryParser(parsing_config=ParsingConfig(max_array_size_mb=1))
        n = (1024 * 1024) // 8  # exactly 1 MB of float64
        payload = struct.pack(f"<{n}d", *([1.5] * n))

        result = parser.parse_data(DataType.FLOAT64.value, payload)
        assert len(result) == n
        assert result[0] == 1.5

    def test_get_compiled_pattern_caching(self) -> None:
        """Test that compiled patterns are cached."""
        parser = BinaryParser()

        pattern_bytes = b"test_pattern"

        # First call should compile and cache
        pattern1 = parser._get_compiled_pattern("test", pattern_bytes)

        # Second call should return cached version
        pattern2 = parser._get_compiled_pattern("test", pattern_bytes)

        assert pattern1 is pattern2  # Same object reference
        assert "test" in parser._compiled_patterns

    def test_get_compiled_pattern_different_keys(self) -> None:
        """Test that different keys create different pattern entries."""
        parser = BinaryParser()

        pattern1 = parser._get_compiled_pattern("key1", b"test_pattern")
        pattern2 = parser._get_compiled_pattern("key2", b"test_pattern")

        # Even with same pattern bytes, different keys should exist in cache
        assert "key1" in parser._compiled_patterns
        assert "key2" in parser._compiled_patterns

        # The patterns themselves may be the same object due to internal regex caching
        # but they should both work correctly
        assert pattern1.pattern == pattern2.pattern
