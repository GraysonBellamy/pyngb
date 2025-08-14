"""
Unit tests for pyngb constants and configurations.
"""

from typing import Any, cast

import pytest

from pyngb.constants import BinaryMarkers, DataType, FileMetadata, PatternConfig


class TestDataType:
    """Test DataType enum."""

    def test_data_type_values(self):
        """Test that DataType enum has correct byte values."""
        assert DataType.INT32.value == b"\x03"
        assert DataType.FLOAT32.value == b"\x04"
        assert DataType.FLOAT64.value == b"\x05"
        assert DataType.STRING.value == b"\x1f"

    def test_data_type_comparison(self):
        """Test DataType comparisons."""
        assert DataType.FLOAT64.value == b"\x05"
        assert DataType.FLOAT64.value != DataType.FLOAT32.value

    def test_data_type_membership(self):
        """Test DataType membership testing."""
        byte_val = b"\x05"
        data_types = [dt.value for dt in DataType]
        assert byte_val in data_types


class TestBinaryMarkers:
    """Test BinaryMarkers dataclass."""

    def test_binary_markers_immutable(self):
        """Test that BinaryMarkers is frozen (immutable)."""
        markers = BinaryMarkers()
        # Should not be able to modify attributes
        with pytest.raises(AttributeError):
            markers.START_DATA = b"different_value"
            cast(Any, markers).START_DATA = b"different_value"

    def test_binary_markers_values(self):
        """Test BinaryMarkers have correct values."""
        markers = BinaryMarkers()

        assert markers.START_DATA == b"\xa0\x01"
        assert markers.TYPE_PREFIX == b"\x17\xfc\xff\xff"
        assert markers.TYPE_SEPARATOR == b"\x80\x01"
        assert markers.END_FIELD == b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"

        # Test that END_DATA is a longer sequence
        assert len(markers.END_DATA) > 10
        assert isinstance(markers.END_DATA, bytes)

    def test_binary_markers_uniqueness(self):
        """Test that all markers are unique."""
        markers = BinaryMarkers()
        marker_values = [
            markers.END_FIELD,
            markers.TYPE_PREFIX,
            markers.TYPE_SEPARATOR,
            markers.END_TABLE,
            markers.START_DATA,
        ]

        # All should be different
        for i, marker1 in enumerate(marker_values):
            for j, marker2 in enumerate(marker_values):
                if i != j:
                    assert marker1 != marker2


class TestPatternConfig:
    """Test PatternConfig dataclass."""

    def test_pattern_config_defaults(self):
        """Test PatternConfig default values."""
        config = PatternConfig()

        # Test metadata patterns
        assert "instrument" in config.metadata_patterns
        assert "sample_name" in config.metadata_patterns
        assert "sample_mass" in config.metadata_patterns

        # Test column map
        assert "8d" in config.column_map
        assert config.column_map["8d"] == "time"
        assert config.column_map["8e"] == "sample_temperature"

        # Test temperature program patterns
        assert "stage_type" in config.temp_prog_patterns
        assert "temperature" in config.temp_prog_patterns

        # Test calibration constants
        assert "p0" in config.cal_constants_patterns
        assert len(config.cal_constants_patterns) == 6  # p0 through p5

    def test_pattern_config_modifiable(self):
        """Test that PatternConfig can be modified."""
        config = PatternConfig()

        # Should be able to add new patterns
        config.metadata_patterns["custom_field"] = (b"\x99\x99", b"\x88\x88")
        assert "custom_field" in config.metadata_patterns

        # Should be able to modify column map
        config.column_map["ff"] = "custom_column"
        assert config.column_map["ff"] == "custom_column"

    def test_pattern_config_pattern_structure(self):
        """Test the structure of patterns in PatternConfig."""
        config = PatternConfig()

        # Metadata patterns should be tuples of bytes
        for field_name, (category, field_bytes) in config.metadata_patterns.items():
            assert isinstance(category, bytes)
            assert isinstance(field_bytes, bytes)
            assert len(category) >= 2
            assert len(field_bytes) >= 2

        # Temp prog patterns should be bytes
        for field_name, pattern in config.temp_prog_patterns.items():
            assert isinstance(pattern, bytes)
            assert len(pattern) >= 2

        # Cal constants patterns should be bytes
        for field_name, pattern in config.cal_constants_patterns.items():
            assert isinstance(pattern, bytes)
            assert len(pattern) == 2  # Should be 2 bytes each

    def test_pattern_config_column_map_types(self):
        """Test column map has correct types."""
        config = PatternConfig()

        for hex_id, column_name in config.column_map.items():
            assert isinstance(hex_id, str)
            assert isinstance(column_name, str)
            assert len(hex_id) >= 1  # At least one character
            assert len(column_name) >= 1  # At least one character


class TestFileMetadata:
    """Test FileMetadata TypedDict."""

    def test_file_metadata_usage(self):
        """Test FileMetadata can be used as a type hint and dict."""
        # Can create as regular dict
        metadata: FileMetadata = {
            "instrument": "Test Instrument",
            "sample_name": "Test Sample",
            "sample_mass": 15.5,
        }

        assert metadata["instrument"] == "Test Instrument"
        assert metadata["sample_mass"] == 15.5

        # Can add optional fields
        metadata["operator"] = "Test User"
        assert metadata["operator"] == "Test User"

    def test_file_metadata_optional_fields(self):
        """Test that FileMetadata fields are optional."""
        # Empty metadata should be valid
        metadata: FileMetadata = {}
        assert isinstance(metadata, dict)

        # Partial metadata should be valid
        metadata = {"instrument": "Test"}
        assert metadata.get("sample_name") is None
        assert metadata.get("instrument") == "Test"

    def test_file_metadata_field_types(self):
        """Test FileMetadata field type checking."""
        metadata: FileMetadata = {
            "instrument": "string_value",
            "sample_mass": 15.5,  # float
            "temperature_program": {},  # dict
            "calibration_constants": {"p0": 1.0},  # dict of floats
            "file_hash": {"method": "BLAKE2b", "hash": "abc123"},  # dict
        }

        assert isinstance(metadata["instrument"], str)
        assert isinstance(metadata["sample_mass"], (int, float))
        assert isinstance(metadata["temperature_program"], dict)
        assert isinstance(metadata["calibration_constants"], dict)
        assert isinstance(metadata["file_hash"], dict)
