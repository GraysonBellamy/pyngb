"""
Unit tests for pyngb constants: the FileMetadata contract.

The binary-format constants (dtype ids, markers, channel map) live in
``pyngb.format`` and are covered by test_tokenizer.py / test_field_map.py.
"""

from pyngb.constants import FileMetadata


class TestFileMetadata:
    """Test FileMetadata TypedDict."""

    def test_file_metadata_usage(self) -> None:
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

    def test_file_metadata_optional_fields(self) -> None:
        """Test that FileMetadata fields are optional."""
        # Empty metadata should be valid
        metadata: FileMetadata = {}
        assert isinstance(metadata, dict)

        # Partial metadata should be valid
        metadata = {"instrument": "Test"}
        assert metadata.get("sample_name") is None
        assert metadata.get("instrument") == "Test"

    def test_file_metadata_field_types(self) -> None:
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
