"""
Unit tests for pyngb exceptions.
"""

import pytest

from pyngb.exceptions import (
    NGBCorruptedFileError,
    NGBDataTypeError,
    NGBParseError,
    NGBStreamNotFoundError,
    NGBUnsupportedVersionError,
)


class TestNGBExceptions:
    """Test custom exception classes."""

    def test_ngb_parse_error_base(self):
        """Test that NGBParseError is the base exception."""
        error = NGBParseError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_ngb_corrupted_file_error(self):
        """Test NGBCorruptedFileError inheritance."""
        error = NGBCorruptedFileError("File is corrupted")
        assert str(error) == "File is corrupted"
        assert isinstance(error, NGBParseError)
        assert isinstance(error, Exception)

    def test_ngb_unsupported_version_error(self):
        """Test NGBUnsupportedVersionError inheritance."""
        error = NGBUnsupportedVersionError("Version not supported")
        assert str(error) == "Version not supported"
        assert isinstance(error, NGBParseError)

    def test_ngb_data_type_error(self):
        """Test NGBDataTypeError inheritance."""
        error = NGBDataTypeError("Unknown data type")
        assert str(error) == "Unknown data type"
        assert isinstance(error, NGBParseError)

    def test_ngb_stream_not_found_error(self):
        """Test NGBStreamNotFoundError inheritance."""
        error = NGBStreamNotFoundError("Stream not found")
        assert str(error) == "Stream not found"
        assert isinstance(error, NGBParseError)

    def test_exception_chaining(self):
        """Test that exceptions can be chained."""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            with pytest.raises(NGBCorruptedFileError) as exc_info:
                raise NGBCorruptedFileError("Wrapped error") from e

            assert str(exc_info.value) == "Wrapped error"
            assert exc_info.value.__cause__ is e

    def test_all_exceptions_importable(self):
        """Test that all exceptions are properly importable."""
        exceptions = [
            NGBParseError,
            NGBCorruptedFileError,
            NGBUnsupportedVersionError,
            NGBDataTypeError,
            NGBStreamNotFoundError,
        ]

        for exc_class in exceptions:
            assert callable(exc_class)
            assert isinstance(exc_class, type) and issubclass(exc_class, Exception)

            # Test instantiation
            instance = exc_class("test message")
            assert str(instance) == "test message"
