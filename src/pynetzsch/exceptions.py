"""
Custom exceptions for NETZSCH NGB file parsing.
"""

__all__ = [
    "NGBParseError",
    "NGBCorruptedFileError",
    "NGBUnsupportedVersionError",
    "NGBDataTypeError",
    "NGBStreamNotFoundError",
]


class NGBParseError(Exception):
    """Base exception for NGB file parsing errors."""

    pass


class NGBCorruptedFileError(NGBParseError):
    """Raised when NGB file is corrupted or has invalid structure."""

    pass


class NGBUnsupportedVersionError(NGBParseError):
    """Raised when NGB file version is not supported."""

    pass


class NGBDataTypeError(NGBParseError):
    """Raised when encountering unknown or invalid data type."""

    pass


class NGBStreamNotFoundError(NGBParseError):
    """Raised when expected stream is not found in NGB file."""

    pass
