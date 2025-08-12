"""
Custom exceptions for NETZSCH NGB file parsing.
"""

__all__ = [
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBParseError",
    "NGBStreamNotFoundError",
    "NGBUnsupportedVersionError",
]


class NGBParseError(Exception):
    """Base exception for NGB file parsing errors."""


class NGBCorruptedFileError(NGBParseError):
    """Raised when NGB file is corrupted or has invalid structure."""


class NGBUnsupportedVersionError(NGBParseError):
    """Raised when NGB file version is not supported."""


class NGBDataTypeError(NGBParseError):
    """Raised when encountering unknown or invalid data type."""


class NGBStreamNotFoundError(NGBParseError):
    """Raised when expected stream is not found in NGB file."""
