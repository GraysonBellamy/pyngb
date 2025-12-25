"""
Custom exceptions for NETZSCH NGB file parsing.
"""

__all__ = [
    "NGBBaselineError",
    "NGBConfigurationError",
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBMetadataExtractionError",
    "NGBParseError",
    "NGBResourceLimitError",
    "NGBStreamNotFoundError",
    "NGBUnsupportedVersionError",
    "NGBValidationError",
]


class NGBParseError(Exception):
    """Base exception for NGB file parsing errors."""


class NGBConfigurationError(NGBParseError):
    """Raised when configuration validation fails."""


class NGBCorruptedFileError(NGBParseError):
    """Raised when NGB file is corrupted or has invalid structure."""


class NGBUnsupportedVersionError(NGBParseError):
    """Raised when NGB file version is not supported."""


class NGBDataTypeError(NGBParseError):
    """Raised when encountering unknown or invalid data type."""


class NGBStreamNotFoundError(NGBParseError):
    """Raised when expected stream is not found in NGB file."""


class NGBMetadataExtractionError(NGBParseError):
    """Raised when failed to extract metadata from binary stream."""


class NGBBaselineError(NGBParseError):
    """Raised when baseline subtraction operation failed."""


class NGBValidationError(NGBParseError):
    """Raised when data validation failed.

    Attributes:
        errors: List of specific validation error messages
    """

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


class NGBResourceLimitError(NGBParseError):
    """Raised when resource limit exceeded (file size, memory, etc.)."""
