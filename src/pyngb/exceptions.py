"""
Custom exceptions for NETZSCH NGB file parsing.
"""

__all__ = [
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBParseError",
    "NGBResourceLimitError",
    "NGBStreamNotFoundError",
]


class NGBParseError(Exception):
    """Base exception for NGB file parsing errors."""


class NGBCorruptedFileError(NGBParseError):
    """Raised when NGB file is corrupted or has invalid structure.

    Structured attributes locate the corruption so tests and callers can
    assert on facts instead of message prose. All are optional; ``None``
    means "not applicable / unknown".

    Attributes:
        stream: Stream number the corruption was found in.
        offset: Byte offset within the stream blob.
        table_index: Index of the affected table within its stream.
        declared: Size/count the file declared.
        available: Size/count actually available.
    """

    def __init__(
        self,
        message: str = "",
        *,
        stream: int | None = None,
        offset: int | None = None,
        table_index: int | None = None,
        declared: int | None = None,
        available: int | None = None,
    ) -> None:
        super().__init__(message)
        self.stream = stream
        self.offset = offset
        self.table_index = table_index
        self.declared = declared
        self.available = available


class NGBDataTypeError(NGBParseError):
    """Raised when encountering unknown or invalid data type."""


class NGBStreamNotFoundError(NGBParseError):
    """Raised when expected stream is not found in NGB file."""


class NGBResourceLimitError(NGBParseError):
    """Raised when resource limit exceeded (file size, memory, etc.).

    Structured attributes mirror :class:`NGBCorruptedFileError` so limit
    violations are equally assertable without message matching.

    Attributes:
        stream: Stream number the violation was found in.
        offset: Byte offset within the stream blob.
        declared: Size the file declared (bytes).
        limit: The configured limit that was exceeded (bytes).
    """

    def __init__(
        self,
        message: str = "",
        *,
        stream: int | None = None,
        offset: int | None = None,
        declared: int | None = None,
        limit: int | None = None,
    ) -> None:
        super().__init__(message)
        self.stream = stream
        self.offset = offset
        self.declared = declared
        self.limit = limit
