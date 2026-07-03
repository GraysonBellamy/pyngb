"""Parsing configuration for pyNGB."""

from dataclasses import dataclass

__all__ = ["ParsingConfig"]


@dataclass(frozen=True, slots=True)
class ParsingConfig:
    """Configuration for binary parsing operations.

    These limits guard against pathological or malicious inputs (e.g. a
    decompression bomb inside the NGB ZIP container). Real NGB streams
    decompress to well under a megabyte, so the defaults leave orders of
    magnitude of headroom; raise them only if a legitimate file trips one.

    Attributes:
        max_stream_size_mb: Maximum decompressed size in MB of any single
            stream member inside the NGB archive (default: 1000). Checked
            against the ZIP directory's declared size before decompression.
        max_tables_per_stream: Maximum number of tables per stream (default: 10000).
            Real NGB streams typically contain fewer than ~1000 tables.
        max_array_size_mb: Maximum size in MB of a single data payload
            decoded into a column array (default: 500).
    """

    max_stream_size_mb: int = 1000
    max_tables_per_stream: int = 10000
    max_array_size_mb: int = 500

    def __post_init__(self) -> None:
        """Validate parsing configuration."""
        if self.max_stream_size_mb <= 0:
            raise ValueError("max_stream_size_mb must be positive")
        if self.max_tables_per_stream <= 0:
            raise ValueError("max_tables_per_stream must be positive")
        if self.max_array_size_mb <= 0:
            raise ValueError("max_array_size_mb must be positive")
