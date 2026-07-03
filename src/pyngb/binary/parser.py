"""
Low-level binary parsing operations for NGB files.
"""

import logging
import re
import struct

import numpy as np
import numpy.typing as npt

from ..config import ParsingConfig
from ..constants import BinaryMarkers, BinaryProcessing, DataType
from ..exceptions import NGBResourceLimitError
from .handlers import DataTypeRegistry

__all__ = ["BinaryParser"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class BinaryParser:
    """Handles binary data parsing operations.

    This class provides low-level binary parsing functionality for NGB files,
    including table splitting and value parsing.

    The parser maintains compiled regex patterns for performance and includes
    a pluggable data type registry for extensibility.

    Example:
        >>> parser = BinaryParser()
        >>> tables = parser.split_tables(binary_stream_data)

    Attributes:
        markers: Binary markers used for parsing
        _compiled_patterns: Cache of compiled regex patterns
        _data_type_registry: Registry of data type handlers
    """

    def __init__(
        self,
        markers: BinaryMarkers | None = None,
        parsing_config: ParsingConfig | None = None,
    ):
        self.markers = markers or BinaryMarkers()
        self.binary_config = BinaryProcessing()
        self.parsing_config = parsing_config or ParsingConfig()
        self._compiled_patterns: dict[str, re.Pattern[bytes]] = {}
        self._data_type_registry = DataTypeRegistry()

        # Precompile commonly used patterns for performance
        self._compiled_patterns["table_sep"] = re.compile(
            self.markers.TABLE_SEPARATOR, re.DOTALL
        )

    def _get_compiled_pattern(self, key: str, pattern: bytes) -> re.Pattern[bytes]:
        """Cache compiled regex patterns for performance."""
        pat = self._compiled_patterns.get(key)
        if pat is None:
            pat = re.compile(pattern, re.DOTALL)
            self._compiled_patterns[key] = pat
        return pat

    @staticmethod
    def _parse_string_enhanced(value: bytes) -> str | None:
        """Enhanced string parsing supporting multiple NETZSCH NGB string formats.

        Automatically detects and handles three formats found in NGB files:
        1. Standard: 4-byte length prefix + UTF-8 data
        2. Standard: 4-byte length prefix + UTF-16LE data (fallback)
        3. NETZSCH proprietary: fffeff + char_count + UTF-16LE data

        The parser tries formats in order of discovery frequency, with robust
        error handling and Unicode support including special characters.

        Args:
            value: Binary string data from NGB field payload

        Returns:
            Decoded string with null bytes stripped, or None if all parsing attempts fail

        Note:
            This method was enhanced through reverse engineering analysis to support
            the proprietary fffeff format discovered in NETZSCH instrument data.
        """
        if len(value) < 4:
            return None

        try:
            # Try fffeff format first (discovered through reverse engineering)
            if value.startswith(b"\xff\xfe\xff") and len(value) >= 4:
                char_count = value[3]
                if char_count == 0xFF:
                    # 0xff may be a length-escape for strings >255 chars; the
                    # encoding is unknown without a real long-comment file.
                    logger.warning(
                        "fffeff string with 0xff length byte; value may be a "
                        "truncated long string"
                    )
                expected_bytes = 4 + (
                    char_count * 2
                )  # 4 header + char_count UTF-16LE chars
                if len(value) >= expected_bytes:
                    string_bytes = value[4:expected_bytes]
                    try:
                        decoded = string_bytes.decode(
                            "utf-16le", errors="ignore"
                        ).strip("\x00")
                        if decoded:  # Only return if we got meaningful text
                            return decoded
                    except UnicodeDecodeError:
                        pass

            # Try standard format (4-byte length prefix)
            length = struct.unpack("<I", value[:4])[0]
            if length <= len(value) - 4 and length > 0:
                string_bytes = value[4 : 4 + length]

                # Strict UTF-8 first: a UTF-16LE payload contains bytes that
                # are invalid UTF-8, so it falls through to the UTF-16LE
                # branch instead of being silently stripped to ASCII.
                try:
                    decoded = string_bytes.decode("utf-8").strip().replace("\x00", "")
                    if decoded:
                        return decoded
                except UnicodeDecodeError:
                    try:
                        decoded = string_bytes.decode("utf-16le").strip("\x00")
                        if decoded:
                            return decoded
                    except UnicodeDecodeError:
                        logger.debug(
                            "String payload is neither valid UTF-8 nor UTF-16LE"
                        )

        except (struct.error, IndexError):
            pass

        return None

    @staticmethod
    def parse_value(data_type: bytes, value: bytes) -> int | float | str | bytes | None:
        """Parse binary value based on data type.

        Args:
            data_type: Data type identifier from DataType enum
            value: Binary data to parse

        Returns:
            Parsed value (int for INT32, float for FLOAT32/64, str for STRING,
            raw bytes for unknown types), or None if parsing fails.

        Raises:
            ValueError: If data length doesn't match expected type size
        """
        try:
            if data_type == DataType.INT32.value:
                if len(value) != 4:
                    raise ValueError(f"INT32 requires 4 bytes, got {len(value)}")
                return int(struct.unpack("<i", value)[0])
            if data_type == DataType.FLOAT32.value:
                if len(value) != 4:
                    raise ValueError(f"FLOAT32 requires 4 bytes, got {len(value)}")
                return float(struct.unpack("<f", value)[0])
            if data_type == DataType.FLOAT64.value:
                if len(value) != 8:
                    raise ValueError(f"FLOAT64 requires 8 bytes, got {len(value)}")
                return float(struct.unpack("<d", value)[0])
            if data_type == DataType.STRING.value:
                return BinaryParser._parse_string_enhanced(value)
            return value
        except (struct.error, ValueError) as e:
            logger.debug(f"Failed to parse value: {e}")
            return None
        except Exception as e:
            # Broad catch keeps the parser robust against unexpected binary shapes,
            # but log at warning so silent corruption stays visible in normal use.
            logger.warning(f"Unexpected error parsing value: {e}")
            return None

    def itemsize(self, data_type: bytes) -> int | None:
        """Byte width of one element of ``data_type``, or None if unhandled."""
        return self._data_type_registry.itemsize(data_type)

    def parse_data(
        self, data_type: bytes, payload: bytes | memoryview
    ) -> npt.NDArray[np.float64]:
        """Decode a data payload into values, enforcing the array size limit.

        Args:
            data_type: Binary data type identifier
            payload: Raw array payload to decode

        Returns:
            Decoded float64 array

        Raises:
            NGBResourceLimitError: If the payload exceeds max_array_size_mb
            NGBDataTypeError: If no handler exists for the data type
        """
        max_bytes = self.parsing_config.max_array_size_mb * 1024 * 1024
        if len(payload) > max_bytes:
            raise NGBResourceLimitError(
                f"Data payload is {len(payload):,} bytes, exceeding "
                f"max_array_size_mb limit of {self.parsing_config.max_array_size_mb}"
            )
        return self._data_type_registry.parse_data(data_type, payload)

    def split_tables(self, data: bytes) -> list[bytes]:
        """Split binary data into tables using the known separator.

        NGB streams contain multiple tables separated by a specific byte
        sequence. This method efficiently splits the stream into individual
        tables for further processing.

        Args:
            data: Binary data from an NGB stream

        Returns:
            List of binary table data chunks

        Example:
            >>> stream_data = load_stream_from_ngb()
            >>> tables = parser.split_tables(stream_data)
            >>> print(f"Found {len(tables)} tables")
            Found 15 tables

        Note:
            If no separator is found, returns the entire data as a single table.
        """
        if not data:
            logger.debug("Empty data provided to split_tables")
            return []

        sep = self.markers.TABLE_SEPARATOR
        if not sep:
            # Defensive: if no separator configured, return the whole payload
            logger.debug("No table separator configured, returning single table")
            return [data]

        # Fast non-regex split using bytes.find to determine boundaries while
        # preserving the historical offset semantics.
        indices: list[int] = []
        search_pos = 0
        while True:
            idx = data.find(sep, search_pos)
            if idx == -1:
                break
            cut = idx + self.binary_config.TABLE_SPLIT_OFFSET
            if cut < 0:
                cut = 0
            indices.append(cut)
            # Continue searching after the separator
            search_pos = idx + len(sep)

        if not indices:
            logger.debug("No table separators found, returning single table")
            return [data]

        # Build table slices from computed boundaries; the last table runs to
        # the end of the data payload.
        ends = [*indices[1:], len(data)]
        tables = [data[i:j] for i, j in zip(indices, ends)]

        # Filter out empty tables
        valid_tables = [table for table in tables if table]

        max_tables = self.parsing_config.max_tables_per_stream
        if len(valid_tables) > max_tables:
            raise NGBResourceLimitError(
                f"Stream contains {len(valid_tables)} tables, "
                f"exceeding max_tables_per_stream limit of {max_tables}"
            )

        logger.debug(f"Split data into {len(valid_tables)} valid tables")

        return valid_tables
