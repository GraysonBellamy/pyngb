"""
Low-level binary parsing operations for NGB files.
"""

from __future__ import annotations

import logging
import re
import struct
from typing import Any

from ..constants import BinaryMarkers, BinaryProcessing, DataType
from .handlers import DataTypeRegistry

__all__ = ["BinaryParser"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class BinaryParser:
    """Handles binary data parsing operations with memory optimization.

    This class provides low-level binary parsing functionality for NGB files,
    including table splitting, data extraction, and value parsing. It uses
    memory-efficient techniques like memoryview to minimize copying.

    The parser maintains compiled regex patterns for performance and includes
    a pluggable data type registry for extensibility.

    Example:
        >>> parser = BinaryParser()
        >>> tables = parser.split_tables(binary_stream_data)
        >>> data = parser.extract_data_array(tables[0], DataType.FLOAT64.value)
        >>> [1.0, 2.0, 3.0, ...]

    Attributes:
        markers: Binary markers used for parsing
        _compiled_patterns: Cache of compiled regex patterns
        _data_type_registry: Registry of data type handlers

    Performance Notes:
        - Uses memoryview to avoid unnecessary memory copies
        - Caches compiled regex patterns for repeated use
        - Leverages NumPy frombuffer for fast array parsing
    """

    def __init__(self, markers: BinaryMarkers | None = None):
        self.markers = markers or BinaryMarkers()
        self.binary_config = BinaryProcessing()
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

                # Try UTF-8 first (as documented)
                try:
                    decoded = (
                        string_bytes.decode("utf-8", errors="ignore")
                        .strip()
                        .replace("\x00", "")
                    )
                    if decoded:
                        return decoded
                except UnicodeDecodeError:
                    pass

                # Try UTF-16LE fallback
                try:
                    decoded = string_bytes.decode("utf-16le", errors="ignore").strip(
                        "\x00"
                    )
                    if decoded:
                        return decoded
                except UnicodeDecodeError:
                    pass

        except (struct.error, IndexError):
            pass

        return None

    @staticmethod
    def parse_value(data_type: bytes, value: bytes) -> Any:
        """Parse binary value based on data type.

        Args:
            data_type: Data type identifier from DataType enum
            value: Binary data to parse

        Returns:
            Parsed value or None if parsing fails

        Raises:
            ValueError: If data length doesn't match expected type size
        """
        try:
            if data_type == DataType.INT32.value:
                if len(value) != 4:
                    raise ValueError(f"INT32 requires 4 bytes, got {len(value)}")
                return struct.unpack("<i", value)[0]
            if data_type == DataType.FLOAT32.value:
                if len(value) != 4:
                    raise ValueError(f"FLOAT32 requires 4 bytes, got {len(value)}")
                return struct.unpack("<f", value)[0]
            if data_type == DataType.FLOAT64.value:
                if len(value) != 8:
                    raise ValueError(f"FLOAT64 requires 8 bytes, got {len(value)}")
                return struct.unpack("<d", value)[0]
            if data_type == DataType.STRING.value:
                return BinaryParser._parse_string_enhanced(value)
            return value
        except (struct.error, ValueError) as e:
            logger.debug(f"Failed to parse value: {e}")
            return None
        except Exception as e:
            logger.debug(f"Unexpected error parsing value: {e}")
            return None

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
        logger.debug(f"Split data into {len(valid_tables)} valid tables")

        return valid_tables

    def handle_corrupted_data(self, data: bytes, context: str = "") -> list[float]:
        """Handle corrupted or malformed data gracefully.

        Args:
            data: Potentially corrupted binary data
            context: Context information for logging

        Returns:
            Empty list for corrupted data
        """
        logger.warning(f"Handling corrupted data in {context}: {len(data)} bytes")

        # Try to recover partial data if possible
        if len(data) >= self.binary_config.MIN_FLOAT64_BYTES:
            try:
                # Try to extract what we can
                return self._data_type_registry.parse_data(
                    DataType.FLOAT64.value,
                    memoryview(data)[: self.binary_config.MIN_FLOAT64_BYTES],
                )
            except Exception as exc:
                logger.debug(f"Failed partial parse in handle_corrupted_data: {exc}")

        return []

    def validate_data_integrity(self, table: bytes) -> bool:
        """Validate that a table has proper START_DATA and END_DATA markers.

        Args:
            table: Binary table data to validate

        Returns:
            True if table has valid structure, False otherwise
        """
        has_start = self.markers.START_DATA in table
        has_end = self.markers.END_DATA in table

        if not has_start:
            logger.debug("Table missing START_DATA marker")
            return False

        if not has_end:
            logger.debug("Table missing END_DATA marker")
            return False

        # Check that END_DATA comes after START_DATA
        start_pos = table.find(self.markers.START_DATA)
        end_pos = table.find(self.markers.END_DATA)

        if end_pos <= start_pos:
            logger.debug("END_DATA marker appears before or at START_DATA position")
            return False

        return True

    def extract_data_array(self, table: bytes, data_type: bytes) -> list[float]:
        """Extract array of numerical data with memory optimization.

        Extracts arrays of floating-point data from binary tables using
        efficient memory operations and NumPy for fast conversion.

        Args:
            table: Binary table data containing the array
            data_type: Data type identifier (from DataType enum)

        Returns:
            List of floating-point values, empty list if no data found

        Raises:
            NGBDataTypeError: If data type is not supported

        Example:
            >>> table_data = get_table_from_stream()
            >>> values = parser.extract_data_array(table_data, DataType.FLOAT64.value)
            >>> print(f"Extracted {len(values)} data points")
            Extracted 1500 data points

        Performance:
            Uses NumPy frombuffer which is 10-50x faster than struct.iter_unpack
            for large arrays.
        """
        # Validate table structure first
        if not self.validate_data_integrity(table):
            return []

        # Find data boundaries efficiently
        start_idx = table.find(self.markers.START_DATA)
        if start_idx == -1:
            logger.debug("START_DATA marker not found in table")
            return []

        # Advance to the first byte after the START_DATA header
        # The payload begins after the START_DATA marker offset to skip
        # marker and header bytes present in the stream format.
        start_idx += self.binary_config.START_DATA_HEADER_OFFSET

        # Find end marker in the remaining data
        end_idx = table.find(self.markers.END_DATA, start_idx)
        if end_idx == -1:
            logger.debug("END_DATA marker not found in table")
            return []

        # Extract data chunk efficiently using memoryview to avoid copying
        data_chunk = memoryview(table)[start_idx:end_idx]

        # Validate data chunk is not empty
        if not data_chunk:
            logger.debug("Data chunk is empty")
            return []

        # Use pluggable data type registry
        try:
            return self._data_type_registry.parse_data(data_type, data_chunk)
        except Exception as e:
            # Fallback to empty list for unknown data types
            logger.debug(f"Failed to parse data with type {data_type.hex()}: {e}")
            return []
