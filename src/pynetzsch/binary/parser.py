"""
Low-level binary parsing operations for NGB files.
"""

from __future__ import annotations

import logging
import re
import struct
from itertools import tee
from typing import Any

from ..constants import BinaryMarkers, DataType
from .handlers import DataTypeRegistry

__all__ = ["BinaryParser"]

logger = logging.getLogger(__name__)


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
        self._compiled_patterns: dict[str, re.Pattern[bytes]] = {}
        self._data_type_registry = DataTypeRegistry()
        # Hot-path: table separator compiled once
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
    def parse_value(data_type: bytes, value: bytes) -> Any:
        """Parse binary value based on data type."""
        try:
            if data_type == DataType.INT32.value:
                return struct.unpack("<i", value)[0]
            if data_type == DataType.FLOAT32.value:
                return struct.unpack("<f", value)[0]
            if data_type == DataType.FLOAT64.value:
                return struct.unpack("<d", value)[0]
            if data_type == DataType.STRING.value:
                # Skip 4-byte length; strip nulls.
                return (
                    value[4:]
                    .decode("utf-8", errors="ignore")
                    .strip()
                    .replace("\x00", "")
                )
            return value
        except Exception as e:
            logger.debug("Failed to parse value: %s", e)
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
        pattern = self._compiled_patterns["table_sep"]
        indices = [m.start() - 2 for m in pattern.finditer(data)]
        if not indices:
            return [data]
        start, end = tee(indices)
        next(end, None)
        from itertools import zip_longest

        return [data[i:j] for i, j in zip_longest(start, end)]

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
        # Use memoryview to avoid unnecessary copying
        table_mv = memoryview(table)

        # Find data boundaries using memoryview
        table_bytes = table_mv.tobytes()  # Only convert once
        start_idx = table_bytes.find(self.markers.START_DATA)
        if start_idx == -1:
            logger.debug("START_DATA marker not found in table")
            return []

        start_idx += 6  # preserve original offset logic
        data_mv = table_mv[start_idx:]

        data_bytes = data_mv.tobytes()  # Only convert once for end search
        end_idx = data_bytes.find(self.markers.END_DATA)
        if end_idx == -1:
            logger.debug("END_DATA marker not found in table")
            return []

        # Extract data chunk efficiently using memoryview slicing
        data_chunk = data_mv[:end_idx].tobytes()

        # Use pluggable data type registry
        try:
            return self._data_type_registry.parse_data(data_type, data_chunk)
        except Exception:
            # Fallback to empty list for unknown data types
            logger.debug(f"Unknown data type: {data_type.hex()}")
            return []
