"""
Data stream processing for NGB measurement data.
"""

from __future__ import annotations

import logging
from itertools import tee, zip_longest
from typing import List, Optional

import polars as pl

try:
    from polars.exceptions import ShapeError  # type: ignore[import]
except ImportError:
    # Fallback for older versions of polars
    ShapeError = ValueError

from ..binary import BinaryParser
from ..constants import PatternConfig
from ..exceptions import NGBDataTypeError

__all__ = ["DataStreamProcessor"]

logger = logging.getLogger(__name__)


class DataStreamProcessor:
    """Processes data streams from NGB files with optimized parsing."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        self.config = config
        self.parser = parser
        self._table_sep_re = self.parser._get_compiled_pattern(
            "table_sep", self.parser.markers.TABLE_SEPARATOR
        )

    # --- Stream 2 ---
    def process_stream_2(self, stream_data: bytes) -> pl.DataFrame:
        """Process primary data stream (stream_2)."""
        # Split into tables - exact original logic
        indices = [m.start() - 2 for m in self._table_sep_re.finditer(stream_data)]
        start, end = tee(indices)
        next(end, None)
        stream_table = [stream_data[i:j] for i, j in zip_longest(start, end)]

        output: List[float] = []
        output_polars = pl.DataFrame()
        title: Optional[str] = None

        col_map = self.config.column_map
        markers = self.parser.markers

        for table in stream_table:
            if table[1:2] == b"\x17":  # header
                title = table[0:1].hex()
                title = col_map.get(title, title)
                if len(output) > 1:
                    try:
                        output_polars = output_polars.with_columns(
                            pl.Series(name=title, values=output)
                        )
                    except ShapeError:
                        logger.debug("Shape mismatch when adding column '%s'", title)
                output = []

            if table[1:2] == b"\x75":  # data
                start_data = table.find(markers.START_DATA) + 6
                if start_data == 5:  # find() returned -1
                    logger.debug("START_DATA marker not found in table - skipping")
                    continue

                data = table[start_data:]
                end_data = data.find(markers.END_DATA)
                if end_data == -1:
                    logger.debug("END_DATA marker not found in table - skipping")
                    continue

                data = data[:end_data]
                data_type = table[start_data - 7 : start_data - 6]

                try:
                    parsed_data = self.parser._data_type_registry.parse_data(
                        data_type, data
                    )
                    output.extend(parsed_data)
                except NGBDataTypeError as e:
                    logger.debug(f"Failed to parse data: {e}")
                    continue

        return output_polars

    # --- Stream 3 ---
    def process_stream_3(
        self, stream_data: bytes, existing_df: pl.DataFrame
    ) -> pl.DataFrame:
        """Process secondary data stream (stream_3)."""
        # Split into tables - exact original logic
        indices = [m.start() - 2 for m in self._table_sep_re.finditer(stream_data)]
        start, end = tee(indices)
        next(end, None)
        stream_table = [stream_data[i:j] for i, j in zip_longest(start, end)]

        output: List[float] = []
        output_polars = existing_df
        title: Optional[str] = None

        col_map = self.config.column_map
        markers = self.parser.markers

        for table in stream_table:
            if table[22:25] == b"\x80\x22\x2b":  # header
                title = table[0:1].hex()
                title = col_map.get(title, title)
                output = []

            if table[1:2] == b"\x75":  # data
                start_data = table.find(markers.START_DATA) + 6
                if start_data == 5:  # find() returned -1
                    logger.debug("START_DATA marker not found in table - skipping")
                    continue

                data = table[start_data:]
                end_data = data.find(markers.END_DATA)
                if end_data == -1:
                    logger.debug("END_DATA marker not found in table - skipping")
                    continue

                data = data[:end_data]
                data_type = table[start_data - 7 : start_data - 6]

                try:
                    parsed_data = self.parser._data_type_registry.parse_data(
                        data_type, data
                    )
                    output.extend(parsed_data)
                except NGBDataTypeError as e:
                    logger.debug(f"Failed to parse data: {e}")
                    continue

                # Save after each data block (original behavior)
                try:
                    output_polars = output_polars.with_columns(
                        pl.Series(name=title, values=output)
                    )
                except ShapeError:
                    # Silently ignore shape issues as before
                    pass

        return output_polars
