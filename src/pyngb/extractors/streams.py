"""
Data stream processing for NGB measurement data.
"""

from __future__ import annotations

import logging
from itertools import tee, zip_longest

import polars as pl

from polars.exceptions import ShapeError  # type: ignore[import-untyped]

from ..binary import BinaryParser
from ..constants import BinaryProcessing, PatternConfig, StreamMarkers
from ..exceptions import NGBDataTypeError

__all__ = ["DataStreamProcessor"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataStreamProcessor:
    """Processes data streams from NGB files with optimized parsing."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        self.config = config
        self.parser = parser
        self.binary_config = BinaryProcessing()
        self.stream_markers = StreamMarkers()
        self._table_sep_re = self.parser._get_compiled_pattern(
            "table_sep", self.parser.markers.TABLE_SEPARATOR
        )

    def _split_tables(self, stream_data: bytes) -> list[bytes]:
        """Split a stream into tables using the cached table-separator pattern.

        Mirrors the existing splitting logic used elsewhere in the codebase to
        avoid behavioral drift while keeping this class self-contained.
        """
        indices = [m.start() - 2 for m in self._table_sep_re.finditer(stream_data)]
        start, end = tee(indices)
        next(end, None)
        tables = [stream_data[i:j] for i, j in zip_longest(start, end)]
        return [t for t in tables if t]

    # --- Stream 2 ---
    def process_stream_2(self, stream_data: bytes) -> pl.DataFrame:
        """Process primary data stream (stream_2)."""
        # Split into tables - preserve original splitting behavior
        stream_table = self._split_tables(stream_data)

        output: list[float] = []
        output_polars = pl.DataFrame()
        title: str | None = None

        col_map = self.config.column_map
        markers = self.parser.markers

        for table in stream_table:
            # Check for header marker
            header_slice = table[
                self.stream_markers.STREAM2_HEADER_POS : self.stream_markers.STREAM2_HEADER_POS
                + len(self.stream_markers.STREAM2_HEADER)
            ]
            if header_slice == self.stream_markers.STREAM2_HEADER:
                title = table[0:1].hex()
                title = col_map.get(title, title)
                if len(output) > 1:
                    try:
                        output_polars = output_polars.with_columns(
                            pl.Series(name=title, values=output)
                        )
                    except ShapeError:
                        logger.debug(f"Shape mismatch when adding column '{title}'")
                output = []

            # Check for data marker
            data_slice = table[
                self.stream_markers.DATA_MARKER_POS : self.stream_markers.DATA_MARKER_POS
                + len(self.stream_markers.STREAM2_DATA)
            ]
            if data_slice == self.stream_markers.STREAM2_DATA:
                start_idx = table.find(markers.START_DATA)
                if start_idx == -1:
                    logger.debug("START_DATA marker not found in table - skipping")
                    continue

                payload_start = start_idx + self.binary_config.START_DATA_HEADER_OFFSET
                data = table[payload_start:]
                end_data = data.find(markers.END_DATA)
                if end_data == -1:
                    logger.debug("END_DATA marker not found in table - skipping")
                    continue

                data = data[:end_data]
                # Data type byte immediately precedes START_DATA
                if start_idx <= 0:
                    logger.debug(
                        "No data type byte found before START_DATA - skipping table"
                    )
                    continue
                data_type = table[start_idx - 1 : start_idx]

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
        # Split into tables - preserve original splitting behavior
        stream_table = self._split_tables(stream_data)

        output: list[float] = []
        output_polars = existing_df
        title: str | None = None

        col_map = self.config.column_map
        markers = self.parser.markers

        for table in stream_table:
            # Check for Stream 3 header marker
            header_slice = table[
                self.stream_markers.STREAM3_HEADER_POS : self.stream_markers.STREAM3_HEADER_POS
                + len(self.stream_markers.STREAM3_HEADER)
            ]
            if header_slice == self.stream_markers.STREAM3_HEADER:
                title = table[0:1].hex()
                title = col_map.get(title, title)
                output = []

            # Check for data marker (same as Stream 2)
            data_slice = table[
                self.stream_markers.DATA_MARKER_POS : self.stream_markers.DATA_MARKER_POS
                + len(self.stream_markers.STREAM3_DATA)
            ]
            if data_slice == self.stream_markers.STREAM3_DATA:
                start_idx = table.find(markers.START_DATA)
                if start_idx == -1:
                    logger.debug("START_DATA marker not found in table - skipping")
                    continue

                payload_start = start_idx + self.binary_config.START_DATA_HEADER_OFFSET
                data = table[payload_start:]
                end_data = data.find(markers.END_DATA)
                if end_data == -1:
                    logger.debug("END_DATA marker not found in table - skipping")
                    continue

                data = data[:end_data]
                if start_idx <= 0:
                    logger.debug(
                        "No data type byte found before START_DATA - skipping table"
                    )
                    continue
                data_type = table[start_idx - 1 : start_idx]

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
