"""
Data stream processing for NGB measurement data.
"""

import logging

import polars as pl

from polars.exceptions import ShapeError

from ..binary import BinaryParser
from ..constants import BinaryProcessing, PatternConfig, StreamMarkers
from ..exceptions import NGBCorruptedFileError, NGBDataTypeError

__all__ = ["DataStreamProcessor"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataStreamProcessor:
    """Processes data streams from NGB files with optimized parsing.

    Structural corruption (a truncated data table, a payload that disagrees
    with its declared element count, an unknown data type, or a channel whose
    length does not match the rest of the frame) raises
    :class:`~pyngb.exceptions.NGBCorruptedFileError` rather than silently
    producing a frame with missing or wrong columns.
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        self.config = config
        self.parser = parser
        self.binary_config = BinaryProcessing()
        self.stream_markers = StreamMarkers()

    @staticmethod
    def _standardize_column_values(
        title: str | None, values: list[float]
    ) -> list[float]:
        """Convert raw instrument values to pyngb's public units."""
        if title == "time":
            # NGB stream time values are stored in minutes. pyngb exposes seconds
            # throughout its public API so rates such as DTG are unambiguous.
            return [value * 60.0 for value in values]
        return values

    def _extract_data_values(
        self, table: bytes, table_index: int
    ) -> list[float] | None:
        """Decode the data payload of a table carrying the data marker.

        Returns None for structural tables that carry the marker byte but no
        payload (the 90-byte category tables present in every file). Raises
        NGBCorruptedFileError when a payload exists but is structurally
        inconsistent.
        """
        markers = self.parser.markers

        start_idx = table.find(markers.START_DATA)
        if start_idx == -1:
            return None

        payload = table[start_idx + self.binary_config.START_DATA_HEADER_OFFSET :]
        end_idx = payload.find(markers.END_DATA)
        if end_idx == -1:
            raise NGBCorruptedFileError(
                f"data table {table_index}: START_DATA without END_DATA - "
                "stream is truncated or corrupt"
            )
        payload = payload[:end_idx]

        # START_DATA is followed by a u32 LE element count. Validating it
        # against the payload length catches truncation and START_DATA
        # false-matches that would otherwise yield short or garbage columns.
        count = int.from_bytes(table[start_idx + 2 : start_idx + 6], "little")
        data_type = table[start_idx - 1 : start_idx]
        itemsize = self.parser.itemsize(data_type)
        if itemsize is not None and len(payload) != count * itemsize:
            raise NGBCorruptedFileError(
                f"data table {table_index}: payload is {len(payload)} bytes but "
                f"the count field declares {count} values of {itemsize} bytes"
            )

        try:
            return self.parser.parse_data(data_type, payload)
        except NGBDataTypeError as e:
            raise NGBCorruptedFileError(f"data table {table_index}: {e}") from e

    def _process_stream(
        self,
        stream_data: bytes,
        header_marker: bytes,
        header_pos: int,
        initial_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """Decode one measurement stream into columns of ``initial_df``.

        A channel's header table precedes its data tables, and data tables
        carry no channel identity of their own. Data accumulated in ``output``
        therefore always belongs to the channel named by the *most recent*
        header, and is flushed under that name when the next header (or the
        end of the stream) is reached.
        """
        stream_table = self.parser.split_tables(stream_data)

        output: list[float] = []
        frame = initial_df
        title: str | None = None
        col_map = self.config.column_map

        def flush_channel() -> None:
            nonlocal output, frame
            if output:
                if title is None:
                    raise NGBCorruptedFileError(
                        f"{len(output)} data values precede any channel header"
                    )
                if title in frame.columns:
                    logger.warning(
                        f"Channel '{title}' appears more than once; "
                        "overwriting the earlier column"
                    )
                try:
                    values = self._standardize_column_values(title, output)
                    frame = frame.with_columns(pl.Series(name=title, values=values))
                except ShapeError as e:
                    raise NGBCorruptedFileError(
                        f"channel '{title}' has {len(output)} values but the "
                        f"frame has {frame.height} rows"
                    ) from e
            output = []

        data_marker = self.stream_markers.STREAM2_DATA
        data_pos = self.stream_markers.DATA_MARKER_POS

        for index, table in enumerate(stream_table):
            if table[header_pos : header_pos + len(header_marker)] == header_marker:
                flush_channel()
                channel_id = table[0:1].hex()
                title = col_map.get(channel_id, channel_id)

            if table[data_pos : data_pos + len(data_marker)] == data_marker:
                values = self._extract_data_values(table, index)
                if values is not None:
                    output.extend(values)

        # Real files end with data-less trailing headers, but a stream must
        # not depend on them to emit its last column.
        flush_channel()

        return frame

    def process_stream_2(self, stream_data: bytes) -> pl.DataFrame:
        """Process primary data stream (stream_2)."""
        return self._process_stream(
            stream_data,
            self.stream_markers.STREAM2_HEADER,
            self.stream_markers.STREAM2_HEADER_POS,
            pl.DataFrame(),
        )

    def process_stream_3(
        self, stream_data: bytes, existing_df: pl.DataFrame
    ) -> pl.DataFrame:
        """Process secondary data stream (stream_3), merging into existing_df."""
        return self._process_stream(
            stream_data,
            self.stream_markers.STREAM3_HEADER,
            self.stream_markers.STREAM3_HEADER_POS,
            existing_df,
        )
