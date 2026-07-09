"""Measurement-data assembly: streams 2 and 3 into a Polars frame.

The data streams are sequences of tables driven by their type refs: a
channel-header table (type_ref 0x2B22, category low byte = channel id) is
followed by one value table per measurement segment (type_ref 0x2B23), each
carrying exactly one data array — field 0x0F40 (f64) for f64 channels or
0x0F3D (f32) for f32 channels. Segment arrays concatenate in stream order;
tables with any other type ref are structural and ignored.

Data streams are load-bearing, so unlike metadata extraction the policy here
is strict: any malformed or truncated span in stream 2/3 is fatal before
assembly begins, as is data preceding a header or a channel whose length
disagrees with the rest of the frame.
"""

from __future__ import annotations

import logging

import numpy as np
import numpy.typing as npt
import polars as pl

from ..exceptions import NGBCorruptedFileError
from .document import NGBDocument, Table
from .maps import (
    CHANNEL_HEADER_TYPE,
    DATA_FIELDS,
    SEGMENT_VALUES_TYPE,
    channel_name,
)

__all__ = ["build_dataframe"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

#: The measurement streams, in merge order (stream 3 is optional).
_DATA_STREAMS = (2, 3)


def _minutes_to_seconds(values: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    """NGB stores the time channel in minutes; pyngb's public API exposes
    seconds throughout. This is the single place the conversion happens."""
    return values * 60.0


def _data_array(table: Table) -> npt.NDArray[np.float64] | None:
    """The one data array of a segment-value table, decoded to float64."""
    for entry in table.fields.values():
        if (entry.field_id, entry.dtype) in DATA_FIELDS and entry.element_count:
            decoded = entry.array()
            if isinstance(decoded, np.ndarray):  # always true: f32/f64 fields
                return decoded
    return None


def _assemble_stream(
    doc: NGBDocument, stream_id: int, frame: pl.DataFrame
) -> pl.DataFrame:
    chunks: list[npt.NDArray[np.float64]] = []
    title: str | None = None

    def flush() -> pl.DataFrame:
        nonlocal chunks, frame
        if chunks:
            values = np.concatenate(chunks)
            if title is None:
                raise NGBCorruptedFileError(
                    f"stream_{stream_id}: {len(values)} data values precede "
                    "any channel header",
                    stream=stream_id,
                )
            if title in frame.columns:
                logger.warning(
                    f"Channel '{title}' appears more than once; "
                    "overwriting the earlier column"
                )
            if title == "time":
                values = _minutes_to_seconds(values)
            if frame.width and len(values) != frame.height:
                raise NGBCorruptedFileError(
                    f"channel '{title}' has {len(values)} values but the "
                    f"frame has {frame.height} rows",
                    stream=stream_id,
                    declared=len(values),
                    available=frame.height,
                )
            frame = frame.with_columns(pl.Series(name=title, values=values))
        chunks = []
        return frame

    for table in doc.tables_of(stream_id):
        if table.type_ref == CHANNEL_HEADER_TYPE:
            frame = flush()
            title = channel_name(table.category)
        elif table.type_ref == SEGMENT_VALUES_TYPE:
            values = _data_array(table)
            if values is not None and len(values):
                if title is None:
                    raise NGBCorruptedFileError(
                        f"stream_{stream_id} table {table.index}: data values "
                        "precede any channel header",
                        stream=stream_id,
                        table_index=table.index,
                    )
                chunks.append(values)
        # Any other type ref is a structural table; ignored, no flush.

    # Real files end with a data-less trailing header, but a stream must not
    # depend on it to emit its last column.
    return flush()


def build_dataframe(doc: NGBDocument) -> pl.DataFrame:
    """Assemble the measurement data of streams 2 and 3 into one frame.

    Raises:
        NGBCorruptedFileError: A data stream tokenized with malformed or
            truncated spans, data values precede any channel header, or a
            channel's length disagrees with the rest of the frame.
    """
    for stream_id in _DATA_STREAMS:
        if stream_id not in doc.streams:
            continue
        defects = doc.defects(stream_id)
        if defects:
            first = defects[0]
            raise NGBCorruptedFileError(
                f"stream_{stream_id} contains {len(defects)} {first.kind} "
                f"span(s); first at offset {first.start} - refusing to "
                "assemble measurement data from a damaged stream",
                stream=stream_id,
                offset=first.start,
            )

    frame = pl.DataFrame()
    for stream_id in _DATA_STREAMS:
        if stream_id in doc.streams:
            frame = _assemble_stream(doc, stream_id, frame)
    return frame
