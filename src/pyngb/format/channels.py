"""Measurement-data assembly: streams 2 and 3 into a Polars frame.

The data streams are sequences of tables driven by their type refs: a
channel-header table (type_ref 0x2B22, category low byte = channel id) is
followed by one value table per measurement segment (type_ref 0x2B23), each
carrying exactly one data array — field 0x0F40 (f64) for f64 channels or
0x0F3D (f32) for f32 channels. Segment arrays concatenate in stream order;
tables with any other type ref are structural and ignored.

A stream may carry more than one *run* — a complete, independent measurement
with its own full set of channels. ``.ngb-ss3``/``.ngb-bs3`` files hold one
run; "Sample + Correction" ``.ngb-ds3`` files hold two back-to-back in the
same streams: the raw sample measurement first, then a verbatim copy of the
correction measurement it was run against. A run boundary is where a channel
header repeats a channel already seen in the current run of that stream;
:func:`count_runs` reports the (cross-stream validated) run count and
:func:`build_dataframe` assembles exactly one run.

Data streams are load-bearing, so unlike metadata extraction the policy here
is strict: any malformed or truncated span in stream 2/3 is fatal before
assembly begins, as is data preceding a header, a channel whose length
disagrees with the rest of the frame, runs within a stream whose channel
signatures disagree, or data streams that disagree on the run count.
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

__all__ = ["build_dataframe", "count_runs"]

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


def _split_runs(tables: tuple[Table, ...]) -> list[list[Table]]:
    """Partition one stream's tables into measurement runs.

    A new run starts at a channel-header table whose channel (category) has
    already opened in the current run: within a run each channel's header
    appears exactly once, so a repeat can only be the next run. Structural
    tables between the last value table of one run and the first header of
    the next carry no channel data and stay with the earlier run, where
    assembly ignores them.
    """
    runs: list[list[Table]] = [[]]
    seen: set[int] = set()
    for table in tables:
        if table.type_ref == CHANNEL_HEADER_TYPE:
            if table.category in seen:
                runs.append([])
                seen = set()
            seen.add(table.category)
        runs[-1].append(table)
    return runs


def _stream_run_count(doc: NGBDocument, stream_id: int) -> int:
    """Runs in one stream; 0 when the stream has no channel headers at all.

    A repeated channel header alone does not prove a second run: a genuine
    Sample + Correction file repeats the FULL channel sequence verbatim, so
    every run must carry the same channel signature. A stray duplicate
    header (one channel repeating mid-stream) fails that check and raises
    instead of silently truncating run 0 at the duplicate.
    """
    tables = doc.tables_of(stream_id)
    if not any(t.type_ref == CHANNEL_HEADER_TYPE for t in tables):
        return 0
    runs = _split_runs(tables)
    signatures = [
        tuple(t.category for t in run if t.type_ref == CHANNEL_HEADER_TYPE)
        for run in runs
    ]
    if len(set(signatures)) > 1:
        shown = " vs ".join(
            "(" + ", ".join(f"0x{c:04X}" for c in sig) + ")"
            for sig in dict.fromkeys(signatures)
        )
        raise NGBCorruptedFileError(
            f"stream_{stream_id}: measurement runs disagree on their channel "
            f"signature: {shown}; a stray duplicate channel header is "
            "corruption, not a second run"
        )
    return len(runs)


def count_runs(doc: NGBDocument) -> int:
    """The number of measurement runs in the document's data streams.

    Returns 1 for ``.ngb-ss3``/``.ngb-bs3`` files, 2 for "Sample +
    Correction" ``.ngb-ds3`` files, and 0 when no data stream carries any
    channel data.

    Raises:
        NGBCorruptedFileError: Runs within a stream disagree on their
            channel signature, or the data streams disagree on the run
            count — runs could not be paired.
    """
    counts = {
        stream_id: n
        for stream_id in _DATA_STREAMS
        if stream_id in doc.streams and (n := _stream_run_count(doc, stream_id))
    }
    if not counts:
        return 0
    if len(set(counts.values())) > 1:
        raise NGBCorruptedFileError(
            "data streams disagree on the measurement-run count: "
            + ", ".join(f"stream_{sid} has {n}" for sid, n in counts.items())
        )
    return next(iter(counts.values()))


def _assemble_stream(
    doc: NGBDocument, stream_id: int, run: int, frame: pl.DataFrame
) -> pl.DataFrame:
    runs = _split_runs(doc.tables_of(stream_id))
    tables = runs[run] if run < len(runs) else ()
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

    for table in tables:
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


def build_dataframe(doc: NGBDocument, *, run: int = 0) -> pl.DataFrame:
    """Assemble one measurement run of streams 2 and 3 into a frame.

    Args:
        doc: The parsed document.
        run: Which run to assemble, in stream order. Run 0 is the (only) run
            of an ``.ngb-ss3``/``.ngb-bs3`` file and the raw sample
            measurement of an ``.ngb-ds3``; run 1 is the ``.ngb-ds3``'s
            embedded correction measurement.

    Raises:
        ValueError: ``run`` is negative or not present in the file.
        NGBCorruptedFileError: A data stream tokenized with malformed or
            truncated spans, data values precede any channel header, a
            channel's length disagrees with the rest of the frame, runs
            within a stream disagree on their channel signature, or the
            data streams disagree on the run count.
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

    n_runs = count_runs(doc)
    if run < 0 or (run > 0 and run >= max(n_runs, 1)):
        raise ValueError(
            f"run {run} requested but the file contains {n_runs} measurement run(s)"
        )

    frame = pl.DataFrame()
    for stream_id in _DATA_STREAMS:
        if stream_id in doc.streams:
            frame = _assemble_stream(doc, stream_id, run, frame)
    return frame
