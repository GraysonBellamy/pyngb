"""The parsed-document layer: streams of tables of decoded fields.

One pass of the strict tokenizer over each section of each stream assembles
the token sequence into :class:`Table` objects (a REF-scalar token whose
payload matches the table-open form starts a table; every following field
record belongs to it). The result, :class:`NGBDocument`, is the queryable
model that all metadata extraction and channel assembly run against —
extraction rules become keyed lookups, never byte scans.

Severity policy deliberately does not live here: assembly never raises on
malformed or truncated spans (they are retained per stream and surfaced via
:meth:`NGBDocument.has_defect`; the channel builder treats them as fatal for
data streams, metadata extraction ignores them). The only exceptions raised
are the container/limit errors from the layers below plus
:class:`NGBResourceLimitError` for a stream exceeding
``max_tables_per_stream``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
from typing import NamedTuple

import numpy as np
import numpy.typing as npt

from ..config import ParsingConfig
from ..exceptions import NGBResourceLimitError
from .container import StreamData, open_ngb
from .grammar import (
    DType,
    FieldToken,
    Mode,
    UnknownSpan,
    decode_array,
    decode_scalar,
    ref_class_name,
    ref_type_ref,
    tokenize,
)
from .maps import KNOWN_FIELD_IDS

__all__ = ["Field", "NGBDocument", "Table", "load_document"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ScalarValue = int | float | str | bytes | None


class Field(NamedTuple):
    """One decoded field record.

    Scalars are decoded eagerly into ``value``; array payloads stay as the
    zero-copy ``raw`` view until :meth:`array` is called (deliberately
    uncached — channel assembly consumes each array exactly once). A
    NamedTuple rather than a dataclass: one is built per record, and
    positional tuple construction is the difference between the document
    layer fitting its per-parse time budget or not.
    """

    field_id: int
    dtype: DType
    mode: Mode
    value: ScalarValue  # decoded scalar; None for arrays
    element_count: int | None  # arrays only
    raw: memoryview
    span: tuple[int, int]

    def array(self) -> npt.NDArray[np.float64] | bytes:
        """Decode an array payload (numeric dtypes -> float64, byte-oriented
        dtypes -> raw bytes). Scalars raise: ``value`` already holds them."""
        if self.mode != Mode.ARRAY:
            raise ValueError(f"field 0x{self.field_id:04X} is a scalar; use .value")
        return decode_array(self.dtype, self.raw)


@dataclass(frozen=True, slots=True)
class Table:
    """One serialized table: an open record, then uniquely-id'd fields.

    ``index`` is the table's ordinal within its stream — stream order is
    semantic (first-match-wins fields, occurrence-based classification, and
    channel segments all depend on it).
    """

    stream_id: int
    index: int
    category: int
    type_ref: int
    class_name: str | None  # inline class-definition opens only
    fields: dict[int, Field]  # insertion order == record order
    preamble: bool
    span: tuple[int, int]

    def get(self, field_id: int) -> Field | None:
        return self.fields.get(field_id)

    def value(self, field_id: int) -> ScalarValue:
        """The decoded scalar of ``field_id``, or None if absent/array."""
        entry = self.fields.get(field_id)
        return entry.value if entry is not None else None

    def has_fields(self, *field_ids: int) -> bool:
        return all(fid in self.fields for fid in field_ids)

    def strings(self) -> list[str]:
        """Decoded string-field values in record order."""
        return [
            entry.value
            for entry in self.fields.values()
            if entry.dtype == DType.STRING and isinstance(entry.value, str)
        ]


@dataclass(frozen=True, slots=True)
class NGBDocument:
    """Every stream of an NGB file, parsed into tables.

    Holds the :class:`StreamData` blobs so the zero-copy field views stay
    valid for the document's lifetime.
    """

    streams: dict[int, StreamData]
    tables: dict[int, tuple[Table, ...]]
    spans: dict[int, tuple[UnknownSpan, ...]]
    orphans: dict[int, tuple[Field, ...]]

    def tables_of(self, stream_id: int) -> tuple[Table, ...]:
        return self.tables.get(stream_id, ())

    def by_category(self, stream_id: int, category: int) -> Iterator[Table]:
        return (t for t in self.tables_of(stream_id) if t.category == category)

    def find(
        self,
        stream_id: int,
        *,
        category: int | None = None,
        type_ref: int | None = None,
        with_fields: Iterable[int] = (),
    ) -> Iterator[Table]:
        """Tables of a stream, in stream order, matching every given filter."""
        required = tuple(with_fields)
        for table in self.tables_of(stream_id):
            if category is not None and table.category != category:
                continue
            if type_ref is not None and table.type_ref != type_ref:
                continue
            if required and not table.has_fields(*required):
                continue
            yield table

    def first(
        self,
        stream_id: int,
        *,
        category: int | None = None,
        type_ref: int | None = None,
        with_fields: Iterable[int] = (),
    ) -> Table | None:
        return next(
            self.find(
                stream_id,
                category=category,
                type_ref=type_ref,
                with_fields=with_fields,
            ),
            None,
        )

    def has_defect(self, stream_id: int) -> bool:
        """True when the stream tokenized with malformed/truncated spans."""
        return any(
            span.kind in ("malformed", "truncated")
            for span in self.spans.get(stream_id, ())
        )

    def defects(self, stream_id: int) -> list[UnknownSpan]:
        return [
            span
            for span in self.spans.get(stream_id, ())
            if span.kind in ("malformed", "truncated")
        ]

    def unknown_fields(self) -> dict[int, list[tuple[int, int, int]]]:
        """Fields not named in :mod:`.maps`, per stream.

        Returns ``{stream_id: [(category, field_id, dtype), ...]}`` sorted and
        deduplicated — the systematic enumeration of unmapped format knowledge
        (pinned by the census goldens as a format-drift tripwire).
        """
        result: dict[int, list[tuple[int, int, int]]] = {}
        for stream_id, tables in self.tables.items():
            seen = {
                (table.category, entry.field_id, int(entry.dtype))
                for table in tables
                for entry in table.fields.values()
                if entry.field_id not in KNOWN_FIELD_IDS
            }
            result[stream_id] = sorted(seen)
        return result


@dataclass
class _OpenTable:
    """Mutable accumulator for the table currently being assembled."""

    category: int
    type_ref: int
    class_name: str | None
    start: int
    fields: dict[int, Field] = dataclass_field(default_factory=dict)
    preamble: bool = False
    end: int = 0


def _field_of(token: FieldToken) -> Field:
    start, end, field_id, dtype, mode, raw, element_count = token
    value = decode_scalar(dtype, raw) if mode == Mode.SCALAR else None
    return Field(field_id, dtype, mode, value, element_count, raw, (start, end))


def assemble_stream(
    stream: StreamData, limits: ParsingConfig
) -> tuple[tuple[Table, ...], tuple[UnknownSpan, ...], tuple[Field, ...]]:
    """Assemble one stream's sections into tables.

    Walks every section in directory order (sections are contiguous, so this
    is also stream order). Returns (tables, unknown spans, orphan fields);
    orphans — field records before any table open — never occur in pristine
    files (every section opens with a prologue, then a class-definition table
    open) and are invisible to extraction.

    Raises:
        NGBResourceLimitError: More than ``limits.max_tables_per_stream``
            tables, or an array record exceeding ``max_array_size_mb``
            (propagated from the tokenizer).
    """
    tables: list[Table] = []
    spans: list[UnknownSpan] = []
    orphans: list[Field] = []
    current: _OpenTable | None = None
    max_tables = limits.max_tables_per_stream

    def close(current: _OpenTable) -> None:
        tables.append(
            Table(
                stream_id=stream.stream_id,
                index=len(tables),
                category=current.category,
                type_ref=current.type_ref,
                class_name=current.class_name,
                fields=current.fields,
                preamble=current.preamble,
                span=(current.start, current.end),
            )
        )
        if len(tables) > max_tables:
            raise NGBResourceLimitError(
                f"stream_{stream.stream_id} contains more than {max_tables} "
                "tables, exceeding max_tables_per_stream",
                stream=stream.stream_id,
                declared=len(tables),
                limit=max_tables,
            )

    # Hot loop: one iteration per record of the stream. Locals for the
    # per-record calls, tuple unpacking instead of attribute access, and
    # positional Field construction keep document assembly within the same
    # time budget as the tokenizer walk itself.
    decode = decode_scalar
    scalar = Mode.SCALAR
    ref = DType.REF
    for entry in stream.sections:
        for item in tokenize(
            stream.raw, start=entry.offset, end=entry.end, limits=limits
        ):
            if isinstance(item, UnknownSpan):
                spans.append(item)
                if (
                    item.kind == "preamble"
                    and current is not None
                    and not current.fields
                ):
                    current.preamble = True
                continue

            start, end, field_id, dtype, mode, raw, element_count = item

            if dtype is ref and mode is scalar:
                type_ref = ref_type_ref(raw)
                if type_ref is not None:
                    if current is not None:
                        close(current)
                    current = _OpenTable(
                        category=field_id,
                        type_ref=type_ref,
                        class_name=ref_class_name(raw),
                        start=start,
                        end=end,
                    )
                    continue

            if current is None:
                logger.warning(
                    f"stream_{stream.stream_id}: field 0x{field_id:04X} at "
                    f"offset {start} precedes any table open; ignoring"
                )
                orphans.append(_field_of(item))
                continue
            if field_id in current.fields:
                logger.warning(
                    f"stream_{stream.stream_id} table {len(tables)}: duplicate "
                    f"field 0x{field_id:04X} at offset {start}; "
                    "keeping the first occurrence"
                )
            else:
                value = decode(dtype, raw) if mode is scalar else None
                current.fields[field_id] = Field(
                    field_id, dtype, mode, value, element_count, raw, (start, end)
                )
            current.end = end
    if current is not None:
        close(current)

    return tuple(tables), tuple(spans), tuple(orphans)


def load_document(
    path: str | Path,
    *,
    streams: Iterable[int] | None = None,
    limits: ParsingConfig | None = None,
) -> NGBDocument:
    """Parse an NGB file into its full document model.

    Args:
        path: Path to the ``.ngb-*`` file.
        streams: Stream numbers to load; None loads every stream present.
        limits: Resource limits (stream size, array size, table count).

    Raises:
        FileNotFoundError, zipfile.BadZipFile, NGBStreamNotFoundError,
        NGBCorruptedFileError: As raised by :func:`pyngb.format.open_ngb`
            for container-level problems.
        NGBResourceLimitError: A declared size exceeds the configured limits.
    """
    limits = limits or ParsingConfig()
    loaded = open_ngb(path, streams=streams, limits=limits)
    tables: dict[int, tuple[Table, ...]] = {}
    spans: dict[int, tuple[UnknownSpan, ...]] = {}
    orphans: dict[int, tuple[Field, ...]] = {}
    for stream_id, stream in loaded.items():
        tables[stream_id], spans[stream_id], orphans[stream_id] = assemble_stream(
            stream, limits
        )
    return NGBDocument(streams=loaded, tables=tables, spans=spans, orphans=orphans)
