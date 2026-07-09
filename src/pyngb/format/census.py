"""Structural census of a parsed document: the format-drift tripwire.

``document_census`` summarizes what the format layer saw in each stream —
table counts, per-dtype record counts, span-kind counts and gap bytes, the
type-ref set, and the unknown-field enumeration. The census goldens pin this
per fixture, so a Proteus format change (a new field, a new table type, a
new residual byte form) fails loudly and the diff is the mapping to-do list.

Everything is JSON-friendly (string keys, hex ids) so the same structure
serves the goldens and, later, the ``pyngb inspect`` CLI.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from .document import NGBDocument
from .grammar import DType

__all__ = ["document_census"]


def _stream_census(doc: NGBDocument, stream_id: int) -> dict[str, Any]:
    tables = doc.tables_of(stream_id)

    dtype_counts: Counter[int] = Counter()
    for table in tables:
        dtype_counts[DType.REF] += 1  # the table-open record
        for entry in table.fields.values():
            dtype_counts[entry.dtype] += 1
    for orphan in doc.orphans.get(stream_id, ()):
        dtype_counts[orphan.dtype] += 1

    span_counts: Counter[str] = Counter()
    gap_bytes = 0
    for span in doc.spans.get(stream_id, ()):
        span_counts[span.kind] += 1
        gap_bytes += span.end - span.start

    return {
        "tables": len(tables),
        "records_by_dtype": {
            f"0x{dtype:02x}": count for dtype, count in sorted(dtype_counts.items())
        },
        "spans_by_kind": dict(sorted(span_counts.items())),
        "gap_bytes": gap_bytes,
        "type_refs": sorted({f"0x{table.type_ref:04x}" for table in tables}),
        "orphan_fields": len(doc.orphans.get(stream_id, ())),
    }


def document_census(doc: NGBDocument) -> dict[str, Any]:
    """The per-stream structural census plus the unknown-field enumeration."""
    unknown = doc.unknown_fields()
    return {
        "streams": {
            str(stream_id): _stream_census(doc, stream_id)
            for stream_id in sorted(doc.streams)
        },
        "unknown_fields": {
            str(stream_id): [
                f"0x{category:04x}/0x{field_id:04x}/0x{dtype:02x}"
                for category, field_id, dtype in triples
            ]
            for stream_id, triples in sorted(unknown.items())
        },
    }
