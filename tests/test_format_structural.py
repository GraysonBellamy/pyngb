"""Structural invariants of the format layer over every real fixture.

These are the format-drift tripwires: every byte of every section of every
stream must be either a decoded record or a classified span — an
unclassified (malformed/truncated) span in a pristine file means the
grammar model is wrong or the format moved. Span shapes are pinned to the
observed forms so a drift shows up as a loud, specific diff.
"""

import json
from collections import Counter
from pathlib import Path

import pytest

from pyngb.format import (
    DType,
    FieldToken,
    UnknownSpan,
    load_document,
    open_ngb,
    ref_type_ref,
    tokenize,
)
from pyngb.format.census import document_census
from support.ngb_builder import assert_accounting

FIXTURE_DIR = Path(__file__).parent / "test_files"
GOLDEN_DIR = Path(__file__).parent / "goldens"
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*")) if FIXTURE_DIR.exists() else []

# Everything a pristine file may contain besides records.
CLEAN_KINDS = {"prologue", "preamble", "table_trailer", "bare_record"}

# Channel-header and segment-value table type refs (streams 2 and 3).
CHANNEL_HEADER_TYPE = 0x2B22
SEGMENT_VALUES_TYPE = 0x2B23
# Each segment-value table carries exactly one data array: field 0x0F40
# (f64) for f64 channels, field 0x0F3D (f32) for f32 channels. Empirical
# correction to the rewrite brief, which called 0x0F3D "unmapped aux": the
# legacy stream processor decodes either without looking at field ids.
DATA_FIELDS = {(0x0F40, DType.F64), (0x0F3D, DType.F32)}


def tokenize_section(stream, entry) -> list:
    items = list(tokenize(stream.raw, start=entry.offset, end=entry.end))
    assert_accounting(items, entry.offset, entry.end)
    return items


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_every_byte_of_every_section_is_classified(fixture: Path) -> None:
    """Total coverage, zero unclassified spans, across all streams."""
    for stream in open_ngb(fixture).values():
        for entry in stream.sections:
            items = tokenize_section(stream, entry)
            bad = [
                item
                for item in items
                if isinstance(item, UnknownSpan) and item.kind not in CLEAN_KINDS
            ]
            assert not bad, (
                f"stream_{stream.stream_id} section {entry.section_id}: "
                f"unclassified spans {bad[:5]}"
            )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_span_shapes_match_the_observed_forms(fixture: Path) -> None:
    """Pin the byte lengths of every non-record form (drift tripwire)."""
    lengths: dict[str, set[int]] = {}
    for stream in open_ngb(fixture).values():
        for entry in stream.sections:
            for item in tokenize_section(stream, entry):
                if isinstance(item, UnknownSpan):
                    lengths.setdefault(item.kind, set()).add(item.end - item.start)
    assert lengths["prologue"] == {64}
    assert lengths["preamble"] == {47}
    assert lengths["table_trailer"] == {3}
    assert lengths.get("bare_record", {28}) == {28}


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_every_section_starts_with_a_prologue(fixture: Path) -> None:
    for stream in open_ngb(fixture).values():
        for entry in stream.sections:
            first = next(iter(tokenize(stream.raw, start=entry.offset, end=entry.end)))
            assert isinstance(first, UnknownSpan)
            assert first.kind == "prologue"


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_data_streams_use_the_pinned_type_refs(fixture: Path) -> None:
    """Channel headers open with type_ref 0x2B22 and segment-value tables
    with 0x2B23 in BOTH data streams of BOTH vintages (verify-item 5 of the
    rewrite plan)."""
    streams = open_ngb(fixture, streams=[2, 3])
    for stream in streams.values():
        refs = Counter()
        for item in tokenize_section(stream, stream.main):
            if isinstance(item, FieldToken) and item.dtype == DType.REF:
                type_ref = ref_type_ref(item.raw)
                if type_ref is not None:
                    refs[type_ref] += 1
        assert refs[CHANNEL_HEADER_TYPE] > 0
        assert refs[SEGMENT_VALUES_TYPE] > 0


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_channel_data_element_counts_match_the_parity_goldens(
    fixture: Path,
) -> None:
    """Token-level cross-check against the C0 goldens: within stream 2,
    every channel's data-array elements sum to the pinned row count."""
    golden_path = GOLDEN_DIR / f"{fixture.name}.parity.json"
    assert golden_path.exists(), f"missing parity golden for {fixture.name}"
    num_rows = json.loads(golden_path.read_text(encoding="utf-8"))["num_rows"]

    stream = open_ngb(fixture, streams=[2])[2]
    per_channel: list[int] = []
    current: int | None = None
    for item in tokenize_section(stream, stream.main):
        if not isinstance(item, FieldToken):
            continue
        if item.dtype == DType.REF:
            type_ref = ref_type_ref(item.raw)
            if type_ref == CHANNEL_HEADER_TYPE:
                per_channel.append(0)
                current = len(per_channel) - 1
            elif type_ref is not None and type_ref != SEGMENT_VALUES_TYPE:
                current = None
        elif (item.field_id, item.dtype) in DATA_FIELDS and current is not None:
            per_channel[current] += item.element_count or 0
    assert per_channel, "no channel header tables found in stream 2"
    assert all(total == num_rows for total in per_channel), (
        f"per-channel element counts {per_channel} != golden num_rows {num_rows}"
    )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_census_matches_the_golden(fixture: Path) -> None:
    """The full structural census — table counts, dtype counts, span kinds,
    type-ref sets, and the unknown-field enumeration — is pinned per fixture.
    A diff here means the format model moved (or Proteus grew a new field:
    the unknown-field diff is then the Phase-2 to-do list)."""
    golden_path = GOLDEN_DIR / f"{fixture.name}.census.json"
    assert golden_path.exists(), f"missing census golden for {fixture.name}"
    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    assert golden["fixture"] == fixture.name

    census = document_census(load_document(fixture))
    assert census["streams"] == golden["streams"]
    assert census["unknown_fields"] == golden["unknown_fields"]
