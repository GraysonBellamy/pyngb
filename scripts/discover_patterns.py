"""Utility helpers to discover new metadata (category, field) byte patterns.

Run this script to infer the (category, field) two-byte identifiers for new
metadata values you know exist inside ``Streams/stream_1.table`` of an NGB file.

The existing extraction logic (see ``PatternConfig.metadata_patterns`` and
``MetadataExtractor``) relies on patterns of the form::

    <category> ... <field> ... TYPE_PREFIX <data_type> TYPE_SEPARATOR <value> END_FIELD

Where:
    * category: 2 bytes (e.g. b"\x75\x17")
    * field:    2 bytes (e.g. b"\x59\x10")
    * TYPE_PREFIX: fixed (``BinaryMarkers.TYPE_PREFIX``)
    * data_type: 1 byte (e.g. 0x1f STRING, 0x05 FLOAT64)
    * TYPE_SEPARATOR: fixed (``BinaryMarkers.TYPE_SEPARATOR``)
    * value: for strings => 4-byte length prefix then UTF-16LE text; for numbers => raw bytes
    * END_FIELD: fixed (``BinaryMarkers.END_FIELD``)

Given one or more known plaintext values, this script searches for their UTF-16LE
encoded form inside ``stream_1.table`` and then walks backwards to brute-force
candidate (category, field) byte pairs that produce a valid pattern match.

Usage (CLI):

    python -m scripts.discover_patterns path/to/file.ngb-ss3 "Known String" "Another Value"

You can also import ``discover_patterns`` and call ``infer_patterns`` directly.
"""

from __future__ import annotations

import argparse
import itertools
import re
import struct
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from pyngb.constants import BinaryMarkers


@dataclass
class DiscoveredPattern:
    """Result container for an inferred metadata pattern (string or numeric)."""

    value: str
    occurrences: int
    category: bytes | None
    field: bytes | None
    data_type: bytes | None
    confidence: float  # heuristic 0..1
    notes: str = ""
    kind: str = "string"  # 'string' or 'float64'

    def pattern_snippet(self) -> str:
        if self.category and self.field:
            return f"(category={self.category.hex()} field={self.field.hex()} dtype={self.data_type.hex() if self.data_type else '??'} kind={self.kind})"
        return f"(pattern incomplete kind={self.kind})"


def _utf16le(s: str) -> bytes:
    # Values are stored with 4-byte length prefix + UTF-16LE characters.
    encoded = s.encode("utf-16le", errors="ignore")
    length_prefix = struct.pack("<I", len(encoded))
    return length_prefix + encoded


def _load_stream1(path: str) -> bytes:
    with zipfile.ZipFile(path, "r") as z:
        return z.read("Streams/stream_1.table")


def _infer_for_value(
    stream: bytes, value: str, markers: BinaryMarkers
) -> DiscoveredPattern:
    target = _utf16le(value)
    hits = [m.start() for m in re.finditer(re.escape(target), stream)]
    if not hits:
        return DiscoveredPattern(
            value, 0, None, None, None, 0.0, "Value bytes not found"
        )

    END_FIELD = markers.END_FIELD
    TYPE_PREFIX = markers.TYPE_PREFIX
    TYPE_SEPARATOR = markers.TYPE_SEPARATOR

    # Heuristic search window before TYPE_PREFIX
    MAX_BACK = 160  # bytes to look back for category/field candidates
    results: list[tuple[bytes, bytes, bytes, float]] = []

    for hit in hits:
        # hit points to start of length prefix; value bytes start at hit+4
        # search backwards for the nearest TYPE_PREFIX prior to length prefix
        prefix_idx = stream.rfind(TYPE_PREFIX, max(0, hit - 300), hit)
        if prefix_idx == -1:
            continue
        # Data type is the single byte immediately after TYPE_PREFIX
        data_type = stream[
            prefix_idx + len(TYPE_PREFIX) : prefix_idx + len(TYPE_PREFIX) + 1
        ]
        # Validate the sequence has TYPE_SEPARATOR between data_type and target
        sep_expected_idx = prefix_idx + len(TYPE_PREFIX) + 1
        if (
            stream[sep_expected_idx : sep_expected_idx + len(TYPE_SEPARATOR)]
            != TYPE_SEPARATOR
        ):
            # Not a canonical field sequence
            continue
        # Basic structural validation: confirm END_FIELD after value
        end_search_start = hit + len(target)
        end_idx = stream.find(END_FIELD, end_search_start, end_search_start + 200)
        if end_idx == -1:
            continue
        # Brute force category/field pair within window before TYPE_PREFIX.
        window_start = max(0, prefix_idx - MAX_BACK)
        window = stream[window_start:prefix_idx]
        best_score = 0.0
        best: tuple[bytes, bytes] | None = None
        # We try each pair of offsets (i<j) for category and field bytes.
        for i, j in itertools.combinations(range(len(window) - 1), 2):
            cat = window[i : i + 2]
            field = window[j : j + 2]
            if len(cat) < 2 or len(field) < 2:
                continue
            # Quick filter: avoid markers bytes fragments
            if cat in (TYPE_PREFIX[:2], TYPE_SEPARATOR, END_FIELD[:2]):
                continue
            # Construct minimal pattern
            pat = re.compile(
                re.escape(cat)
                + b".{0,80}?"
                + re.escape(field)
                + b".{0,80}?"
                + re.escape(TYPE_PREFIX)
                + re.escape(data_type)
                + re.escape(TYPE_SEPARATOR)
                + re.escape(target)
                + re.escape(END_FIELD),
                re.DOTALL,
            )
            search_region = stream[window_start + i : end_idx + len(END_FIELD)]
            m = pat.match(search_region)
            if m:
                gap = m.end() - m.start()
                score = 1.0 / (1 + gap / 40)  # shorter => higher
                if score > best_score:
                    best_score = score
                    best = (cat, field)
        if best:
            results.append((best[0], best[1], data_type, best_score))

    if not results:
        return DiscoveredPattern(
            value, len(hits), None, None, None, 0.0, "Pattern not confidently inferred"
        )

    # Pick highest score
    cat, field, dtype, score = max(results, key=lambda t: t[3])
    return DiscoveredPattern(value, len(hits), cat, field, dtype, min(score, 1.0))


def _infer_for_float(
    stream: bytes, value: float, markers: BinaryMarkers, tol: float = 1e-6
) -> DiscoveredPattern:
    """Infer pattern for a FLOAT64 value.

    Strategy: search for exact 8-byte little-endian sequence; if not found, scan
    all candidate field structures (TYPE_PREFIX .. TYPE_SEPARATOR .. 8 bytes .. END_FIELD)
    and compare decoded float within tolerance.
    """
    import struct

    target_bytes = struct.pack("<d", value)
    END_FIELD = markers.END_FIELD
    TYPE_PREFIX = markers.TYPE_PREFIX
    TYPE_SEPARATOR = markers.TYPE_SEPARATOR

    def search_matches(data: bytes) -> list[int]:
        positions = []
        start = 0
        while True:
            idx = data.find(target_bytes, start)
            if idx == -1:
                break
            positions.append(idx)
            start = idx + 1
        return positions

    hits = search_matches(stream)
    # Fallback approximate scan if no exact match found
    if not hits:
        import struct

        pattern = re.compile(
            re.escape(TYPE_PREFIX)
            + b"."
            + re.escape(TYPE_SEPARATOR)
            + b"(.{8})"
            + re.escape(END_FIELD),
            re.DOTALL,
        )
        for m in pattern.finditer(stream):
            raw = m.group(1)
            try:
                v = struct.unpack("<d", raw)[0]
            except struct.error:  # narrow exception (Bandit B112)
                continue
            if abs(v - value) <= tol:
                hits.append(m.start(1))
    if not hits:
        return DiscoveredPattern(
            str(value),
            0,
            None,
            None,
            None,
            0.0,
            "Float bytes not found (tight tolerance)",
            kind="float64",
        )

    MAX_BACK = 160
    results: list[tuple[bytes, bytes, bytes, float]] = []
    for hit in hits:
        # locate the TYPE_PREFIX preceding the value bytes (which start at hit)
        prefix_idx = stream.rfind(TYPE_PREFIX, max(0, hit - 300), hit)
        if prefix_idx == -1:
            continue
        data_type = stream[
            prefix_idx + len(TYPE_PREFIX) : prefix_idx + len(TYPE_PREFIX) + 1
        ]
        # Validate separator present
        sep_expected_idx = prefix_idx + len(TYPE_PREFIX) + 1
        if (
            stream[sep_expected_idx : sep_expected_idx + len(TYPE_SEPARATOR)]
            != TYPE_SEPARATOR
        ):
            continue
        # END_FIELD after the 8 bytes
        end_idx = stream.find(END_FIELD, hit + 8, hit + 8 + 200)
        if end_idx == -1:
            continue
        window_start = max(0, prefix_idx - MAX_BACK)
        window = stream[window_start:prefix_idx]
        best_score = 0.0
        best: tuple[bytes, bytes] | None = None
        for i, j in itertools.combinations(range(len(window) - 1), 2):
            cat = window[i : i + 2]
            field = window[j : j + 2]
            if len(cat) < 2 or len(field) < 2:
                continue
            if cat in (TYPE_PREFIX[:2], TYPE_SEPARATOR, END_FIELD[:2]):
                continue
            pat = re.compile(
                re.escape(cat)
                + b".{0,80}?"
                + re.escape(field)
                + b".{0,80}?"
                + re.escape(TYPE_PREFIX)
                + re.escape(data_type)
                + re.escape(TYPE_SEPARATOR)
                + re.escape(stream[hit : hit + 8])
                + re.escape(END_FIELD),
                re.DOTALL,
            )
            search_region = stream[window_start + i : end_idx + len(END_FIELD)]
            m = pat.match(search_region)
            if m:
                gap = m.end() - m.start()
                score = 1.0 / (1 + gap / 40)
                if score > best_score:
                    best_score = score
                    best = (cat, field)
        if best:
            results.append((best[0], best[1], data_type, best_score))

    if not results:
        return DiscoveredPattern(
            str(value),
            len(hits),
            None,
            None,
            None,
            0.0,
            "Pattern not confidently inferred",
            kind="float64",
        )
    cat, field, dtype, score = max(results, key=lambda t: t[3])
    return DiscoveredPattern(
        str(value), len(hits), cat, field, dtype, min(score, 1.0), kind="float64"
    )


def infer_patterns(path: str, values: Sequence[str]) -> list[DiscoveredPattern]:
    """Infer (category, field) byte patterns for provided values.

    Tries string first; if value parses cleanly as float, also attempts float64 search.
    """
    stream = _load_stream1(path)
    markers = BinaryMarkers()
    results: list[DiscoveredPattern] = []
    for v in values:
        # Attempt float detection if looks numeric
        stripped = v.strip()
        is_num = False
        try:
            # Accept colon-separated name:value form; split and use the value side
            if ":" in stripped and not stripped.startswith("http"):
                # keep original label only for printing; we parse the right side
                label, val_part = stripped.split(":", 1)
                val_part = val_part.strip()
                fval = float(val_part)
                res = _infer_for_float(stream, fval, markers)
                # attach original label to notes if pattern found
                if res.category:
                    res.notes = f"label={label.strip()}"
                results.append(res)
                is_num = True
            else:
                fval = float(stripped)
                res = _infer_for_float(stream, fval, markers)
                results.append(res)
                is_num = True
        except ValueError:
            pass
        if not is_num:
            results.append(_infer_for_value(stream, stripped, markers))
    return results


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Infer metadata byte patterns")
    parser.add_argument("file", help="Path to .ngb-ss3 file")
    parser.add_argument(
        "values",
        nargs="+",
        help="Known metadata values (string or numeric or name:value)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    results = infer_patterns(args.file, args.values)
    for r in results:
        print(f"Value: {r.value}")
        print(f"  Occurrences: {r.occurrences}")
        print(f"  Category: {r.category.hex() if r.category else None}")
        print(f"  Field:    {r.field.hex() if r.field else None}")
        print(f"  DataType: {r.data_type.hex() if r.data_type else None}")
        print(f"  Confidence: {r.confidence:.2f}")
        if r.category and r.field:
            cat_hex = r.category.hex()
            field_hex = r.field.hex()
            print(
                "  Suggested metadata_patterns entry:\n"
                + f"    config.metadata_patterns['new_field'] = (b'\\x{cat_hex[:2]}\\x{cat_hex[2:]}', b'\\x{field_hex[:2]}\\x{field_hex[2:]}')"
            )
        if r.notes:
            print(f"  Notes: {r.notes}")
        print()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
