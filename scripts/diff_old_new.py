#!/usr/bin/env python3
"""Development differ: legacy regex backbone vs. the 0.4.0 format layer.

Usage:
    uv run python scripts/diff_old_new.py [--census] [FIXTURE ...]

For every fixture (default: all six), parses with BOTH stacks and diffs:

- metadata, key by key, recursively, with floats compared BITWISE
  (struct-packed, NaN-safe) — zero tolerances, ever;
- data columns: names, order, and per-column little-endian float64 bytes.

``--census`` additionally prints what the old code silently ignored: the
per-stream unknown-field enumeration from the document layer.

This tool exists only for the C2/C3 window while the two stacks coexist —
it is strictly stronger than the parity goldens during development because
it diffs every field, not just pinned ones. Deleted at C4.

Exits nonzero on any difference.
"""

from __future__ import annotations

import argparse
import struct
import sys
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from pyngb.core import NGBParser
from pyngb.format import build_dataframe, build_metadata, load_document

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "test_files"

FIXTURES = (
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3",
    "Douglas_Fir_STA_10K_250730_R13.ngb-ss3",
    "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3",
    "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3",
    "Red_Oak_STA_10K_250731_R7.ngb-ss3",
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3",
)


def values_equal(old: Any, new: Any) -> bool:
    """Recursive equality with bitwise float comparison (NaN-safe)."""
    if isinstance(old, float) or isinstance(new, float):
        if not isinstance(old, (int, float)) or not isinstance(new, (int, float)):
            return False
        return struct.pack("<d", float(old)) == struct.pack("<d", float(new))
    if isinstance(old, dict) and isinstance(new, dict):
        return old.keys() == new.keys() and all(
            values_equal(old[k], new[k]) for k in old
        )
    if isinstance(old, list) and isinstance(new, list):
        return len(old) == len(new) and all(
            values_equal(a, b) for a, b in zip(old, new)
        )
    return type(old) is type(new) and bool(old == new)


def diff_metadata(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    problems = []
    for key in sorted(old.keys() | new.keys()):
        if key not in new:
            problems.append(f"metadata[{key}]: missing in NEW (old={old[key]!r})")
        elif key not in old:
            problems.append(f"metadata[{key}]: extra in NEW (new={new[key]!r})")
        elif not values_equal(old[key], new[key]):
            problems.append(f"metadata[{key}]: old={old[key]!r} != new={new[key]!r}")
    return problems


def diff_data(old_df: pl.DataFrame, new_df: pl.DataFrame) -> list[str]:
    problems = []
    if old_df.columns != new_df.columns:
        problems.append(f"columns: old={old_df.columns} != new={new_df.columns}")
    for name in old_df.columns:
        if name not in new_df.columns:
            continue
        old_bytes = np.asarray(old_df[name].to_numpy(), dtype="<f8").tobytes()
        new_bytes = np.asarray(new_df[name].to_numpy(), dtype="<f8").tobytes()
        if old_bytes != new_bytes:
            problems.append(f"column[{name}]: byte-level mismatch")
    return problems


def diff_fixture(path: Path, census: bool) -> list[str]:
    old_metadata, old_table = NGBParser().parse(path)
    old_df = pl.from_arrow(old_table)
    assert isinstance(old_df, pl.DataFrame)

    doc = load_document(path)
    new_metadata = build_metadata(doc)
    new_df = build_dataframe(doc)

    problems = diff_metadata(dict(old_metadata), dict(new_metadata))
    problems += diff_data(old_df, new_df)

    if census:
        unknown = doc.unknown_fields()
        total = sum(len(v) for v in unknown.values())
        print(
            f"  census: {total} unknown (category, field, dtype) triples "
            f"the old code never saw:"
        )
        for stream_id in sorted(unknown):
            triples = unknown[stream_id]
            print(f"    stream {stream_id}: {len(triples)}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures", nargs="*", default=list(FIXTURES))
    parser.add_argument(
        "--census", action="store_true", help="print the unknown-field census"
    )
    args = parser.parse_args(argv)

    failed = False
    for fixture in args.fixtures:
        path = Path(fixture)
        if not path.exists():
            path = FIXTURE_DIR / fixture
        print(f"{path.name}:")
        problems = diff_fixture(path, args.census)
        if problems:
            failed = True
            for problem in problems:
                print(f"  DIFF {problem}")
        else:
            print("  OK - old and new stacks agree bitwise")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
