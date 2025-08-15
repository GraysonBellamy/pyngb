#!/usr/bin/env python3
"""
Example: Process all test files (.ngb-ss3 and .ngb-bs3) and clean up outputs.

This example uses BatchProcessor to generate CSV/Parquet/metadata for all
files in tests/test_files, writing to a temporary directory that is removed
on exit.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from collections.abc import Sequence

import polars as pl

from pyngb.batch import BatchProcessor


ess_type = Sequence[str]


def discover_test_files(base: Path) -> list[str]:
    files = [
        *(str(p) for p in base.glob("*.ngb-ss3")),
        *(str(p) for p in base.glob("*.ngb-bs3")),
    ]
    return sorted(files)


def run(files: ess_type) -> None:
    with tempfile.TemporaryDirectory(prefix="pyngb_example_all_") as tmp:
        out_dir = Path(tmp)
        print(f"Output directory (temporary): {out_dir}")
        bp = BatchProcessor(max_workers=4, verbose=True)
        results = bp.process_files(
            list(files), output_format="both", output_dir=str(out_dir), skip_errors=True
        )
        print(f"Processed {len(results)} files")
        summary = pl.DataFrame(results)
        summary_path = out_dir / "processing_summary.csv"
        summary.write_csv(summary_path)
        print(f"Summary saved to: {summary_path}")
        # All outputs are cleaned when the temporary directory is deleted


def main() -> int:
    base = Path("tests/test_files")
    files = discover_test_files(base)
    print(f"Discovered {len(files)} files:")
    for f in files:
        print(f"  - {Path(f).name}")
    if not files:
        print("No files found.")
        return 0
    run(files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
