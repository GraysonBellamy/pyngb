#!/usr/bin/env python3
"""
Process all .ngb-ss3 and .ngb-bs3 files in tests/test_files with BatchProcessor.

Outputs CSV, Parquet, metadata JSON per file, plus a processing_summary.csv.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Sequence

import polars as pl

from pyngb.batch import BatchProcessor


def discover_test_files(base: Path) -> list[str]:
    files = [
        *(str(p) for p in base.glob("*.ngb-ss3")),
        *(str(p) for p in base.glob("*.ngb-bs3")),
    ]
    return sorted(files)


def process_files(files: Sequence[str], output_dir: Path, workers: int = 4) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    bp = BatchProcessor(max_workers=workers, verbose=True)
    results = bp.process_files(
        list(files), output_format="both", output_dir=str(output_dir), skip_errors=True
    )
    summary = pl.DataFrame(results)
    summary_path = output_dir / "processing_summary.csv"
    summary.write_csv(summary_path)
    return summary_path


def main() -> int:
    base = Path("tests/test_files")
    if not base.exists():
        print(f"Test files directory not found: {base}")
        return 1

    out_dir = Path("tmp_test_files_all")
    files = discover_test_files(base)
    print(f"Discovered {len(files)} files:")
    for f in files:
        print(f"  - {Path(f).name}")

    if not files:
        print("No files to process.")
        return 0

    summary_path = process_files(files, out_dir, workers=4)
    print(f"Summary saved to: {summary_path}")
    print(f"Outputs written under: {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
