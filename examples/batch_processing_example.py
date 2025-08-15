#!/usr/bin/env python3
"""
Example: Batch Processing and Data Validation

This example demonstrates batch processing and validation using the
current pyngb API.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from collections.abc import Sequence

import polars as pl

from pyngb import BatchProcessor, NGBDataset, read_ngb
from pyngb.validation import QualityChecker, validate_sta_data


def demo_validation(file_path: str) -> None:
    table = read_ngb(file_path)
    issues = validate_sta_data(table)
    if issues:
        print("Validation issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✅ Data validation passed!")

    checker = QualityChecker(table)
    result = checker.full_validation()
    print(result.report())


def demo_batch_processing(files: Sequence[str], output_dir: str) -> list[dict]:
    processor = BatchProcessor(max_workers=4, verbose=True)
    results = processor.process_files(
        list(files), output_format="both", output_dir=output_dir
    )
    return results


def demo_dataset(files: Sequence[str], output_dir: Path) -> None:
    dataset = NGBDataset([Path(f) for f in files])
    summary = dataset.summary()
    print("Dataset summary:")
    print(f"  Files: {summary.get('file_count', 0)}")
    rng = summary.get("sample_mass_range")
    if (
        isinstance(rng, tuple)
        and len(rng) == 2
        and all(isinstance(x, (int, float)) for x in rng)
    ):
        print(f"  Sample mass range: {rng[0]:.2f}-{rng[1]:.2f} mg")

    # Export metadata CSV into the example output directory
    meta_path = output_dir / "dataset_metadata.csv"
    dataset.export_metadata(str(meta_path), format="csv")
    if meta_path.exists():
        df = pl.read_csv(meta_path)
        print(f"Exported metadata columns: {df.columns}")


def main() -> int:
    # Find some files from tests if available
    candidates = [
        "../tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
        "tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
    ]
    files = [str(p) for p in candidates if Path(p).exists()]
    if not files:
        print("No example NGB files found; update paths in this script to run.")
        return 0

    print("=== Example 1: Validation ===")
    demo_validation(files[0])

    print("=== Example 2: Batch Processing ===")
    # Use a temporary output directory so the example cleans up after itself
    with tempfile.TemporaryDirectory(prefix="pyngb_example_out_") as tmp:
        out_dir = Path(tmp)
        results = demo_batch_processing(files, str(out_dir))
        print(f"Processed {len(results)} files")

        print("\n=== Example 3: Dataset Management ===")
        demo_dataset(files, out_dir)
        # Files are cleaned up when the TemporaryDirectory context exits
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
