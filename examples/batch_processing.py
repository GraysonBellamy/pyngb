#!/usr/bin/env python3
"""
Example: Batch Processing Multiple NGB Files

This example demonstrates how to efficiently process multiple NGB files
using pyngb's batch processing capabilities.

Requirements:
    - pyngb
    - polars (automatically installed with pyngb)

Usage:
    python batch_processing.py --input-dir ./data/ --output-dir ./results/
    python batch_processing.py --files file1.ngb-ss3 file2.ngb-ss3
"""

import argparse
import sys
import tempfile
from collections.abc import Sequence
from typing import Union
from pathlib import Path

import polars as pl

from pyngb import BatchProcessor, NGBDataset, process_directory


def demonstrate_batch_processor(files: Sequence[Union[str, Path]], output_dir: str):
    """Demonstrate BatchProcessor class usage."""

    print("\n🔧 Method 1: BatchProcessor Class")
    print("-" * 50)

    # Initialize batch processor
    processor = BatchProcessor(
        max_workers=2,  # Use 2 cores for processing
        verbose=True,  # Show progress information
    )

    # Process files
    results = processor.process_files(
        list(files),
        output_format="both",  # Create both Parquet and CSV files
        output_dir=output_dir,
        skip_errors=True,  # Continue processing if individual files fail
    )

    # Analyze results
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "error"]

    print("\n📊 Processing Results:")
    print(f"  ✅ Successful: {len(successful)}")
    print(f"  ❌ Failed: {len(failed)}")

    if successful:
        total_rows = int(sum(float(r.get("rows") or 0) for r in successful))
        avg_time = (
            sum(float(r.get("processing_time") or 0.0) for r in successful)
            / len(successful)
            if successful
            else 0.0
        )

        print(f"  📈 Total data points: {total_rows:,}")
        print(f"  ⏱️  Average processing time: {avg_time:.2f} seconds")

        print("\n📁 Output Files Created:")
        for result in successful:
            file_val = result.get("file")
            if not isinstance(file_val, str):
                continue
            input_path = Path(file_val)
            base_name = input_path.stem

            parquet_file = Path(output_dir) / f"{base_name}.parquet"
            csv_file = Path(output_dir) / f"{base_name}.csv"
            metadata_file = Path(output_dir) / f"{base_name}_metadata.json"

            if parquet_file.exists():
                print(f"  ✅ {parquet_file}")
            if csv_file.exists():
                print(f"  ✅ {csv_file}")
            if metadata_file.exists():
                print(f"  ✅ {metadata_file}")

    if failed:
        print("\n❌ Failed Files:")
        for result in failed:
            print(f"  {result['file']}: {result.get('error', 'Unknown error')}")

    return results


def demonstrate_convenience_function(input_dir: str, _output_dir: str):
    """Demonstrate process_directory convenience function."""

    print("\n⚡ Method 2: Convenience Function")
    print("-" * 50)

    # Process entire directory
    results = process_directory(
        directory=input_dir, pattern="*.ngb-ss3", output_format="parquet", max_workers=2
    )

    print(f"📊 Processed {len(results)} files from directory")

    successful = [r for r in results if r["status"] == "success"]
    if successful:
        print(f"  ✅ {len(successful)} files processed successfully")

        # Show sample information
        for result in successful[:3]:  # Show first 3
            file_val = result.get("file")
            name = Path(file_val).name if isinstance(file_val, str) else "<unknown>"
            print(f"    {name}: {result.get('rows', 0)} rows")

        if len(successful) > 3:
            print(f"    ... and {len(successful) - 3} more files")

    return results


def demonstrate_dataset_management(files: Sequence[Union[str, Path]], out_dir: Path):
    """Demonstrate NGBDataset for dataset management."""

    print("\n📚 Method 3: Dataset Management")
    print("-" * 50)

    # Create dataset
    dataset = NGBDataset([Path(f) for f in files])

    print("📊 Dataset Overview:")
    print(f"  Files: {len(dataset)}")

    # Get summary statistics
    try:
        summary = dataset.summary()

        print(f"  Loadable files: {summary.get('loadable_files', 0)}")

        if "unique_instruments" in summary:
            instruments = summary["unique_instruments"]
            print(f"  Instruments: {instruments}")

        if "unique_operators" in summary:
            operators = summary["unique_operators"]
            print(f"  Operators: {operators}")

        rng = summary.get("sample_mass_range")
        if (
            isinstance(rng, tuple)
            and len(rng) == 2
            and all(isinstance(x, (int, float)) for x in rng)
        ):
            print(f"  Sample mass range: {rng[0]:.1f} to {rng[1]:.1f} mg")

        if summary.get("avg_sample_mass"):
            avg_mass = summary["avg_sample_mass"]
            print(f"  Average sample mass: {avg_mass:.1f} mg")

    except Exception as e:
        print(f"  ⚠️  Could not generate summary: {e}")

    # Export metadata
    try:
        metadata_file = out_dir / "dataset_metadata.csv"
        dataset.export_metadata(str(metadata_file), format="csv")

        if metadata_file.exists():
            print(f"  📁 Metadata exported to: {metadata_file}")

            # Show metadata preview
            metadata_df = pl.read_csv(metadata_file)
            print("  📋 Metadata preview:")
            print(f"    Columns: {metadata_df.columns}")
            print(f"    Rows: {metadata_df.height}")

    except Exception as e:
        print(f"  ⚠️  Could not export metadata: {e}")


def create_processing_summary(results: list[dict], output_dir: str):
    """Create a summary report of batch processing results."""

    print("\n📋 Creating Processing Summary")
    print("-" * 50)

    # Compile summary data
    summary_data = []

    for result in results:
        file_path = Path(result["file"])

        summary_row = {
            "file_name": file_path.name,
            "file_path": str(file_path),
            "status": result["status"],
            "rows": result.get("rows", 0),
            "columns": result.get("columns", 0),
            "sample_name": result.get("sample_name", "Unknown"),
            "processing_time": result.get("processing_time", 0),
            "error": result.get("error", ""),
        }

        summary_data.append(summary_row)

    # Create summary DataFrame
    summary_df = pl.DataFrame(summary_data)

    # Save summary
    summary_file = Path(output_dir) / "processing_summary.csv"
    summary_df.write_csv(summary_file)

    print(f"📁 Summary saved to: {summary_file}")

    # Display statistics
    total_files = len(results)
    successful_files = len([r for r in results if r["status"] == "success"])
    total_rows = sum(r.get("rows", 0) for r in results if r["status"] == "success")
    total_time = sum(r.get("processing_time", 0) for r in results)

    print("\n📊 Final Statistics:")
    print(f"  Total files processed: {total_files}")
    print(
        f"  Successful: {successful_files} ({successful_files / total_files * 100:.1f}%)"
    )
    print(f"  Total data points: {total_rows:,}")
    print(f"  Total processing time: {total_time:.2f} seconds")

    if successful_files > 0:
        avg_time = total_time / successful_files
        print(f"  Average time per file: {avg_time:.2f} seconds")


def main():
    """Main function to run the batch processing example."""

    parser = argparse.ArgumentParser(
        description="Demonstrate pyngb batch processing capabilities"
    )
    parser.add_argument("--input-dir", type=str, help="Directory containing NGB files")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for processed files (defaults to a temporary directory)",
    )
    parser.add_argument("--files", nargs="+", help="Specific files to process")

    args = parser.parse_args()

    print("🚀 pyngb Batch Processing Example")
    print("=" * 60)
    print("This example demonstrates efficient processing of multiple NGB files.")
    print()

    # Determine files to process
    files_to_process = []

    if args.files:
        files_to_process = args.files
        print(f"📁 Processing {len(files_to_process)} specified files")

    elif args.input_dir:
        input_path = Path(args.input_dir)
        if not input_path.exists():
            print(f"❌ Input directory not found: {args.input_dir}")
            return 1

        files_to_process = list(input_path.glob("*.ngb-ss3"))
        print(f"📁 Found {len(files_to_process)} NGB files in {args.input_dir}")

    else:
        # Look for test files
        possible_dirs = ["../tests/test_files/", "tests/test_files/", "./test_files/"]

        for test_dir in possible_dirs:
            test_path = Path(test_dir)
            if test_path.exists():
                test_files = list(test_path.glob("*.ngb-ss3"))
                if test_files:
                    files_to_process = [str(f) for f in test_files]
                    print(f"📁 Using test files from {test_dir}")
                    break

        if not files_to_process:
            print("❌ No NGB files found!")
            print()
            print("Usage:")
            print("  python batch_processing.py --input-dir ./data/")
            print("  python batch_processing.py --files file1.ngb-ss3 file2.ngb-ss3")
            return 1

    # Check that files exist
    valid_files = []
    for file_path in files_to_process:
        if Path(file_path).exists():
            valid_files.append(str(file_path))
        else:
            print(f"⚠️  File not found: {file_path}")

    if not valid_files:
        print("❌ No valid files to process!")
        return 1

    files_to_process = valid_files
    print(f"✅ Will process {len(files_to_process)} files")

    # Create output directory (temporary if not provided)
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Output directory: {output_dir}")
        # Run example normally, leave artifacts when user chose path
        cleanup = False
    else:
        tmp = tempfile.TemporaryDirectory(prefix="pyngb_batch_out_")
        output_dir = Path(tmp.name)
        print(f"📁 Output directory (temporary): {output_dir}")
        cleanup = True

    # Demonstrate different batch processing methods
    all_results = []

    # Method 1: BatchProcessor class
    results1 = demonstrate_batch_processor(files_to_process, str(output_dir))
    all_results.extend(results1)

    # Method 2: Convenience function (if input directory provided)
    if args.input_dir:
        _ = demonstrate_convenience_function(args.input_dir, str(output_dir))

    # Method 3: Dataset management
    demonstrate_dataset_management(files_to_process, output_dir)

    # Create processing summary
    create_processing_summary(all_results, str(output_dir))

    print("\n✅ Batch processing demonstration completed!")
    print("\n💡 What you learned:")
    print("  - How to use BatchProcessor for parallel processing")
    print("  - How to handle processing errors gracefully")
    print("  - How to manage datasets with NGBDataset")
    print("  - How to export data in multiple formats")
    print("  - How to create processing summaries")

    print("\n📁 Check the output directory for processed files:")
    print(f"  {output_dir.absolute()}")

    # Cleanup temporary directory if we created one
    if cleanup:
        print("\n🧹 Cleaning up temporary output directory...")
        # TemporaryDirectory will clean up automatically when going out of scope

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
