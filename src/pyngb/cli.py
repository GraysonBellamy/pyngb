#!/usr/bin/env python3
"""
Enhanced command-line interface for pyngb.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import polars as pl

from . import __version__, get_sta_data, load_ngb_data


def setup_logging(verbose: bool) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=level
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="pyngb - Parse NETZSCH STA NGB files",
        epilog="Examples:\n"
        "  pyngb sample.ngb-ss3\n"
        "  pyngb *.ngb-ss3 -o output_dir/\n"
        "  pyngb sample.ngb-ss3 -f csv --metadata-only\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("files", nargs="+", help="NGB files to parse")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )

    # Output options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument(
        "-o", "--output", help="Output directory (default: same as input)"
    )
    output_group.add_argument(
        "-f",
        "--format",
        choices=["parquet", "csv", "json", "all"],
        default="parquet",
        help="Output format (default: parquet)",
    )
    output_group.add_argument(
        "--metadata-only",
        action="store_true",
        help="Extract only metadata (no measurement data)",
    )

    # Processing options
    process_group = parser.add_argument_group("Processing Options")
    process_group.add_argument(
        "--validate", action="store_true", help="Validate files before processing"
    )
    process_group.add_argument(
        "--summary",
        action="store_true",
        help="Create summary report for batch processing",
    )

    # Logging options
    log_group = parser.add_argument_group("Logging Options")
    log_group.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    log_group.add_argument(
        "--quiet", action="store_true", help="Suppress non-error output"
    )

    return parser.parse_args()


def validate_file(file_path: Path) -> bool:
    """Validate NGB file before processing."""
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return False

    if not file_path.suffix.endswith(".ngb-ss3"):
        logging.warning(f"File may not be an NGB file: {file_path}")

    try:
        # Quick validation by attempting to load metadata
        metadata, _ = get_sta_data(str(file_path))
        if not metadata:
            logging.warning(f"No metadata found in: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Validation failed for {file_path}: {e}")
        return False


def process_file(file_path: Path, args: argparse.Namespace) -> Optional[dict]:
    """Process a single NGB file."""
    if not args.quiet:
        print(f"Processing {file_path.name}...")

    try:
        if args.metadata_only:
            metadata, data = get_sta_data(str(file_path))
            summary = {
                "filename": file_path.name,
                "metadata": metadata,
                "data_points": data.num_rows,
                "columns": data.column_names,
            }
        else:
            table = load_ngb_data(str(file_path))

            # Extract metadata from table
            metadata_bytes = table.schema.metadata.get(b"file_metadata", b"{}")
            metadata = json.loads(metadata_bytes)

            summary = {
                "filename": file_path.name,
                "metadata": metadata,
                "data_points": table.num_rows,
                "columns": table.column_names,
            }

            # Save data in requested format(s)
            output_dir = Path(args.output) if args.output else file_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            base_name = file_path.stem

            if args.format in ["parquet", "all"]:
                parquet_file = output_dir / f"{base_name}.parquet"
                table.to_pandas().to_parquet(parquet_file)
                if not args.quiet:
                    print(f"  → {parquet_file}")

            if args.format in ["csv", "all"]:
                csv_file = output_dir / f"{base_name}.csv"
                df: pl.DataFrame = pl.from_arrow(table)  # type: ignore[assignment]
                df.write_csv(csv_file)
                if not args.quiet:
                    print(f"  → {csv_file}")

            if args.format in ["json", "all"]:
                json_file = output_dir / f"{base_name}.json"
                df_for_json: pl.DataFrame = pl.from_arrow(table)  # type: ignore[assignment]
                with open(json_file, "w") as f:
                    json.dump(
                        {"metadata": metadata, "data": df_for_json.to_dicts()},
                        f,
                        indent=2,
                        default=str,
                    )
                if not args.quiet:
                    print(f"  → {json_file}")

        # Always save metadata
        if args.output:
            output_dir = Path(args.output)
            output_dir.mkdir(parents=True, exist_ok=True)
            metadata_file = output_dir / f"{file_path.stem}_metadata.json"
        else:
            metadata_file = file_path.parent / f"{file_path.stem}_metadata.json"

        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        if not args.quiet:
            print(f"  ✓ Processed {summary['data_points']} data points")

        return summary

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        if args.verbose:
            logging.exception("Full error details:")
        return None


def create_summary_report(summaries: list, output_dir: Path) -> None:
    """Create a summary report for batch processing."""
    if not summaries:
        return

    # Create DataFrame from summaries
    report_data = []
    for summary in summaries:
        if summary is None:
            continue

        metadata = summary.get("metadata", {})
        report_data.append(
            {
                "filename": summary["filename"],
                "sample_name": metadata.get("sample_name", "Unknown"),
                "instrument": metadata.get("instrument", "Unknown"),
                "operator": metadata.get("operator", "Unknown"),
                "data_points": summary["data_points"],
                "columns_count": len(summary["columns"]),
                "columns": ", ".join(summary["columns"]),
                "sample_mass": metadata.get("sample_mass", None),
                "crucible_mass": metadata.get("crucible_mass", None),
                "date_performed": metadata.get("date_performed", None),
                "project": metadata.get("project", None),
                "material": metadata.get("material", None),
            }
        )

    if report_data:
        df = pl.DataFrame(report_data)

        # Save as CSV
        summary_file = output_dir / "processing_summary.csv"
        df.write_csv(summary_file)

        # Also save detailed JSON
        detailed_file = output_dir / "processing_summary.json"
        with open(detailed_file, "w") as f:
            json.dump(summaries, f, indent=2, default=str)

        print("\n📊 Summary report saved:")
        print(f"  → {summary_file}")
        print(f"  → {detailed_file}")

        # Print basic statistics
        print("\n📈 Processing Statistics:")
        print(f"  Files processed: {len(report_data)}")
        print(f"  Total data points: {sum(r['data_points'] for r in report_data):,}")

        if any(r["sample_mass"] for r in report_data):
            masses = [r["sample_mass"] for r in report_data if r["sample_mass"]]
            print(f"  Average sample mass: {sum(masses) / len(masses):.2f} mg")


def main() -> int:
    """Main entry point."""
    args = parse_args()

    setup_logging(args.verbose)

    if args.quiet and args.verbose:
        print("Warning: --quiet and --verbose are mutually exclusive, using --verbose")
        args.quiet = False

    # Expand glob patterns
    files: list[Path] = []
    for pattern in args.files:
        pattern_path = Path(pattern)
        if "*" in pattern or "?" in pattern:
            files.extend(Path(".").glob(pattern))
        else:
            files.append(pattern_path)

    if not files:
        print("No files found to process")
        return 1

    valid_files = []
    if args.validate:
        if not args.quiet:
            print(f"Validating {len(files)} files...")
        for file_path in files:
            if validate_file(file_path):
                valid_files.append(file_path)
        files = valid_files

        if not files:
            print("No valid files to process")
            return 1

    # Process files
    summaries = []
    failed_count = 0

    for file_path in files:
        summary = process_file(file_path, args)
        if summary:
            summaries.append(summary)
        else:
            failed_count += 1

    # Create summary report if requested
    if args.summary and summaries:
        output_dir = Path(args.output) if args.output else Path(".")
        create_summary_report(summaries, output_dir)

    # Final status
    success_count = len(summaries)
    total_count = success_count + failed_count

    if not args.quiet:
        print("\n✅ Processing complete:")
        print(f"  Successfully processed: {success_count}/{total_count} files")
        if failed_count > 0:
            print(f"  Failed: {failed_count} files")

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
