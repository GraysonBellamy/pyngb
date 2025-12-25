"""Command-line interface for pyNGB."""

import argparse
import logging
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .loaders import read_ngb

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser.

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(description="Parse NETZSCH STA NGB files")
    parser.add_argument("input", help="Input NGB file path")
    parser.add_argument("-o", "--output", help="Output directory", default=".")
    parser.add_argument(
        "-f",
        "--format",
        choices=["parquet", "csv", "all"],
        default="parquet",
        help="Output format",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "-b", "--baseline", help="Baseline file path for baseline subtraction"
    )
    parser.add_argument(
        "--dynamic-axis",
        choices=["time", "sample_temperature", "furnace_temperature"],
        default="sample_temperature",
        help="Axis for dynamic segment alignment during baseline subtraction (default: sample_temperature)",
    )

    return parser


def validate_input_file(input_path: Path) -> None:
    """Validate input NGB file exists and is valid.

    Args:
        input_path: Path to input NGB file

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path is not a file
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    if not input_path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")

    # Check if it's a valid NGB file extension
    valid_extensions = {".ngb-ss3", ".ngb-bs3"}
    if input_path.suffix.lower() not in valid_extensions:
        logger.warning(
            f"File extension '{input_path.suffix}' may not be a standard NGB format. Proceeding anyway."
        )


def validate_baseline_file(baseline_path: Path) -> None:
    """Validate baseline file exists and is valid.

    Args:
        baseline_path: Path to baseline NGB file

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path is not a file
    """
    if not baseline_path.exists():
        raise FileNotFoundError(f"Baseline file does not exist: {baseline_path}")

    if not baseline_path.is_file():
        raise ValueError(f"Baseline path is not a file: {baseline_path}")

    valid_extensions = {".ngb-ss3", ".ngb-bs3"}
    if baseline_path.suffix.lower() not in valid_extensions:
        logger.warning(
            f"Baseline file extension '{baseline_path.suffix}' may not be a standard NGB format. Proceeding anyway."
        )


def validate_output_directory(output_path: Path) -> None:
    """Validate output directory is writable.

    Args:
        output_path: Path to output directory

    Raises:
        PermissionError: If directory is not writable
        OSError: If directory cannot be created
    """
    # Create directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Test write permissions by creating a temporary file
    test_file = output_path / ".write_test"
    try:
        test_file.touch()
        test_file.unlink()
    except (PermissionError, OSError) as e:
        raise PermissionError(
            f"Cannot write to output directory {output_path}: {e}"
        ) from e


def load_data(
    input_file: str, baseline_file: str | None, dynamic_axis: str
) -> pa.Table:
    """Load NGB data with optional baseline subtraction.

    Args:
        input_file: Path to input NGB file
        baseline_file: Optional path to baseline NGB file
        dynamic_axis: Axis for dynamic segment alignment

    Returns:
        PyArrow Table with loaded data
    """
    if baseline_file:
        logger.info(
            f"Loading data with baseline subtraction (dynamic_axis={dynamic_axis})"
        )
        return read_ngb(
            input_file, baseline_file=baseline_file, dynamic_axis=dynamic_axis
        )
    return read_ngb(input_file)


def write_output_files(
    data: pa.Table,
    output_path: Path,
    base_name: str,
    output_format: str,
) -> None:
    """Write parsed data to output file(s).

    Args:
        data: PyArrow Table to write
        output_path: Directory to write files to
        base_name: Base filename (without extension)
        output_format: Output format ("parquet", "csv", or "all")
    """
    if output_format in ("parquet", "all"):
        parquet_file = output_path / f"{base_name}.parquet"
        pq.write_table(data, parquet_file, compression="snappy")
        logger.debug(f"Wrote Parquet file: {parquet_file}")

    if output_format in ("csv", "all"):
        # Optimize: Only convert to Polars when needed for CSV output
        df = pl.from_arrow(data)
        # Ensure we have a DataFrame for CSV writing
        if isinstance(df, pl.DataFrame):
            csv_file = output_path / f"{base_name}.csv"
            df.write_csv(csv_file)
            logger.debug(f"Wrote CSV file: {csv_file}")


def main(argv: list[str] | None = None) -> int:
    """Command-line interface for the NGB parser.

    Provides a command-line tool for parsing NGB files and converting
    them to various output formats including Parquet and CSV.

    Usage:
        python -m pyngb input.ngb-ss3 [options]

    Examples:
        # Parse to Parquet (default)
        python -m pyngb sample.ngb-ss3

        # Parse to CSV with verbose logging
        python -m pyngb sample.ngb-ss3 -f csv -v

        # Parse to both formats in custom directory
        python -m pyngb sample.ngb-ss3 -f all -o /output/dir

    Args:
        argv: Command-line arguments (defaults to sys.argv)

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Configure logging
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))

    try:
        # Validate input file
        input_path = Path(args.input)
        validate_input_file(input_path)

        # Validate baseline file if provided
        if args.baseline:
            baseline_path = Path(args.baseline)
            validate_baseline_file(baseline_path)

        # Load data
        data = load_data(args.input, args.baseline, args.dynamic_axis)

        # Validate and prepare output directory
        output_path = Path(args.output)
        validate_output_directory(output_path)

        # Determine output filename
        base_name = input_path.stem
        # Add suffix to indicate baseline subtraction was performed
        if args.baseline:
            base_name += "_baseline_subtracted"

        # Write output files
        write_output_files(data, output_path, base_name, args.format)

        # Log success
        if args.baseline:
            logger.info(
                f"Successfully parsed {args.input} with baseline subtraction from {args.baseline}"
            )
        else:
            logger.info(f"Successfully parsed {args.input}")

        return 0

    except Exception as e:
        match e:
            case FileNotFoundError() | ValueError() | PermissionError():
                logger.error(str(e))
            case OSError():
                logger.error(f"OS error while processing file {args.input}: {e}")
            case ImportError():
                logger.error(f"Required dependency not available: {e}")
            case _:
                logger.error(f"Unexpected error while parsing file {args.input}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
