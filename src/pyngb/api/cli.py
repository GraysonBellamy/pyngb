"""Command-line interface for pyNGB."""

import argparse
import logging
import sys
import zipfile
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..exceptions import NGBParseError
from .loaders import read_ngb

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser.

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(description="Parse NETZSCH STA NGB files")
    parser.add_argument("input", nargs="+", help="Input NGB file path(s)")
    parser.add_argument("-o", "--output", help="Output directory", default=".")
    parser.add_argument(
        "-f",
        "--format",
        choices=["parquet", "csv", "both"],
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
        output_format: Output format ("parquet", "csv", or "both")
    """
    if output_format in ("parquet", "both"):
        parquet_file = output_path / f"{base_name}.parquet"
        pq.write_table(data, parquet_file, compression="snappy")
        logger.debug(f"Wrote Parquet file: {parquet_file}")

    if output_format in ("csv", "both"):
        # Optimize: Only convert to Polars when needed for CSV output
        df = pl.from_arrow(data)
        # Ensure we have a DataFrame for CSV writing
        if isinstance(df, pl.DataFrame):
            csv_file = output_path / f"{base_name}.csv"
            df.write_csv(csv_file)
            logger.debug(f"Wrote CSV file: {csv_file}")


def process_file(
    input_file: str,
    output_path: Path,
    output_format: str,
    baseline_file: str | None,
    dynamic_axis: str,
) -> None:
    """Parse one NGB file and write its output file(s).

    Args:
        input_file: Path to input NGB file
        output_path: Validated output directory
        output_format: Output format ("parquet", "csv", or "both")
        baseline_file: Optional path to baseline NGB file
        dynamic_axis: Axis for dynamic segment alignment

    Raises:
        Anything read_ngb or the filesystem raises; the caller decides
        whether one failure aborts the run.
    """
    input_path = Path(input_file)
    validate_input_file(input_path)

    data = load_data(input_file, baseline_file, dynamic_axis)

    base_name = input_path.stem
    # Add suffix to indicate baseline subtraction was performed
    if baseline_file:
        base_name += "_baseline_subtracted"

    write_output_files(data, output_path, base_name, output_format)

    if baseline_file:
        logger.info(
            f"Successfully parsed {input_file} with baseline subtraction from {baseline_file}"
        )
    else:
        logger.info(f"Successfully parsed {input_file}")


def main(argv: list[str] | None = None) -> int:
    """Command-line interface for the NGB parser.

    Provides a command-line tool for parsing NGB files and converting
    them to various output formats including Parquet and CSV.

    Usage:
        python -m pyngb input.ngb-ss3 [input2.ngb-ss3 ...] [options]

    Examples:
        # Parse to Parquet (default)
        python -m pyngb sample.ngb-ss3

        # Parse to CSV with verbose logging
        python -m pyngb sample.ngb-ss3 -f csv -v

        # Parse many files to both formats in a custom directory
        python -m pyngb *.ngb-ss3 -f both -o /output/dir

        # Baseline-subtract every input against the same baseline
        python -m pyngb *.ngb-ss3 -b baseline.ngb-bs3

    Args:
        argv: Command-line arguments (defaults to sys.argv)

    Returns:
        Exit code (0 when every file succeeded, 1 otherwise)
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Configure logging
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))

    # Validate shared inputs once; failures here abort the whole run.
    try:
        if args.baseline:
            validate_baseline_file(Path(args.baseline))
        output_path = Path(args.output)
        validate_output_directory(output_path)
    except (FileNotFoundError, ValueError, PermissionError, OSError) as e:
        logger.error(str(e))
        return 1

    # Per-file failures don't stop the remaining files.
    failures = 0
    for input_file in args.input:
        try:
            process_file(
                input_file, output_path, args.format, args.baseline, args.dynamic_axis
            )
        except zipfile.BadZipFile:
            logger.error(f"{input_file} is not a valid NGB file (not a ZIP archive)")
            failures += 1
        except (FileNotFoundError, ValueError, PermissionError) as e:
            logger.error(str(e))
            failures += 1
        except NGBParseError as e:
            logger.error(f"Failed to parse {input_file}: {e}")
            failures += 1
        except OSError as e:
            logger.error(f"OS error while processing file {input_file}: {e}")
            failures += 1

    if failures:
        logger.error(f"{failures} of {len(args.input)} file(s) failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
