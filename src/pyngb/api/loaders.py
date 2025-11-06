"""
High-level API functions for loading NGB data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Union, overload

import pyarrow as pa

from ..constants import FileMetadata
from ..core import NGBParser
from ..util import get_hash, set_metadata

__all__ = ["main", "read_ngb"]


@overload
def read_ngb(
    path: Union[str, Path],
    *,
    return_metadata: Literal[False] = False,
    baseline_file: None = None,
    dynamic_axis: str = "time",
) -> pa.Table: ...


@overload
def read_ngb(
    path: Union[str, Path],
    *,
    return_metadata: Literal[True],
    baseline_file: None = None,
    dynamic_axis: str = "time",
) -> tuple[FileMetadata, pa.Table]: ...


@overload
def read_ngb(
    path: Union[str, Path],
    *,
    return_metadata: Literal[False] = False,
    baseline_file: Union[str, Path],
    dynamic_axis: str = "time",
) -> pa.Table: ...


@overload
def read_ngb(
    path: Union[str, Path],
    *,
    return_metadata: Literal[True],
    baseline_file: Union[str, Path],
    dynamic_axis: str = "time",
) -> tuple[FileMetadata, pa.Table]: ...


def read_ngb(
    path: Union[str, Path],
    *,
    return_metadata: bool = False,
    baseline_file: Union[str, Path, None] = None,
    dynamic_axis: str = "sample_temperature",
) -> Union[pa.Table, tuple[FileMetadata, pa.Table]]:
    """
    Read NETZSCH NGB file data with optional baseline subtraction.

    This is the primary function for loading NGB files. By default, it returns
    a PyArrow table with embedded metadata. For direct metadata access, use return_metadata=True.
    When baseline_file is provided, baseline subtraction is performed automatically.

    Parameters
    ----------
    path : str or Path
        Path to the NGB file (.ngb-ss3 or similar extension).
        Supports absolute and relative paths, as strings or Path objects.
    return_metadata : bool, default False
        If False (default), return PyArrow table with embedded metadata.
        If True, return (metadata, data) tuple.
    baseline_file : str, Path, or None, default None
        Path to baseline file (.ngb-bs3) for baseline subtraction.
        If provided, performs automatic baseline subtraction. The baseline file
        must have an identical temperature program to the sample file.
    dynamic_axis : str, default "sample_temperature"
        Axis to use for dynamic segment alignment in baseline subtraction.
        Options: "time", "sample_temperature", "furnace_temperature"

    Returns
    -------
    pa.Table or tuple[FileMetadata, pa.Table]
        - If return_metadata=False: PyArrow table with embedded metadata
        - If return_metadata=True: (metadata dict, PyArrow table) tuple
        - If baseline_file provided: baseline-subtracted data

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist
    NGBStreamNotFoundError
        If required data streams are missing from the NGB file
    NGBCorruptedFileError
        If the file structure is invalid or corrupted
    zipfile.BadZipFile
        If the file is not a valid ZIP archive

    Examples
    --------
    Basic usage (recommended for most users):

    >>> from pyngb import read_ngb
    >>> import polars as pl
    >>>
    >>> # Load NGB file
    >>> data = read_ngb("experiment.ngb-ss3")
    >>>
    >>> # Convert to DataFrame for analysis
    >>> df = pl.from_arrow(data)
    >>> print(f"Shape: {df.height} rows x {df.width} columns")
    Shape: 2500 rows x 8 columns

    >>> # Access embedded metadata
    >>> import json
    >>> metadata = json.loads(data.schema.metadata[b'file_metadata'])
    >>> print(f"Sample: {metadata['sample_name']}")
    >>> print(f"Instrument: {metadata['instrument']}")
    Sample: Polymer Sample A
    Instrument: NETZSCH STA 449 F3 Jupiter

    Advanced usage (for metadata-heavy workflows):

    >>> # Get metadata and data separately
    >>> metadata, data = read_ngb("experiment.ngb-ss3", return_metadata=True)
    >>>
    >>> # Work with metadata directly
    >>> print(f"Operator: {metadata.get('operator', 'Unknown')}")
    >>> print(f"Sample mass: {metadata.get('sample_mass', 0)} mg")
    >>> print(f"Data points: {data.num_rows}")
    Operator: Jane Smith
    Sample mass: 15.2 mg
    Data points: 2500

    >>> # Use metadata for data processing
    >>> df = pl.from_arrow(data)
    >>> initial_mass = metadata['sample_mass']
    >>> df = df.with_columns(
    ...     (pl.col('mass') / initial_mass * 100).alias('mass_percent')
    ... )

    Data analysis workflow:

    >>> # Simple analysis
    >>> data = read_ngb("sample.ngb-ss3")
    >>> df = pl.from_arrow(data)
    >>>
    >>> # Basic statistics
    >>> if "sample_temperature" in df.columns:
    ...     temp_range = df["sample_temperature"].min(), df["sample_temperature"].max()
    ...     print(f"Temperature range: {temp_range[0]:.1f} to {temp_range[1]:.1f} °C")
    Temperature range: 25.0 to 800.0 °C

    >>> # Mass loss calculation
    >>> if "mass" in df.columns:
    ...     mass_loss = (df["mass"].max() - df["mass"].min()) / df["mass"].max() * 100
    ...     print(f"Mass loss: {mass_loss:.2f}%")
    Mass loss: 12.3%

    Performance Notes
    -----------------
    - Fast binary parsing with NumPy optimization
    - Memory-efficient processing with PyArrow
    - Typical parsing time: 0.1-10 seconds depending on file size
    - Includes file hash for integrity verification

    See Also
    --------
    NGBParser : Low-level parser for custom processing
    BatchProcessor : Process multiple files efficiently
    """
    parser = NGBParser()
    metadata, data = parser.parse(path)

    # Add file hash to metadata
    file_hash = get_hash(path)
    if file_hash is not None:
        metadata["file_hash"] = {
            "file": Path(path).name,
            "method": "BLAKE2b",
            "hash": file_hash,
        }

    # Handle baseline subtraction if requested
    if baseline_file is not None:
        from ..baseline import subtract_baseline

        # Validate dynamic_axis
        valid_axes = ["time", "sample_temperature", "furnace_temperature"]
        if dynamic_axis not in valid_axes:
            raise ValueError(
                f"dynamic_axis must be one of {valid_axes}, got '{dynamic_axis}'"
            )

        # Perform baseline subtraction (this will load baseline metadata internally)
        subtracted_df = subtract_baseline(
            path,
            baseline_file,
            dynamic_axis,  # type: ignore  # We validated it above
        )

        # Convert back to PyArrow
        data = subtracted_df.to_arrow()

    if return_metadata:
        return metadata, data

    # Attach metadata to the Arrow table
    data = set_metadata(data, tbl_meta={"file_metadata": metadata, "type": "STA"})

    # Initialize column metadata for all columns
    from ..util import initialize_table_column_metadata

    data = initialize_table_column_metadata(data)

    return data


def main() -> int:
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

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    import argparse
    import logging

    # Import these here to avoid circular imports
    import polars as pl
    import pyarrow.parquet as pq

    parser_cli = argparse.ArgumentParser(description="Parse NETZSCH STA NGB files")
    parser_cli.add_argument("input", help="Input NGB file path")
    parser_cli.add_argument("-o", "--output", help="Output directory", default=".")
    parser_cli.add_argument(
        "-f",
        "--format",
        choices=["parquet", "csv", "all"],
        default="parquet",
        help="Output format",
    )
    parser_cli.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser_cli.add_argument(
        "-b", "--baseline", help="Baseline file path for baseline subtraction"
    )
    parser_cli.add_argument(
        "--dynamic-axis",
        choices=["time", "sample_temperature", "furnace_temperature"],
        default="sample_temperature",
        help="Axis for dynamic segment alignment during baseline subtraction (default: sample_temperature)",
    )

    args = parser_cli.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    logger = logging.getLogger(__name__)

    # Validate input file
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file does not exist: {args.input}")
        return 1

    if not input_path.is_file():
        logger.error(f"Input path is not a file: {args.input}")
        return 1

    # Check if it's a valid NGB file extension
    valid_extensions = {".ngb-ss3", ".ngb-bs3"}
    if input_path.suffix.lower() not in valid_extensions:
        logger.warning(
            f"File extension '{input_path.suffix}' may not be a standard NGB format. Proceeding anyway."
        )

    try:
        # Validate baseline file if provided
        if args.baseline:
            baseline_path = Path(args.baseline)
            if not baseline_path.exists():
                logger.error(f"Baseline file does not exist: {args.baseline}")
                return 1
            if not baseline_path.is_file():
                logger.error(f"Baseline path is not a file: {args.baseline}")
                return 1
            if baseline_path.suffix.lower() not in valid_extensions:
                logger.warning(
                    f"Baseline file extension '{baseline_path.suffix}' may not be a standard NGB format. Proceeding anyway."
                )

        # Load data with optional baseline subtraction
        if args.baseline:
            logger.info(
                f"Loading data with baseline subtraction (dynamic_axis={args.dynamic_axis})"
            )
            data = read_ngb(
                args.input, baseline_file=args.baseline, dynamic_axis=args.dynamic_axis
            )
        else:
            data = read_ngb(args.input)

        output_path = Path(args.output)

        # Validate output directory
        try:
            output_path.mkdir(parents=True, exist_ok=True)
            # Test write permissions by creating a temporary file
            test_file = output_path / ".write_test"
            test_file.touch()
            test_file.unlink()
        except (PermissionError, OSError) as e:
            logger.error(f"Cannot write to output directory {args.output}: {e}")
            return 1

        base_name = Path(args.input).stem
        # Add suffix to indicate baseline subtraction was performed
        if args.baseline:
            base_name += "_baseline_subtracted"

        if args.format in ("parquet", "all"):
            pq.write_table(
                data, output_path / f"{base_name}.parquet", compression="snappy"
            )
        if args.format in ("csv", "all"):
            # Optimize: Only convert to Polars when needed for CSV output
            df = pl.from_arrow(data)
            # Ensure we have a DataFrame for CSV writing
            if isinstance(df, pl.DataFrame):
                df.write_csv(output_path / f"{base_name}.csv")

        if args.baseline:
            logger.info(
                f"Successfully parsed {args.input} with baseline subtraction from {args.baseline}"
            )
        else:
            logger.info(f"Successfully parsed {args.input}")
        return 0
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input}")
        return 1
    except PermissionError:
        logger.error(
            f"Permission denied accessing file or output directory: {args.input}"
        )
        return 1
    except OSError as e:
        logger.error(f"OS error while processing file {args.input}: {e}")
        return 1
    except ImportError as e:
        logger.error(f"Required dependency not available: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error while parsing file {args.input}: {e}")
        return 1
