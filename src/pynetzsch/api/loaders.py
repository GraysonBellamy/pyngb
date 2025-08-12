"""
High-level API functions for loading NGB data.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from ..constants import FileMetadata
from ..core import NGBParser
from ..util import get_hash, set_metadata

__all__ = ["get_sta_data", "load_ngb_data", "main"]


def load_ngb_data(path: str) -> pa.Table:
    """
    Load a NETZSCH STA NGB file and return PyArrow table with embedded metadata.

    This is the primary public interface for loading NGB files. It parses the
    file and returns a PyArrow table with all measurement data and metadata
    embedded in the table's schema metadata.

    Parameters
    ----------
    path : str
        The path to the NGB file (.ngb-ss3 or similar extension).
        Supports absolute and relative paths.

    Returns
    -------
    pa.Table
        PyArrow table containing:
        - Measurement data as columns (time, temperature, mass, etc.)
        - Embedded metadata in table.schema.metadata
        - File hash for integrity verification

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
    Basic usage:

    >>> import pyarrow as pa
    >>> from pynetzsch import load_ngb_data
    >>>
    >>> # Load NGB file
    >>> table = load_ngb_data("experiment.ngb-ss3")
    >>>
    >>> # Examine structure
    >>> print(f"Shape: {table.num_rows} rows, {table.num_columns} columns")
    >>> print(f"Columns: {table.column_names}")
    Shape: 2500 rows, 8 columns
    Columns: ['time', 'temperature', 'mass', 'dsc', 'purge_flow', ...]

    Accessing metadata:

    >>> # Get embedded metadata
    >>> import json
    >>> metadata_bytes = table.schema.metadata[b'file_metadata']
    >>> metadata = json.loads(metadata_bytes)
    >>>
    >>> print(f"Instrument: {metadata['instrument']}")
    >>> print(f"Sample: {metadata['sample_name']}")
    >>> print(f"Mass: {metadata['sample_mass']} mg")
    Instrument: NETZSCH STA 449 F3 Jupiter
    Sample: Polymer Sample A
    Mass: 15.2 mg

    Working with data:

    >>> # Convert to polars for analysis
    >>> import polars as pl
    >>> df = pl.from_arrow(table)
    >>>
    >>> # Basic analysis
    >>> temp_range = df['temperature'].min(), df['temperature'].max()
    >>> mass_loss = (df['mass'].first() - df['mass'].last()) / df['mass'].first() * 100
    >>> print(f"Temperature range: {temp_range[0]:.1f} to {temp_range[1]:.1f} °C")
    >>> print(f"Mass loss: {mass_loss:.1f}%")
    Temperature range: 25.0 to 800.0 °C
    Mass loss: 12.3%

    Performance Notes
    -----------------
    - Uses optimized NumPy operations for fast binary parsing
    - Memory-efficient processing with memoryview operations
    - Compiled regex patterns for repeated pattern matching
    - Typical parsing time: 0.1-10 seconds depending on file size

    See Also
    --------
    get_sta_data : Get metadata and data as separate objects
    NGBParser : Low-level parser for advanced use cases
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

    # Attach metadata to the Arrow table
    data = set_metadata(data, tbl_meta={"file_metadata": metadata, "type": "STA"})
    return data


def get_sta_data(path: str) -> tuple[FileMetadata, pa.Table]:
    """
    Get STA data and metadata from an NGB file as separate objects.

    This function provides access to the parsed data and metadata as separate
    objects, which can be useful when you need to work with metadata independently
    of the measurement data.

    Parameters
    ----------
    path : str
        Path to the .ngb-ss3 file to parse.
        Supports absolute and relative paths.

    Returns
    -------
    tuple[FileMetadata, pa.Table]
        A tuple containing:
        - FileMetadata: TypedDict with instrument settings, sample info, etc.
        - pa.Table: PyArrow table with measurement data columns

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist
    NGBStreamNotFoundError
        If required data streams are missing
    NGBCorruptedFileError
        If file structure is invalid

    Examples
    --------
    Basic usage:

    >>> from pynetzsch import get_sta_data
    >>>
    >>> metadata, data = get_sta_data("sample.ngb-ss3")
    >>>
    >>> # Work with metadata
    >>> print(f"Operator: {metadata.get('operator', 'Unknown')}")
    >>> print(f"Date: {metadata.get('date_performed', 'Unknown')}")
    >>>
    >>> # Work with data
    >>> print(f"Data points: {data.num_rows}")
    >>> print(f"Measurements: {data.column_names}")
    Operator: John Doe
    Date: 2024-03-15T10:30:00+00:00
    Data points: 2500
    Measurements: ['time', 'temperature', 'mass', 'dsc']

    Advanced metadata access:

    >>> # Access temperature program
    >>> temp_program = metadata.get('temperature_program', {})
    >>> for step, params in temp_program.items():
    ...     print(f"{step}: {params}")
    step_1: {'heating_rate': 10.0, 'temperature': 800.0, 'time': 80.0}

    >>> # Access calibration constants
    >>> cal_constants = metadata.get('calibration_constants', {})
    >>> print(f"Calibration: {cal_constants}")
    Calibration: {'p0': 1.0, 'p1': 0.98, 'p2': 0.001}

    Data analysis workflow:

    >>> import polars as pl
    >>>
    >>> # Convert to DataFrame
    >>> df = pl.from_arrow(data)
    >>>
    >>> # Filter data using metadata
    >>> initial_mass = metadata.get('sample_mass', 0)
    >>> if initial_mass > 0:
    ...     df = df.with_columns([
    ...         (pl.col('mass') / initial_mass * 100).alias('mass_percent')
    ...     ])
    >>>
    >>> print(df.head())

    See Also
    --------
    load_ngb_data : Load data with embedded metadata in PyArrow table
    NGBParser : Low-level parser class for custom processing
    """
    parser = NGBParser()
    return parser.parse(path)


def main() -> int:
    """Command-line interface for the NGB parser.

    Provides a command-line tool for parsing NGB files and converting
    them to various output formats including Parquet and CSV.

    Usage:
        python -m pynetzsch input.ngb-ss3 [options]

    Examples:
        # Parse to Parquet (default)
        python -m pynetzsch sample.ngb-ss3

        # Parse to CSV with verbose logging
        python -m pynetzsch sample.ngb-ss3 -f csv -v

        # Parse to both formats in custom directory
        python -m pynetzsch sample.ngb-ss3 -f all -o /output/dir

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

    args = parser_cli.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    logger = logging.getLogger(__name__)

    try:
        data = load_ngb_data(args.input)
        output_path = Path(args.output)
        output_path.mkdir(parents=True, exist_ok=True)

        base_name = Path(args.input).stem
        if args.format in ("parquet", "all"):
            pq.write_table(
                data, output_path / f"{base_name}.parquet", compression="snappy"
            )
        if args.format in ("csv", "all"):
            df = pl.from_arrow(data).to_pandas()
            df.to_csv(output_path / f"{base_name}.csv", index=False)

        logger.info("Successfully parsed %s", args.input)
        return 0
    except Exception as e:
        logger.error("Failed to parse file: %s", e)
        return 1
