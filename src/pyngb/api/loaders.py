"""
High-level API functions for loading NGB data.
"""

from pathlib import Path
from typing import Literal, overload

import pyarrow as pa

from ..constants import FileMetadata
from ..core import NGBParser
from ..util import get_hash, set_metadata

__all__ = ["main", "read_ngb"]


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    baseline_file: None = None,
    dynamic_axis: str = "time",
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    baseline_file: None = None,
    dynamic_axis: str = "time",
) -> tuple[FileMetadata, pa.Table]: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    baseline_file: str | Path,
    dynamic_axis: str = "time",
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    baseline_file: str | Path,
    dynamic_axis: str = "time",
) -> tuple[FileMetadata, pa.Table]: ...


def read_ngb(
    path: str | Path,
    *,
    return_metadata: bool = False,
    baseline_file: str | Path | None = None,
    dynamic_axis: str = "sample_temperature",
) -> pa.Table | tuple[FileMetadata, pa.Table]:
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

    .. deprecated:: 0.2.0
        This function has been moved to pyngb.api.cli.main().
        This wrapper is maintained for backward compatibility.

    Provides a command-line tool for parsing NGB files and converting
    them to various output formats including Parquet and CSV.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    from .cli import main as cli_main

    return cli_main()
