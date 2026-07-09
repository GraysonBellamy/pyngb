"""
High-level API functions for loading NGB data.
"""

from pathlib import Path
from typing import Literal, overload

import polars as pl
import pyarrow as pa

from ..baseline import BaselineSubtractor
from ..config import ParsingConfig
from ..constants import FileMetadata
from ..exceptions import NGBStreamNotFoundError
from ..format import build_dataframe, build_metadata, load_document
from ..util import get_hash, initialize_table_column_metadata, set_metadata
from .metadata import mark_baseline_corrected

__all__ = ["read_ngb", "read_ngb_metadata"]


def _parse(
    path: str | Path, limits: ParsingConfig | None
) -> tuple[FileMetadata, pl.DataFrame]:
    """Parse metadata and measurement data through the document layer.

    Single seam shared by every full-parse path (plain, baseline sample,
    baseline reference) so the two halves can never diverge.

    Loader policy: streams 1 and 2 are required, stream 3 is optional.
    """
    try:
        doc = load_document(path, streams=(1, 2, 3), limits=limits)
    except NGBStreamNotFoundError:
        # Stream 3 is optional; if 1 or 2 is the one missing, this second
        # request raises again with the accurate message.
        doc = load_document(path, streams=(1, 2), limits=limits)
    return build_metadata(doc), build_dataframe(doc)


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    baseline_file: None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    baseline_file: None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> tuple[FileMetadata, pa.Table]: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    baseline_file: str | Path,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    baseline_file: str | Path,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> tuple[FileMetadata, pa.Table]: ...


def read_ngb(
    path: str | Path,
    *,
    return_metadata: bool = False,
    baseline_file: str | Path | None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
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
    limits : ParsingConfig or None, default None
        Resource limits (stream size, array size, table count) enforced while
        parsing. None uses the defaults, which leave orders of magnitude of
        headroom over real files.

    Returns
    -------
    pa.Table or tuple[FileMetadata, pa.Table]
        - If return_metadata=False: PyArrow table with embedded metadata
        - If return_metadata=True: (metadata dict, PyArrow table) tuple
        - If baseline_file provided: baseline-subtracted data

    Raises
    ------
    ValueError
        If dynamic_axis is not a recognized axis name
    FileNotFoundError
        If the specified file does not exist
    NGBStreamNotFoundError
        If required data streams are missing from the NGB file
    NGBCorruptedFileError
        If the file structure is invalid or corrupted
    NGBResourceLimitError
        If a stream or data payload exceeds the configured resource limits
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
    - Strict single-pass tokenization with NumPy-backed array decoding
    - Memory-efficient processing with PyArrow
    - Typical parsing time: well under a second per file
    - Includes file hash for integrity verification

    See Also
    --------
    read_ngb_metadata : Metadata without decoding the measurement streams
    load_document : The full parsed document model behind this function
    BatchProcessor : Process multiple files efficiently
    """
    valid_axes = ["time", "sample_temperature", "furnace_temperature"]
    if dynamic_axis not in valid_axes:
        raise ValueError(
            f"dynamic_axis must be one of {valid_axes}, got '{dynamic_axis}'"
        )

    metadata, data_df = _parse(path, limits)

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
        baseline_metadata, baseline_df = _parse(baseline_file, limits)
        data_df = BaselineSubtractor().process_baseline_subtraction(
            data_df, baseline_df, metadata, baseline_metadata, dynamic_axis
        )

    # Convert to PyArrow at the API boundary for cross-language compatibility
    # and metadata embedding.
    data = data_df.to_arrow()

    if not return_metadata:
        # Attach file-level metadata to the Arrow schema; with
        # return_metadata=True it is handed back separately instead.
        data = set_metadata(data, tbl_meta={"file_metadata": metadata, "type": "STA"})

    # Column metadata (units, processing history, source) is present on every
    # return path; baseline subtraction changes the meaning of the mass/DSC
    # columns, so tag them as corrected.
    data = initialize_table_column_metadata(data)
    if baseline_file is not None:
        data = mark_baseline_corrected(data, ["mass", "dsc_signal"])

    if return_metadata:
        return metadata, data
    return data


def read_ngb_metadata(
    path: str | Path, *, limits: ParsingConfig | None = None
) -> FileMetadata:
    """Extract file metadata without decoding the measurement streams.

    Reads and processes only stream_1, skipping the stream_2/stream_3 data
    decoding that dominates a full parse. Use this for dataset-level
    operations (summaries, filtering, metadata export) that never touch the
    measurement data.

    Unlike :func:`read_ngb`, the returned metadata carries no ``file_hash``
    key — the hash covers the whole file, which this path deliberately does
    not read in full.

    Args:
        path: Path to the .ngb-ss3 file to parse
        limits: Resource limits enforced while parsing; None uses defaults.

    Returns:
        Metadata dictionary with instrument settings, sample info, etc.

    Raises:
        FileNotFoundError: If the specified file doesn't exist
        NGBStreamNotFoundError: If stream 1 is missing
        NGBCorruptedFileError: If the container structure is invalid
        NGBResourceLimitError: If a stream exceeds the configured resource limits
        zipfile.BadZipFile: If the file is not a valid ZIP archive

    Example:
        >>> from pyngb import read_ngb_metadata
        >>> metadata = read_ngb_metadata("experiment.ngb-ss3")
        >>> print(metadata.get("sample_name"))
    """
    doc = load_document(path, streams=(1,), limits=limits)
    return build_metadata(doc)
