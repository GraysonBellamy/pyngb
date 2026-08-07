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
from ..format import build_dataframe, build_metadata, count_runs, load_document
from ..util import get_hash, initialize_table_column_metadata, set_metadata
from .metadata import mark_baseline_corrected

__all__ = ["read_ngb", "read_ngb_metadata"]

#: Measurement runs a file can physically contain, in stream order.
_RUNS = ("sample", "correction")

#: Run selectors accepted by :func:`read_ngb`: the two embedded runs, plus
#: "corrected" — the sample run with the embedded correction subtracted.
_SELECTORS = ("sample", "correction", "corrected")


def _load(path: str | Path, limits: ParsingConfig | None):
    """Load the full-parse document.

    Loader policy: streams 1 and 2 are required, stream 3 is optional.
    """
    try:
        return load_document(path, streams=(1, 2, 3), limits=limits)
    except NGBStreamNotFoundError:
        # Stream 3 is optional; if 1 or 2 is the one missing, this second
        # request raises again with the accurate message.
        return load_document(path, streams=(1, 2), limits=limits)


def _parse(
    path: str | Path, limits: ParsingConfig | None, run: str = "sample"
) -> tuple[FileMetadata, pl.DataFrame]:
    """Parse metadata and one measurement run through the document layer.

    Single seam shared by every full-parse path (plain, baseline sample,
    baseline reference) so the two halves can never diverge.

    ``run`` is "sample" or "correction". "Sample + Correction" ``.ngb-ds3``
    files embed both measurements; "correction" selects the embedded
    correction and raises on files that carry none. The metadata is
    file-level and always describes the sample measurement (the correction's
    provenance is recorded in its ``correction_file_path`` key).
    """
    doc = _load(path, limits)
    run_index = _RUNS.index(run)
    if run_index > 0 and count_runs(doc) < 2:
        raise ValueError(
            f"{path} contains no embedded correction run; only "
            '"Sample + Correction" .ngb-ds3 files carry one'
        )
    return build_metadata(doc), build_dataframe(doc, run=run_index)


def _parse_baseline(
    path: str | Path, limits: ParsingConfig | None, *, require_embedded: bool = False
) -> tuple[FileMetadata, pl.DataFrame]:
    """Parse a file *as a baseline*: the correction curves it provides.

    A standalone ``.ngb-bs3`` IS a correction measurement, so its single run
    is the baseline. A ``.ngb-ds3`` passed as a baseline (typically the
    sample file itself) contributes its embedded correction run — this is
    what makes ``run="corrected"`` reproduce the corrected curves Proteus
    displays for a Sample + Correction measurement.

    ``require_embedded`` demands an embedded correction run (the
    ``run="corrected"`` path, where the file must be its own baseline);
    without it a single-run file contributes its only run.
    """
    doc = _load(path, limits)
    n_runs = count_runs(doc)
    if require_embedded and n_runs < 2:
        raise ValueError(
            f"{path} contains no embedded correction run; only "
            '"Sample + Correction" .ngb-ds3 files carry one'
        )
    run_index = 1 if n_runs >= 2 else 0
    return build_metadata(doc), build_dataframe(doc, run=run_index)


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    run: Literal["sample", "correction", "corrected"] = "sample",
    baseline_file: None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    run: Literal["sample", "correction", "corrected"] = "sample",
    baseline_file: None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> tuple[FileMetadata, pa.Table]: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[False] = False,
    run: Literal["sample", "correction", "corrected"] = "sample",
    baseline_file: str | Path,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table: ...


@overload
def read_ngb(
    path: str | Path,
    *,
    return_metadata: Literal[True],
    run: Literal["sample", "correction", "corrected"] = "sample",
    baseline_file: str | Path,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> tuple[FileMetadata, pa.Table]: ...


def read_ngb(
    path: str | Path,
    *,
    return_metadata: bool = False,
    run: Literal["sample", "correction", "corrected"] = "sample",
    baseline_file: str | Path | None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table | tuple[FileMetadata, pa.Table]:
    """
    Read NETZSCH NGB file data with optional baseline subtraction.

    This is the primary function for loading NGB files. By default, it returns
    a PyArrow table with embedded metadata. For direct metadata access, use return_metadata=True.
    When baseline_file is provided, baseline subtraction is performed automatically.

    "Sample + Correction" measurements (``.ngb-ds3``) embed two complete raw
    measurements in one file: the sample run and a verbatim copy of the
    correction run it was measured against. Neither is subtracted from the
    other in the stored data — Proteus applies the correction at display
    time. By default the raw sample run is returned; ``run="correction"``
    returns the embedded correction, and ``run="corrected"`` subtracts the
    embedded correction from the sample run, reproducing the corrected
    curves Proteus displays.

    Parameters
    ----------
    path : str or Path
        Path to the NGB file (.ngb-ss3, .ngb-bs3, .ngb-ds3 or similar).
        Supports absolute and relative paths, as strings or Path objects.
    return_metadata : bool, default False
        If False (default), return PyArrow table with embedded metadata.
        If True, return (metadata, data) tuple.
    run : {"sample", "correction", "corrected"}, default "sample"
        What to return: the raw sample run, the embedded correction run, or
        the sample run with the embedded correction subtracted. "correction"
        and "corrected" are only valid for files that embed a correction
        (.ngb-ds3) and cannot be combined with baseline_file. Metadata
        always describes the sample measurement; the correction's provenance
        is in its ``correction_file_path`` key. The selected run is recorded
        in the returned table's schema metadata under the ``run`` key.
    baseline_file : str, Path, or None, default None
        Path to a file providing correction curves for baseline subtraction.
        A ``.ngb-bs3`` contributes its (only) run; a ``.ngb-ds3`` — typically
        the sample file itself — contributes its embedded correction run.
        The baseline must have an identical temperature program to the
        sample file.
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
        If dynamic_axis or run is not a recognized value, a non-"sample" run
        is combined with baseline_file or requested from a file with no
        embedded correction run
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

    Sample + Correction files (.ngb-ds3):

    >>> raw = read_ngb("run.ngb-ds3")                      # raw sample run
    >>> corr = read_ngb("run.ngb-ds3", run="correction")   # embedded correction
    >>> # Corrected curves, as Proteus displays them:
    >>> corrected = read_ngb("run.ngb-ds3", run="corrected")

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
    if run not in _SELECTORS:
        raise ValueError(f"run must be one of {list(_SELECTORS)}, got '{run}'")
    if run != "sample" and baseline_file is not None:
        raise ValueError(
            f"run='{run}' cannot be combined with baseline_file: the "
            "embedded correction run is the baseline"
        )
    # "corrected" is the sample run baseline-subtracted against the file's
    # own embedded correction — the file is its own baseline.
    self_correct = run == "corrected"
    if self_correct:
        baseline_file = path

    metadata, data_df = _parse(path, limits, "sample" if self_correct else run)

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
        baseline_metadata, baseline_df = _parse_baseline(
            baseline_file, limits, require_embedded=self_correct
        )
        data_df = BaselineSubtractor().process_baseline_subtraction(
            data_df, baseline_df, metadata, baseline_metadata, dynamic_axis
        )

    # Convert to PyArrow at the API boundary for cross-language compatibility
    # and metadata embedding.
    data = data_df.to_arrow()

    if not return_metadata:
        # Attach file-level metadata to the Arrow schema; with
        # return_metadata=True it is handed back separately instead. The
        # "run" tag records which run the table holds — the file-level
        # metadata alone cannot distinguish the exports.
        data = set_metadata(
            data,
            tbl_meta={"file_metadata": metadata, "type": "STA", "run": run},
        )

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

    Metadata is file-level: for "Sample + Correction" ``.ngb-ds3`` files it
    describes the sample measurement, with the correction identified by the
    ``correction_file_path`` key.

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
