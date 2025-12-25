"""
High-level API functions for thermal analysis calculations.

This module provides convenient functions for performing DTG analysis
and mass normalization on PyArrow tables.
"""

import json
import numpy as np
import polars as pl
import pyarrow as pa

from ..analysis import dtg

__all__ = [
    "add_dtg",
    "apply_dsc_calibration",
    "calculate_table_dtg",
    "normalize_to_initial_mass",
]


def add_dtg(
    table: pa.Table,
    method: str = "savgol",
    smooth: str = "medium",
    column_name: str = "dtg",
) -> pa.Table:
    """
    Add DTG (derivative thermogravimetry) column to PyArrow table.

    This function calculates the derivative of mass with respect to time
    and adds it as a new column to the existing table.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing thermal analysis data. Must have 'time'
        and 'mass' columns.
    method : {"savgol", "gradient"}, default "savgol"
        DTG calculation method
    smooth : {"strict", "medium", "loose"}, default "medium"
        Smoothing level
    column_name : str, default "dtg"
        Name for the new DTG column

    Returns
    -------
    pa.Table
        New table with added DTG column and preserved metadata

    Raises
    ------
    ValueError
        If required columns ('time', 'mass') are missing from the table

    Examples
    --------
    >>> from pyngb import read_ngb
    >>> from pyngb.api.analysis import add_dtg
    >>>
    >>> # Load data
    >>> table = read_ngb("sample.ngb-ss3")
    >>>
    >>> # Add DTG column using default settings
    >>> table_with_dtg = add_dtg(table)
    >>>
    >>> # Use gradient method with strict smoothing
    >>> table_with_dtg = add_dtg(table, method="gradient", smooth="strict")
    """
    # Check required columns
    column_names = table.column_names
    if "time" not in column_names:
        raise ValueError("Table must contain 'time' column")
    if "mass" not in column_names:
        raise ValueError("Table must contain 'mass' column")

    # Convert to DataFrame for easier manipulation
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Failed to convert PyArrow table to Polars DataFrame")

    # Get data arrays
    time = df.get_column("time").to_numpy()
    mass = df.get_column("mass").to_numpy()

    # Calculate DTG
    dtg_values = dtg(time, mass, method=method, smooth=smooth)

    # Add DTG column
    df = df.with_columns(pl.Series(column_name, dtg_values))

    # Convert back to PyArrow table while preserving all metadata
    new_table = df.to_arrow()

    # Preserve table-level metadata
    if table.schema.metadata:
        new_table = new_table.replace_schema_metadata(table.schema.metadata)

    # Preserve column-level metadata for all existing columns
    from ..util import set_column_metadata, get_column_metadata

    for col in table.column_names:
        if col in new_table.column_names:  # Column exists in new table
            original_metadata = get_column_metadata(table, col)
            if original_metadata:  # If original column had metadata
                new_table = set_column_metadata(
                    new_table, col, original_metadata, replace=True
                )

    # Set metadata for the new DTG column
    dtg_metadata = {
        "units": "mg/min",
        "processing_history": ["calculated"],
        "source": "derived",
    }
    new_table = set_column_metadata(new_table, column_name, dtg_metadata, replace=True)

    return new_table


def calculate_table_dtg(
    table: pa.Table,
    method: str = "savgol",
    smooth: str = "medium",
) -> np.ndarray:
    """
    Calculate DTG from PyArrow table data without modifying the table.

    This function extracts the necessary columns from a PyArrow table and
    calculates DTG values, returning them as a NumPy array.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing thermal analysis data
    method : {"savgol", "gradient"}, default "savgol"
        DTG calculation method
    smooth : {"strict", "medium", "loose"}, default "medium"
        Smoothing level

    Returns
    -------
    np.ndarray
        DTG values as numpy array in mg/min

    Raises
    ------
    ValueError
        If required columns are missing from the table

    Examples
    --------
    >>> from pyngb import read_ngb
    >>> from pyngb.api.analysis import calculate_table_dtg
    >>>
    >>> table = read_ngb("sample.ngb-ss3")
    >>> dtg_values = calculate_table_dtg(table, method="savgol", smooth="medium")
    >>>
    >>> # Find maximum mass loss rate
    >>> max_loss_rate = abs(dtg_values.min())
    >>> print(f"Maximum mass loss rate: {max_loss_rate:.3f} mg/min")
    """
    # Check required columns
    column_names = table.column_names
    if "time" not in column_names:
        raise ValueError("Table must contain 'time' column")
    if "mass" not in column_names:
        raise ValueError("Table must contain 'mass' column")

    # Convert to DataFrame and extract arrays
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Failed to convert PyArrow table to Polars DataFrame")
    time = df.get_column("time").to_numpy()
    mass = df.get_column("mass").to_numpy()

    # Calculate and return DTG
    return dtg(time, mass, method=method, smooth=smooth)


def normalize_to_initial_mass(
    table: pa.Table,
    columns: list[str] | None = None,
) -> pa.Table:
    """
    Normalize mass and DSC columns to the initial sample mass from metadata.

    This function normalizes specified columns (typically 'mass' and DSC signals)
    by dividing by the initial sample mass stored in the table's metadata.
    The columns are updated in place, with units changed to show per-mass normalization
    (e.g., "mg" becomes "mg/mg") and "normalized" added to the processing history.
    The mass column starts at zero (tare weight), so the initial sample mass
    must be retrieved from the extraction metadata.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing thermal analysis data with embedded metadata
    columns : list of str, optional
        Column names to normalize. If None, defaults to ['mass', 'dsc_signal']
        if they exist in the table

    Returns
    -------
    pa.Table
        Table with specified columns normalized in place, updated units showing
        per-mass normalization, and "normalized" added to processing history

    Raises
    ------
    ValueError
        If sample_mass is not found in metadata or is zero/negative
    KeyError
        If specified columns are not found in the table

    Examples
    --------
    >>> from pyngb import read_ngb
    >>> from pyngb.api.analysis import normalize_to_initial_mass
    >>> from pyngb.api.metadata import get_column_units, get_processing_history
    >>>
    >>> # Load data with metadata
    >>> table = read_ngb("sample.ngb-ss3")
    >>> print(f"Before: {get_column_units(table, 'mass')}")  # "mg"
    >>>
    >>> # Normalize mass and DSC to initial sample mass (in place)
    >>> normalized_table = normalize_to_initial_mass(table)
    >>> print(f"After: {get_column_units(normalized_table, 'mass')}")  # "mg/mg"
    >>> print(f"History: {get_processing_history(normalized_table, 'mass')}")  # ["raw", "normalized"]
    >>>
    >>> # Normalize only specific columns
    >>> normalized_table = normalize_to_initial_mass(table, columns=['mass'])
    >>>
    >>> # Check normalized values
    >>> df = normalized_table.to_pandas()
    >>> print(f"Normalized mass: {df['mass'].iloc[0]:.6f}")  # Now in mg/mg units
    """
    # Extract metadata from table schema
    if not table.schema.metadata:
        raise ValueError(
            "Table metadata is missing - cannot retrieve initial sample mass"
        )

    metadata_bytes = table.schema.metadata.get(b"file_metadata")
    if not metadata_bytes:
        raise ValueError("No file_metadata found in table schema")

    try:
        metadata = json.loads(metadata_bytes.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Failed to parse table metadata: {e}") from e

    # Get initial sample mass from metadata
    sample_mass = metadata.get("sample_mass")
    if sample_mass is None:
        raise ValueError("sample_mass not found in metadata")

    if not isinstance(sample_mass, (int, float)) or sample_mass <= 0:
        raise ValueError(
            f"Invalid sample_mass value: {sample_mass} (must be positive number)"
        )

    # Determine columns to normalize
    column_names = table.column_names
    if columns is None:
        # Default to mass and DSC columns if they exist
        default_columns = ["mass", "dsc_signal"]
        columns = [col for col in default_columns if col in column_names]
        if not columns:
            raise ValueError(
                f"No default normalization columns found. Available: {column_names}"
            )
    else:
        # Check that specified columns exist
        missing_columns = [col for col in columns if col not in column_names]
        if missing_columns:
            raise KeyError(f"Columns not found in table: {missing_columns}")

    # Convert to DataFrame for easier manipulation
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Failed to convert PyArrow table to Polars DataFrame")

    # Normalize specified columns in place
    normalization_exprs = []
    for col in columns:
        # Check if column is numeric
        if not df[col].dtype.is_numeric():
            raise ValueError(f"Column '{col}' is not numeric and cannot be normalized")
        # Update the column in place by dividing by sample mass
        normalization_exprs.append((pl.col(col) / sample_mass).alias(col))

    # Apply normalizations (updates existing columns)
    df = df.with_columns(normalization_exprs)

    # Convert back to PyArrow table while preserving all metadata
    new_table = df.to_arrow()
    if table.schema.metadata:
        new_table = new_table.replace_schema_metadata(table.schema.metadata)

    # Preserve column-level metadata for all existing columns
    from ..util import get_column_metadata, set_column_metadata

    for col in table.column_names:
        if col in new_table.column_names:  # Column exists in new table
            original_metadata = get_column_metadata(table, col)
            if original_metadata:  # If original column had metadata
                new_table = set_column_metadata(
                    new_table, col, original_metadata, replace=True
                )

    # Update metadata for normalized columns
    for col in columns:
        # Get original column metadata
        original_metadata = get_column_metadata(table, col) or {}

        # Update units to show per-mass normalization
        original_units = original_metadata.get("units", "unknown")
        updated_units = f"{original_units}/mg"

        # Update metadata
        updated_metadata = {
            **original_metadata,  # Preserve all original metadata
            "units": updated_units,  # Update units
            "processing_history": [
                *original_metadata.get("processing_history", []),
                "normalized",
            ],  # Add processing step
        }

        new_table = set_column_metadata(new_table, col, updated_metadata, replace=True)

    return new_table


def apply_dsc_calibration(
    table: pa.Table,
    temperature_column: str = "sample_temperature",
    dsc_column: str = "dsc_signal",
) -> pa.Table:
    """
    Apply DSC calibration to convert µV to mW using calibration constants from metadata.

    This function applies the DSC calibration formula:
    y = (P2 + P3*z + P4*z^2 + P5*z^3)*exp(-z^2)
    where z = (T-P0)/P1

    The calibration factor y represents sensitivity in µV/mW.
    The calibrated signal is dsc_signal_µV / y to get power in mW.

    The function updates the DSC column in place, changing units from µV to mW
    and adding "calibration_applied" to the processing history. A "calibration_applied"
    flag is also added to the column metadata to track calibration status.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing thermal analysis data with embedded metadata
        containing calibration constants
    temperature_column : str, default "sample_temperature"
        Name of the temperature column to use for calibration (in °C)
    dsc_column : str, default "dsc_signal"
        Name of the DSC signal column to calibrate (in µV)

    Returns
    -------
    pa.Table
        Table with calibrated DSC column in mW, updated units, and calibration
        status added to metadata

    Raises
    ------
    ValueError
        If required columns are missing, calibration constants not found in metadata,
        or if calibration has already been applied
    KeyError
        If required calibration constants (P0-P5) are missing

    Examples
    --------
    >>> from pyngb import read_ngb
    >>> from pyngb.api.analysis import apply_dsc_calibration
    >>> from pyngb.api.metadata import get_column_units
    >>>
    >>> # Load data with metadata
    >>> table = read_ngb("sample.ngb-ss3")
    >>> print(f"Before: {get_column_units(table, 'dsc_signal')}")  # "µV"
    >>>
    >>> # Apply DSC calibration
    >>> calibrated_table = apply_dsc_calibration(table)
    >>> print(f"After: {get_column_units(calibrated_table, 'dsc_signal')}")  # "mW"
    >>>
    >>> # Check calibrated values
    >>> df = calibrated_table.to_pandas()
    >>> print(f"Calibrated DSC: {df['dsc_signal'].iloc[100]:.6f} mW")
    """
    # Check required columns
    column_names = table.column_names
    if temperature_column not in column_names:
        raise ValueError(f"Table must contain '{temperature_column}' column")
    if dsc_column not in column_names:
        raise ValueError(f"Table must contain '{dsc_column}' column")

    # Check if calibration has already been applied
    from ..util import get_column_metadata

    dsc_metadata = get_column_metadata(table, dsc_column)
    if dsc_metadata and dsc_metadata.get("calibration_applied", False):
        raise ValueError(
            f"Calibration has already been applied to column '{dsc_column}'"
        )

    # Extract metadata from table schema
    if not table.schema.metadata:
        raise ValueError(
            "Table metadata is missing - cannot retrieve calibration constants"
        )

    metadata_bytes = table.schema.metadata.get(b"file_metadata")
    if not metadata_bytes:
        raise ValueError("No file_metadata found in table schema")

    try:
        metadata = json.loads(metadata_bytes.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Failed to parse table metadata: {e}") from e

    # Get calibration constants from metadata
    calibration_constants = metadata.get("calibration_constants")
    if calibration_constants is None:
        raise ValueError("calibration_constants not found in metadata")

    # Check for required calibration parameters P0-P5
    required_params = ["p0", "p1", "p2", "p3", "p4", "p5"]
    missing_params = [p for p in required_params if p not in calibration_constants]
    if missing_params:
        raise KeyError(f"Missing calibration constants: {missing_params}")

    # Extract calibration constants
    P0 = calibration_constants["p0"]
    P1 = calibration_constants["p1"]
    P2 = calibration_constants["p2"]
    P3 = calibration_constants["p3"]
    P4 = calibration_constants["p4"]
    P5 = calibration_constants["p5"]

    # Convert to DataFrame for easier manipulation
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Failed to convert PyArrow table to Polars DataFrame")

    # Get temperature and DSC signal arrays
    temperature = df.get_column(temperature_column).to_numpy()
    dsc_signal = df.get_column(dsc_column).to_numpy()

    # Calculate calibration factor using the formula
    # z = (T - P0) / P1
    z = (temperature - P0) / P1

    # y = (P2 + P3*z + P4*z^2 + P5*z^3) * exp(-z^2)
    y = (P2 + P3 * z + P4 * z**2 + P5 * z**3) * np.exp(-(z**2))

    # Apply calibration: calibrated_signal = dsc_signal_uV / y (converts µV to mW)
    # where y is sensitivity in µV/mW, so µV ÷ (µV/mW) = mW
    calibrated_dsc = dsc_signal / y

    # Update DSC column with calibrated values
    df = df.with_columns(pl.Series(dsc_column, calibrated_dsc))

    # Convert back to PyArrow table while preserving all metadata
    new_table = df.to_arrow()
    if table.schema.metadata:
        new_table = new_table.replace_schema_metadata(table.schema.metadata)

    # Preserve column-level metadata for all existing columns
    from ..util import get_column_metadata, set_column_metadata

    for col in table.column_names:
        if col in new_table.column_names:  # Column exists in new table
            original_metadata = get_column_metadata(table, col)
            if original_metadata:  # If original column had metadata
                new_table = set_column_metadata(
                    new_table, col, original_metadata, replace=True
                )

    # Update metadata for the calibrated DSC column
    original_dsc_metadata = get_column_metadata(table, dsc_column) or {}

    # Determine appropriate units based on current state
    current_units = original_dsc_metadata.get("units", "µV")

    # If already normalized (units contain "/mg"), preserve the normalization
    new_units = "mW/mg" if "/mg" in current_units else "mW"

    # Update units and add processing steps
    updated_dsc_metadata = {
        **original_dsc_metadata,  # Preserve all original metadata
        "units": new_units,  # Update units appropriately
        "processing_history": [
            *original_dsc_metadata.get("processing_history", []),
            "calibration_applied",
        ],  # Add processing step
        "calibration_applied": True,  # Add calibration flag
    }

    new_table = set_column_metadata(
        new_table, dsc_column, updated_dsc_metadata, replace=True
    )

    return new_table
