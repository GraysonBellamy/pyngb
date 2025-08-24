"""
High-level API functions for thermal analysis calculations.

This module provides convenient functions for performing DTG analysis
and mass normalization on PyArrow tables.
"""

from __future__ import annotations

import json
import numpy as np
import polars as pl
import pyarrow as pa

from ..analysis import dtg

__all__ = [
    "add_dtg",
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

    # Convert back to PyArrow table while preserving metadata
    new_table = df.to_arrow()
    if table.schema.metadata:
        new_table = new_table.replace_schema_metadata(table.schema.metadata)

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
    New columns with '_normalized' suffix are created, preserving the original data.
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
        New table with additional normalized columns (suffixed with '_normalized')
        and preserved metadata

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
    >>>
    >>> # Load data with metadata
    >>> metadata, table = read_ngb("sample.ngb-ss3")
    >>>
    >>> # Normalize mass and DSC to initial sample mass
    >>> normalized_table = normalize_to_initial_mass(table)
    >>>
    >>> # Normalize only specific columns
    >>> normalized_table = normalize_to_initial_mass(table, columns=['mass'])
    >>>
    >>> # Check normalized values (original columns preserved)
    >>> df = normalized_table.to_pandas()
    >>> print(f"Original mass: {df['mass'].iloc[0]:.3f}")
    >>> print(f"Normalized mass: {df['mass_normalized'].iloc[0]:.3f}")
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

    # Normalize specified columns
    normalization_exprs = []
    for col in columns:
        # Check if column is numeric
        if not df[col].dtype.is_numeric():
            raise ValueError(f"Column '{col}' is not numeric and cannot be normalized")
        normalization_exprs.append(
            (pl.col(col) / sample_mass).alias(f"{col}_normalized")
        )

    # Apply normalizations
    df = df.with_columns(normalization_exprs)

    # Convert back to PyArrow table while preserving all metadata
    new_table = df.to_arrow()
    if table.schema.metadata:
        new_table = new_table.replace_schema_metadata(table.schema.metadata)

    return new_table
