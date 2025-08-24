"""
High-level API functions for thermal analysis calculations.

This module provides convenient functions for performing DTG analysis
on PyArrow tables with the simplified DTG interface.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pyarrow as pa

from ..analysis import dtg

__all__ = [
    "add_dtg",
    "calculate_table_dtg",
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
