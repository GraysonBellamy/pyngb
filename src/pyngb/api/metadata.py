"""
High-level API functions for column metadata management.

This module provides convenient functions for setting, getting, and managing
column-level metadata in PyArrow tables for thermal analysis data.
"""

from __future__ import annotations

from typing import Any, Union, Optional

import pyarrow as pa

from ..util import (
    add_processing_step,
    get_baseline_status,
    get_column_metadata,
    is_baseline_correctable,
    update_column_metadata,
)

__all__ = [
    "add_column_processing_step",
    "get_column_baseline_status",
    "get_column_source",
    "get_column_units",
    "get_processing_history",
    "inspect_column_metadata",
    "is_column_baseline_correctable",
    "mark_baseline_corrected",
    "set_column_source",
    "set_column_units",
]


def set_column_units(table: pa.Table, column: str, units: str) -> pa.Table:
    """Set the units for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to set units for
    units : str
        Physical units (e.g., "mg", "°C", "mW", "mg/min")

    Returns
    -------
    pa.Table
        New table with updated column units

    Raises
    ------
    ValueError
        If column doesn't exist in table

    Examples
    --------
    >>> table = set_column_units(table, "mass", "g")  # Change from mg to g
    >>> units = get_column_units(table, "mass")
    >>> print(units)  # "g"
    """
    return update_column_metadata(table, column, {"units": units})


def get_column_units(table: pa.Table, column: str) -> Optional[str]:  # noqa: UP045
    """Get the units for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to get units for

    Returns
    -------
    str or None
        Units string, or None if not set

    Raises
    ------
    ValueError
        If column doesn't exist in table

    Examples
    --------
    >>> units = get_column_units(table, "mass")
    >>> print(f"Mass is measured in {units}")  # "Mass is measured in mg"
    """
    result = get_column_metadata(table, column, "units")
    return result if isinstance(result, str) else None


def mark_baseline_corrected(
    table: pa.Table, columns: Union[list[str], str]
) -> pa.Table:
    """Mark columns as baseline corrected.

    Only applies to columns that support baseline correction (mass, dsc_signal).
    Other columns are silently ignored.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the columns
    columns : str or list of str
        Column name(s) to mark as baseline corrected

    Returns
    -------
    pa.Table
        New table with baseline_subtracted=True for applicable columns

    Examples
    --------
    >>> # Mark mass and DSC as baseline corrected
    >>> table = mark_baseline_corrected(table, ["mass", "dsc_signal"])
    >>>
    >>> # Check status
    >>> status = get_column_baseline_status(table, "mass")
    >>> print(status)  # True
    """
    if isinstance(columns, str):
        columns = [columns]

    result_table = table
    for column in columns:
        if is_baseline_correctable(column):
            result_table = update_column_metadata(
                result_table, column, {"baseline_subtracted": True}
            )
            # Also add to processing history
            result_table = add_processing_step(
                result_table, column, "baseline_corrected"
            )

    return result_table


def get_processing_history(table: pa.Table, column: str) -> list[str]:
    """Get the processing history for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to get processing history for

    Returns
    -------
    list of str
        List of processing steps applied to the column

    Raises
    ------
    ValueError
        If column doesn't exist in table

    Examples
    --------
    >>> history = get_processing_history(table, "dtg")
    >>> print(history)  # ["calculated"]
    >>>
    >>> # After smoothing
    >>> table = add_column_processing_step(table, "dtg", "smoothed")
    >>> history = get_processing_history(table, "dtg")
    >>> print(history)  # ["calculated", "smoothed"]
    """
    history = get_column_metadata(table, column, "processing_history")
    return history if history is not None else []


def add_column_processing_step(table: pa.Table, column: str, step: str) -> pa.Table:
    """Add a processing step to a column's history.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to add processing step to
    step : str
        Processing step to add (e.g., "smoothed", "filtered", "calibrated")

    Returns
    -------
    pa.Table
        New table with updated processing history

    Raises
    ------
    ValueError
        If column doesn't exist in table

    Examples
    --------
    >>> table = add_column_processing_step(table, "mass", "smoothed")
    >>> history = get_processing_history(table, "mass")
    >>> print(history)  # ["raw", "smoothed"]
    """
    return add_processing_step(table, column, step)


def get_column_source(table: pa.Table, column: str) -> Optional[str]:  # noqa: UP045
    """Get the source type for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to get source for

    Returns
    -------
    str or None
        Source type ("measurement", "calculated", "derived"), or None if not set

    Examples
    --------
    >>> source = get_column_source(table, "time")
    >>> print(source)  # "measurement"
    >>>
    >>> source = get_column_source(table, "dtg")
    >>> print(source)  # "derived"
    """
    result = get_column_metadata(table, column, "source")
    return result if isinstance(result, str) else None


def set_column_source(table: pa.Table, column: str, source: str) -> pa.Table:
    """Set the source type for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to set source for
    source : str
        Source type ("measurement", "calculated", "derived")

    Returns
    -------
    pa.Table
        New table with updated column source

    Examples
    --------
    >>> table = set_column_source(table, "custom_calc", "calculated")
    """
    return update_column_metadata(table, column, {"source": source})


def is_column_baseline_correctable(column_name: str) -> bool:
    """Check if a column supports baseline correction.

    Parameters
    ----------
    column_name : str
        Name of the column to check

    Returns
    -------
    bool
        True if column supports baseline correction

    Examples
    --------
    >>> is_column_baseline_correctable("mass")  # True
    >>> is_column_baseline_correctable("time")  # False
    """
    return is_baseline_correctable(column_name)


def get_column_baseline_status(table: pa.Table, column: str) -> Optional[bool]:  # noqa: UP045
    """Get baseline correction status for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to check

    Returns
    -------
    bool or None
        True if baseline corrected, False if not corrected,
        None if baseline correction doesn't apply

    Examples
    --------
    >>> status = get_column_baseline_status(table, "mass")
    >>> if status is True:
    ...     print("Mass has been baseline corrected")
    >>> elif status is False:
    ...     print("Mass has not been baseline corrected")
    >>> else:
    ...     print("Baseline correction not applicable")
    """
    return get_baseline_status(table, column)


def inspect_column_metadata(table: pa.Table, column: str) -> dict[str, Any]:
    """Get complete metadata information for a column.

    Parameters
    ----------
    table : pa.Table
        PyArrow table containing the column
    column : str
        Name of the column to inspect

    Returns
    -------
    dict
        Complete metadata dictionary for the column

    Raises
    ------
    ValueError
        If column doesn't exist in table

    Examples
    --------
    >>> metadata = inspect_column_metadata(table, "mass")
    >>> print(metadata)
    {
        'units': 'mg',
        'processing_history': ['raw'],
        'source': 'measurement',
        'baseline_subtracted': False
    }
    """
    return get_column_metadata(table, column) or {}
