"""
Public API functions for loading and analyzing NGB data.
"""

from .analysis import add_dtg, calculate_table_dtg, normalize_to_initial_mass
from .loaders import main, read_ngb
from .metadata import (
    add_column_processing_step,
    get_column_baseline_status,
    get_column_source,
    get_column_units,
    get_processing_history,
    inspect_column_metadata,
    is_column_baseline_correctable,
    mark_baseline_corrected,
    set_column_source,
    set_column_units,
)

__all__ = [
    # Metadata functions
    "add_column_processing_step",
    "add_dtg",
    "calculate_table_dtg",
    "get_column_baseline_status",
    "get_column_source",
    "get_column_units",
    "get_processing_history",
    "inspect_column_metadata",
    "is_column_baseline_correctable",
    "main",
    "mark_baseline_corrected",
    "normalize_to_initial_mass",
    "read_ngb",
    "set_column_source",
    "set_column_units",
]
