"""Utility functions for working with Parquet files and PyArrow tables.

This module provides utilities for:
- Table and column metadata operations
- File hashing for provenance tracking
- Column metadata helpers for thermal analysis data

All functions are re-exported from submodules for backward compatibility.
"""

# Import from submodules
from .columns import (
    add_processing_step,
    get_baseline_status,
    get_column_metadata,
    initialize_table_column_metadata,
    is_baseline_correctable,
    set_column_metadata,
    set_default_column_metadata,
    update_column_metadata,
)
from .hashing import get_hash
from .metadata import set_metadata

__all__ = [
    # Column metadata
    "add_processing_step",
    "get_baseline_status",
    "get_column_metadata",
    # Hashing
    "get_hash",
    "initialize_table_column_metadata",
    "is_baseline_correctable",
    "set_column_metadata",
    "set_default_column_metadata",
    # General metadata
    "set_metadata",
    "update_column_metadata",
]
