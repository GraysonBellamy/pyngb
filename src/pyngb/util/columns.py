"""Column metadata helper functions for PyArrow tables."""

import json
import logging
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)


def set_column_metadata(
    table: pa.Table, column: str, metadata: dict[str, Any], replace: bool = False
) -> pa.Table:
    """Set metadata for a specific column.

    Args:
        table: PyArrow table
        column: Column name to set metadata for
        metadata: Dictionary of metadata to set
        replace: If True, replace all metadata. If False, merge with existing metadata.

    Returns:
        New table with updated column metadata

    Raises:
        ValueError: If column doesn't exist in table
    """
    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    if replace:
        # For replacement, we need to create a new field with only the new metadata
        # First get all fields
        fields = []
        for col_name in table.schema.names:
            if col_name == column:
                # Create field with only the new metadata (encoded properly)
                encoded_metadata = {}
                for k, v in metadata.items():
                    if isinstance(v, bytes):
                        encoded_metadata[k.encode() if isinstance(k, str) else k] = v
                    elif isinstance(v, str):
                        encoded_metadata[k.encode() if isinstance(k, str) else k] = (
                            v.encode("utf-8")
                        )
                    else:
                        encoded_metadata[k.encode() if isinstance(k, str) else k] = (
                            json.dumps(v).encode("utf-8")
                        )

                field = table.field(col_name).with_metadata(encoded_metadata)
                fields.append(field)
            else:
                fields.append(table.field(col_name))

        # Create new schema and cast table
        schema = pa.schema(fields, metadata=table.schema.metadata)
        return table.cast(schema)
    # For merging, manually merge and then replace
    existing = get_column_metadata(table, column) or {}
    merged_metadata = {**existing, **metadata}
    return set_column_metadata(table, column, merged_metadata, replace=True)


def get_column_metadata(table: pa.Table, column: str, key: str | None = None) -> Any:
    """Get metadata for a specific column.

    Args:
        table: PyArrow table
        column: Column name to get metadata for
        key: Specific metadata key to retrieve (if None, returns all metadata)

    Returns:
        Column metadata dict (if key=None) or specific value (if key provided)
        Returns None if metadata or key not found

    Raises:
        ValueError: If column doesn't exist in table
    """
    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    field_metadata = table.field(column).metadata
    if not field_metadata:
        return {} if key is None else None

    # Decode metadata from bytes
    metadata = {}
    for k, v in field_metadata.items():
        try:
            key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)

            if isinstance(v, bytes):
                try:
                    # Try to parse as JSON first
                    metadata[key_str] = json.loads(v.decode("utf-8"))
                except json.JSONDecodeError:
                    # Fall back to string
                    metadata[key_str] = v.decode("utf-8")
            else:
                metadata[key_str] = v
        except (UnicodeDecodeError, AttributeError):
            logger.warning(f"Could not decode metadata key/value for column {column}")
            continue

    return metadata.get(key) if key is not None else metadata


def update_column_metadata(
    table: pa.Table, column: str, updates: dict[str, Any]
) -> pa.Table:
    """Update specific fields in column metadata without overwriting all metadata.

    Args:
        table: PyArrow table
        column: Column name to update metadata for
        updates: Dictionary of metadata updates to apply

    Returns:
        New table with updated column metadata

    Raises:
        ValueError: If column doesn't exist in table
    """
    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    # Use set_column_metadata with merge behavior (default)
    return set_column_metadata(table, column, updates, replace=False)


def add_processing_step(table: pa.Table, column: str, step: str) -> pa.Table:
    """Add a processing step to a column's processing history.

    Args:
        table: PyArrow table
        column: Column name to update
        step: Processing step to add (e.g., "smoothed", "normalized")

    Returns:
        New table with updated processing history

    Raises:
        ValueError: If column doesn't exist in table
    """
    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    # Get current processing history
    current_metadata = get_column_metadata(table, column) or {}
    processing_history = current_metadata.get("processing_history", [])

    # Add new step if not already present
    if step not in processing_history:
        updated_history = [*processing_history, step]
        return update_column_metadata(
            table, column, {"processing_history": updated_history}
        )

    # Return table unchanged if step already exists
    return table


def get_baseline_status(table: pa.Table, column: str) -> bool | None:
    """Get baseline correction status for a column.

    Args:
        table: PyArrow table
        column: Column name to check

    Returns:
        True if baseline corrected, False if not, None if not applicable

    Raises:
        ValueError: If column doesn't exist in table
    """
    from ..constants import FIELD_APPLICABILITY

    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    # Check if baseline correction applies to this column type
    if column not in FIELD_APPLICABILITY["baseline_subtracted"]:
        return None

    metadata = get_column_metadata(table, column)
    return metadata.get("baseline_subtracted") if metadata else None


def is_baseline_correctable(column_name: str) -> bool:
    """Check if a column type supports baseline correction.

    Args:
        column_name: Name of the column to check

    Returns:
        True if column supports baseline correction
    """
    from ..constants import FIELD_APPLICABILITY

    return column_name in FIELD_APPLICABILITY["baseline_subtracted"]


def set_default_column_metadata(table: pa.Table, column: str) -> pa.Table:
    """Set default metadata for a column based on its name.

    Args:
        table: PyArrow table
        column: Column name to set default metadata for

    Returns:
        New table with default metadata set for the column

    Raises:
        ValueError: If column doesn't exist in table
    """
    from ..constants import DEFAULT_COLUMN_METADATA

    if column not in table.column_names:
        raise ValueError(f"Column '{column}' not found in table")

    # Get default metadata for this column type
    default_metadata = DEFAULT_COLUMN_METADATA.get(
        column, {"units": "unknown", "processing_history": ["raw"], "source": "unknown"}
    )

    # Ensure we have a proper dict type
    if not isinstance(default_metadata, dict):
        default_metadata = {
            "units": "unknown",
            "processing_history": ["raw"],
            "source": "unknown",
        }

    return set_column_metadata(table, column, default_metadata, replace=True)


def initialize_table_column_metadata(table: pa.Table) -> pa.Table:
    """Initialize default metadata for all columns in a table.

    Args:
        table: PyArrow table to initialize metadata for

    Returns:
        New table with default metadata set for all columns
    """
    result_table = table

    for column in table.column_names:
        # Only set metadata if column doesn't already have metadata
        existing_metadata = get_column_metadata(result_table, column)
        if not existing_metadata:
            result_table = set_default_column_metadata(result_table, column)

    return result_table
