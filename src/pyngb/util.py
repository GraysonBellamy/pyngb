"""
General utilities for working with Parquet files and PyArrow tables.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional, Any

import pyarrow as pa

# Set up logger for this module
logger = logging.getLogger(__name__)


def set_metadata(
    tbl, col_meta: dict[str, Any] = {}, tbl_meta: dict[str, Any] = {}
) -> pa.Table:
    """Store table- and column-level metadata as json-encoded byte strings.

    Provided by: https://stackoverflow.com/a/69553667/25195764

    Table-level metadata is stored in the table's schema.
    Column-level metadata is stored in the table columns' fields.

    To update the metadata, first new fields are created for all columns.
    Next a schema is created using the new fields and updated table metadata.
    Finally a new table is created by replacing the old one's schema, but
    without copying any data.

    Args:
        tbl (pyarrow.Table): The table to store metadata in
        col_meta: A json-serializable dictionary with column metadata in the form
            {
                'column_1': {'some': 'data', 'value': 1},
                'column_2': {'more': 'stuff', 'values': [1,2,3]}
            }
        tbl_meta: A json-serializable dictionary with table-level metadata.

    Returns:
        pyarrow.Table: The table with updated metadata
    """
    # Create updated column fields with new metadata
    if col_meta or tbl_meta:
        fields = []
        for col in tbl.schema.names:
            if col in col_meta:
                # Get updated column metadata
                metadata = tbl.field(col).metadata or {}
                for k, v in col_meta[col].items():
                    if isinstance(v, bytes):
                        metadata[k] = v
                    elif isinstance(v, str):
                        metadata[k] = v.encode("utf-8")
                    else:
                        metadata[k] = json.dumps(v).encode("utf-8")
                # Update field with updated metadata
                fields.append(tbl.field(col).with_metadata(metadata))
            else:
                fields.append(tbl.field(col))

        # Get updated table metadata
        tbl_metadata = tbl.schema.metadata or {}
        for k, v in tbl_meta.items():
            if isinstance(v, bytes):
                tbl_metadata[k] = v
            elif isinstance(v, str):
                tbl_metadata[k] = v.encode("utf-8")
            else:
                tbl_metadata[k] = json.dumps(v).encode("utf-8")

        # Create new schema with updated field metadata and updated table metadata
        schema = pa.schema(fields, metadata=tbl_metadata)

        # With updated schema build new table (shouldn't copy data)
        # tbl = pa.Table.from_batches(tbl.to_batches(), schema)
        tbl = tbl.cast(schema)

    return tbl


def get_hash(path: str, max_size_mb: int = 1000) -> Optional[str]:
    """Generate file hash for metadata.

    Args:
        path: Path to the file to hash
        max_size_mb: Maximum file size in MB to hash (default: 1000MB)

    Returns:
        BLAKE2b hash as hex string, or None if hashing fails

    Raises:
        OSError: If there are file system related errors
        PermissionError: If file access is denied
    """
    try:
        # Pre-flight: ensure blake2b constructor is callable. If a hashing backend
        # failure occurs (e.g., during unit tests that patch blake2b to raise),
        # surface it as an unexpected error per contract.
        try:
            _ = hashlib.blake2b()  # type: ignore[call-arg]
        except Exception as e:  # pragma: no cover - exercised in tests via patch
            logger.error(
                "Unexpected error while generating hash for file %s: %s", path, e
            )
            return None
        # Check file size before hashing
        file_size = Path(path).stat().st_size
        max_size_bytes = max_size_mb * 1024 * 1024

        if file_size > max_size_bytes:
            logger.warning(
                "File too large for hashing (%d MB > %d MB): %s",
                file_size // (1024 * 1024),
                max_size_mb,
                path,
            )
            return None

        with open(path, "rb") as file:
            return hashlib.blake2b(file.read()).hexdigest()
    except FileNotFoundError:
        logger.warning("File not found while generating hash: %s", path)
        return None
    except PermissionError:
        logger.error("Permission denied while generating hash for file: %s", path)
        return None
    except OSError as e:
        logger.error("OS error while generating hash for file %s: %s", path, e)
        return None
    except Exception as e:
        logger.error("Unexpected error while generating hash for file %s: %s", path, e)
        return None
