"""
General utilities for working with Parquet files and PyArrow tables.
"""

import hashlib
import json

import pyarrow as pa


def set_metadata(tbl, col_meta={}, tbl_meta={}) -> pa.Table:
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


def get_hash(path: str) -> str | None:
    """Generate file hash for metadata."""
    try:
        with open(path, "rb") as file:
            return hashlib.blake2b(file.read()).hexdigest()
    except FileNotFoundError:
        print(f"File not found: {path}")
        return None
    except Exception as e:
        print(f"Error occurred while generating file hash: {e}")
        return None
