"""PyArrow table and column metadata utilities."""

from typing import Any

import pyarrow as pa

from .columns import _encode_metadata


def set_metadata(
    tbl: pa.Table,
    col_meta: dict[str, Any] | None = None,
    tbl_meta: dict[str, Any] | None = None,
) -> pa.Table:
    """Store table- and column-level metadata as json-encoded byte strings.

    Table-level metadata is stored in the table's schema.
    Column-level metadata is stored in the table columns' fields. New values
    are merged into any existing metadata; columns absent from the table are
    ignored. The schema is updated in place via cast, which copies no data.

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
    schema = tbl.schema
    for col, meta in (col_meta or {}).items():
        i = schema.get_field_index(col)
        if i == -1:
            continue
        merged = {**(schema.field(i).metadata or {}), **_encode_metadata(meta)}
        schema = schema.set(i, schema.field(i).with_metadata(merged))
    if tbl_meta:
        merged = {**(schema.metadata or {}), **_encode_metadata(tbl_meta)}
        schema = schema.with_metadata(merged)
    if schema is tbl.schema:
        return tbl
    return tbl.cast(schema)
