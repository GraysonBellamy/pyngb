"""Strict, lossless parsing layer for the NGB binary format.

Layered bottom-up: :mod:`.container` opens the ZIP archive and validates each
stream's section directory; :mod:`.grammar` holds every byte-level constant
and tokenizes sections into field records and classified unknown spans;
:mod:`.document` assembles tokens into the queryable table model;
:mod:`.maps` writes down all declarative format knowledge; :mod:`.extract`
and :mod:`.channels` produce FileMetadata and the measurement frame from a
document.

This package is the 0.4.0 extraction backbone. The document layer
(:func:`load_document`, :class:`NGBDocument`, :class:`Table`,
:class:`Field`) is re-exported from the top-level ``pyngb`` namespace;
the byte-level grammar surface lives here only.
"""

from .channels import build_dataframe
from .container import SectionEntry, StreamData, open_ngb, parse_container
from .document import Field, NGBDocument, Table, load_document
from .extract import build_metadata
from .grammar import (
    END_FIELD,
    ITEM_SIZE,
    RECORD_ANCHOR,
    RECORD_HEADER,
    TABLE_TRAILER,
    TYPE_PREFIX,
    DType,
    FieldToken,
    Mode,
    SpanKind,
    UnknownSpan,
    decode_array,
    decode_scalar,
    decode_string,
    ref_class_name,
    ref_type_ref,
    tokenize,
)

__all__ = [
    "END_FIELD",
    "ITEM_SIZE",
    "RECORD_ANCHOR",
    "RECORD_HEADER",
    "TABLE_TRAILER",
    "TYPE_PREFIX",
    "DType",
    "Field",
    "FieldToken",
    "Mode",
    "NGBDocument",
    "SectionEntry",
    "SpanKind",
    "StreamData",
    "Table",
    "UnknownSpan",
    "build_dataframe",
    "build_metadata",
    "decode_array",
    "decode_scalar",
    "decode_string",
    "load_document",
    "open_ngb",
    "parse_container",
    "ref_class_name",
    "ref_type_ref",
    "tokenize",
]
