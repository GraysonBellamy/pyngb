"""Synthetic NGB builder — the tokenizer's dual.

Builds byte-exact NGB structures bottom-up (field records, tables, sections,
streams, whole archives) from the same grammar constants the tokenizer
consumes, so ``tokenize(build(x))`` must reproduce ``x`` exactly. The
builder owns the element-count semantics: array counts are element counts,
never byte counts (except for u8 blobs where they coincide).

The prologue and preamble blobs are verbatim captures from a real fixture
(Red_Oak_STA_10K_250731_R7.ngb-ss3, stream 1); their internal structure is
not yet decoded, so replicating the observed bytes keeps synthetic files
faithful to real ones.
"""

from __future__ import annotations

import struct
import zipfile
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np

from pyngb.format.container import (
    DIRECTORY_OFFSET,
    FORMAT_TAG,
    FORMAT_TAG_OFFSET,
    MAGIC,
    MAGIC_OFFSET,
)
from pyngb.format.grammar import (
    CLASS_BACKREF,
    CLASS_DEF,
    END_FIELD,
    FIELD_BRIDGE,
    FIELD_KIND,
    ITEM_SIZE,
    RECORD_HEADER,
    STRING_BOM,
    TABLE_OPEN_TAG,
    TABLE_TRAILER,
    TYPE_PREFIX,
    DType,
)

__all__ = [
    "PREAMBLE_BLOB",
    "PROLOGUE_BLOB",
    "assert_accounting",
    "build_array",
    "build_scalar",
    "build_section",
    "build_stream",
    "build_table",
    "build_table_open",
    "corrupt_directory",
    "minimal_ngb",
    "write_ngb",
]

# Verbatim from Red_Oak_STA_10K_250731_R7.ngb-ss3 stream_1 (see module doc).
PROLOGUE_BLOB = bytes.fromhex(
    "02000080c30b00000100000002000100000018fcffff03000100000017fcffff"
    "0c0001000100000001000000010018fcffff100001000000020001000000"
    "0300"
)
PREAMBLE_BLOB = bytes.fromhex(
    "0018fcffff03000100000017fcffff0c0001000100000001000000010018fcffff"
    "0a00010000000200010000000300"
)

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")

_SCALAR_PACK = {
    DType.U16: struct.Struct("<H"),
    DType.I32: struct.Struct("<i"),
    DType.F32: struct.Struct("<f"),
    DType.F64: struct.Struct("<d"),
}
_ARRAY_NP = {
    DType.U16: "<u2",
    DType.I32: "<i4",
    DType.F32: "<f4",
    DType.F64: "<f8",
}


def _record(field_id: int, dtype: int, mode: bytes, payload: bytes) -> bytes:
    return (
        RECORD_HEADER
        + _U16.pack(field_id)
        + FIELD_BRIDGE
        + FIELD_KIND
        + TYPE_PREFIX
        + bytes([dtype])
        + mode
        + payload
        + END_FIELD
    )


def encode_string(value: str, form: str = "netzsch") -> bytes:
    """Encode a string payload in one of the observed on-disk forms."""
    if form == "netzsch":
        encoded = value.encode("utf-16le")
        char_count = len(encoded) // 2
        if char_count > 0xFE:
            raise ValueError("netzsch string form holds at most 254 characters")
        return STRING_BOM + bytes([char_count]) + encoded
    if form == "utf8":
        encoded = value.encode("utf-8")
        return _U32.pack(len(encoded)) + encoded
    if form == "utf16":
        encoded = value.encode("utf-16le")
        return _U32.pack(len(encoded)) + encoded
    raise ValueError(f"unknown string form: {form}")


def build_scalar(
    field_id: int,
    dtype: DType,
    value: int | float | str | bytes,
    *,
    string_form: str = "netzsch",
    end_field: bool = True,
) -> bytes:
    """One scalar field record. ``end_field=False`` reproduces the observed
    END_FIELD-less "bare record" variant."""
    if dtype in _SCALAR_PACK:
        assert isinstance(value, (int, float))
        payload = _SCALAR_PACK[dtype].pack(value)
    elif dtype == DType.U8:
        assert isinstance(value, int)
        payload = bytes([value])
    elif dtype == DType.STRING:
        assert isinstance(value, str)
        payload = encode_string(value, string_form)
    elif dtype in (DType.PACKED8, DType.HASH16):
        assert isinstance(value, bytes)
        if len(value) != ITEM_SIZE[dtype]:
            raise ValueError(f"{dtype.name} scalar needs {ITEM_SIZE[dtype]} bytes")
        payload = value
    elif dtype == DType.REF:
        assert isinstance(value, bytes)
        payload = value
    else:  # pragma: no cover - all DType members handled above
        raise ValueError(f"unhandled dtype {dtype}")
    record = _record(field_id, dtype, b"\x80\x01", payload)
    return record if end_field else record[: -len(END_FIELD)]


def build_array(
    field_id: int,
    dtype: DType,
    values: Sequence[float] | bytes | np.ndarray,
) -> bytes:
    """One array field record; the count header is the ELEMENT count."""
    if dtype in _ARRAY_NP:
        payload = np.asarray(values, dtype=_ARRAY_NP[dtype]).tobytes()
        count = len(values)
    elif dtype in (DType.U8, DType.PACKED8, DType.HASH16):
        assert isinstance(values, (bytes, bytearray))
        payload = bytes(values)
        item_size = ITEM_SIZE[dtype]
        if len(payload) % item_size:
            raise ValueError(f"payload not a multiple of {item_size} bytes")
        count = len(payload) // item_size
    else:
        raise ValueError(f"no array form for dtype {dtype}")
    return _record(field_id, dtype, b"\xa0\x01", _U32.pack(count) + payload)


def build_table_open(category: int, type_ref: int, *, class_def: bool = False) -> bytes:
    """The REF record that opens a table. ``class_def=True`` builds the
    inline class-definition form used by the first table of each stream."""
    if class_def:
        prefix = CLASS_DEF + struct.pack("<HH", 1, 8) + b"CDbTable"
    else:
        prefix = CLASS_BACKREF
    payload = prefix + TABLE_OPEN_TAG + _U16.pack(type_ref) + b"\x00\x00"
    return build_scalar(category, DType.REF, payload)


def build_table(
    category: int,
    records: Iterable[bytes],
    *,
    type_ref: int = 0x0BB9,
    class_def: bool = False,
    preamble: bool = False,
    trailer: bool = True,
) -> bytes:
    """A full table: open record, optional preamble blob, field records,
    and the 3-byte trailer."""
    parts = [build_table_open(category, type_ref, class_def=class_def)]
    if preamble:
        parts.append(PREAMBLE_BLOB)
    parts.extend(records)
    if trailer:
        parts.append(TABLE_TRAILER)
    return b"".join(parts)


def build_section(tables: Iterable[bytes], *, prologue: bool = True) -> bytes:
    """Section body: the verbatim prologue blob followed by tables."""
    body = b"".join(tables)
    return (PROLOGUE_BLOB + body) if prologue else body


def build_stream(
    stream_id: int,
    sections: Sequence[tuple[int, bytes]] | None = None,
    *,
    body: bytes | None = None,
    terminator: bool = True,
) -> bytes:
    """A stream blob: container header, section directory, section bodies.

    Either pass explicit ``sections`` as (section_id, body) pairs, or just
    ``body`` for a single main section (id == stream_id). ``terminator``
    appends the 2022-vintage all-zero directory entry.
    """
    if sections is None:
        assert body is not None, "pass sections or body"
        sections = [(stream_id, body)]
    entry_count = len(sections) + (1 if terminator else 0)
    data_start = DIRECTORY_OFFSET + 14 * entry_count

    header = bytearray(data_start)
    header[MAGIC_OFFSET : MAGIC_OFFSET + len(MAGIC)] = MAGIC
    header[FORMAT_TAG_OFFSET : FORMAT_TAG_OFFSET + len(FORMAT_TAG)] = FORMAT_TAG

    pos = DIRECTORY_OFFSET
    offset = data_start
    for section_id, section_body in sections:
        header[pos : pos + 14] = (
            b"\xff\xff"
            + struct.pack("<HII", section_id, offset, len(section_body))
            + b"\x00\x00"
        )
        offset += len(section_body)
        pos += 14
    if terminator:
        header[pos : pos + 14] = b"\xff\xff" + bytes(12)

    return bytes(header) + b"".join(section_body for _, section_body in sections)


def write_ngb(path: Path, streams: dict[int, bytes]) -> Path:
    """Write stream blobs into an NGB ZIP archive at ``path``."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for stream_id, blob in sorted(streams.items()):
            archive.writestr(f"Streams/stream_{stream_id}.table", blob)
    return path


def minimal_ngb(path: Path) -> Path:
    """A structurally complete synthetic NGB file: every dtype, both table
    open forms, preambles, a TOC section, and a data stream. Designed to
    exercise 100% of the tokenizer's record and span forms."""
    stream1_tables = [
        build_table(
            0x1772,
            [
                build_scalar(0x083C, DType.STRING, "Test Project"),
                build_scalar(0x083E, DType.I32, 1_600_000_000),
                build_scalar(0x0834, DType.STRING, "Test Lab", string_form="utf8"),
            ],
            type_ref=0x2AFA,
            class_def=True,
            preamble=True,
        ),
        build_table(
            0x7530,
            [
                build_scalar(0x0840, DType.STRING, "Sample A"),
                build_scalar(0x0C9E, DType.F64, 5.25),
                build_scalar(0x0999, DType.F32, 1.5),
                build_scalar(0x0998, DType.U16, 42),
                build_scalar(0x0997, DType.U8, 1),
                build_scalar(0x0996, DType.PACKED8, bytes(range(8))),
                build_scalar(0x0995, DType.HASH16, bytes(range(16))),
                build_array(0x04BE, DType.U8, np.array([1.0, 2.0], "<f4").tobytes()),
                build_array(0x0994, DType.I32, [1, -2, 3]),
            ],
            type_ref=0x2B0C,
            preamble=True,
        ),
    ]
    toc_table = build_table(
        0x0323, [build_scalar(0x0001, DType.U16, 1)], type_ref=0x0BC6, class_def=True
    )
    stream2_tables = [
        build_table(
            0x178C,
            [build_scalar(0x0FDD, DType.U16, 3)],
            type_ref=0x2B22,
            class_def=True,
            preamble=True,
        ),
        build_table(
            0x7530,
            [build_array(0x0F40, DType.F64, [0.0, 0.5, 1.0])],
            type_ref=0x2B23,
            preamble=True,
        ),
        build_table(
            0x7531,
            [build_array(0x0F40, DType.F64, [1.5, 2.0, 2.5])],
            type_ref=0x2B23,
            preamble=True,
        ),
    ]
    return write_ngb(
        path,
        {
            1: build_stream(1, body=build_section(stream1_tables)),
            2: build_stream(
                2,
                [
                    (2, build_section(stream2_tables)),
                    (1, build_section([toc_table])),
                ],
            ),
        },
    )


def corrupt_directory(stream_blob: bytes, mode: str) -> bytes:
    """Corrupt a built stream's section directory.

    Modes: ``prefix`` clears the first entry's ff-ff prefix (empty
    directory), ``size`` inflates the first section's declared size
    (contiguity / EOF break), ``truncate`` drops the blob's last 8 bytes
    (last section no longer ends at EOF).
    """
    blob = bytearray(stream_blob)
    if mode == "prefix":
        blob[DIRECTORY_OFFSET : DIRECTORY_OFFSET + 2] = b"\x00\x00"
    elif mode == "size":
        section_id, offset, size = struct.unpack_from(
            "<HII", blob, DIRECTORY_OFFSET + 2
        )
        struct.pack_into(
            "<HII", blob, DIRECTORY_OFFSET + 2, section_id, offset, size + 4
        )
    elif mode == "truncate":
        del blob[-8:]
    else:
        raise ValueError(f"unknown corruption mode: {mode}")
    return bytes(blob)


def assert_accounting(items: Sequence, start: int, end: int) -> None:
    """Assert the tokenizer's coverage invariant: spans are contiguous,
    non-overlapping, and cover [start, end) exactly."""
    pos = start
    for item in items:
        assert item.start == pos, f"gap or overlap at {pos}: {item}"
        assert item.end > item.start, f"empty span: {item}"
        pos = item.end
    assert pos == end, f"coverage ends at {pos}, expected {end}"
