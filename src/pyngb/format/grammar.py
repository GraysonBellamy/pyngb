"""Byte-level truth for the NGB record grammar, and the strict tokenizer.

Every metadata field in every NGB stream section follows one uniform record
grammar (verified against all six test fixtures, 25k+ records, during the
2026-07 format investigation)::

    record := 18 fc ff ff 03 80 01            (RECORD_HEADER)
              <field_id u16>
              00 00 01 00 00 00               (FIELD_BRIDGE)
              0c 00                           (FIELD_KIND, always 0x000C)
              17 fc ff ff                     (TYPE_PREFIX)
              <dtype u8>
              (80 01 <scalar> | a0 01 <count u32> <count x itemsize bytes>)
              01 00 00 00 02 00 01 00 00      (END_FIELD)

Array counts are ELEMENT counts; payload bytes = count * ITEM_SIZE[dtype].

The tokenizer is a strict linear walk that is *total*: every byte of a
section is either part of a decoded :class:`FieldToken` or covered by an
explicit :class:`UnknownSpan` — nothing is silently skipped. The enumerable
non-record forms observed in real files are classified spans:

- ``prologue``: the 64-byte section preamble (starts ``02 00 00 80``).
- ``preamble``: a record variant with mode bytes ``00 01`` in the header
  position (``18 fc ff ff 03 00 01 ...``), roughly one per table; not yet
  semantically decoded.
- ``table_trailer``: the 3-byte ``00 03 00`` sequence closing each table.
- ``bare_record``: a table trailer followed by a fixed-size scalar record
  that carries NO END_FIELD terminator (observed only in the 2022-vintage
  fixture, field ids 0x0FDE/0x1165).
- ``malformed``: anything else — grammar violation, resynced past.
- ``truncated``: an array whose declared extent overruns the section; the
  walk stops there because a broken length forfeits resync trust.

Severity policy deliberately does NOT live here: the tokenizer never raises
on corruption (consumers decide whether a malformed/truncated span is fatal).
The only exception raised is :class:`NGBResourceLimitError`, for a
fully-validated array record whose declared payload exceeds
``max_array_size_mb`` — checked before the payload is ever decoded.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from enum import IntEnum
from typing import Final, Literal, NamedTuple

import numpy as np
import numpy.typing as npt

from ..config import ParsingConfig
from ..exceptions import NGBResourceLimitError

__all__ = [
    "END_FIELD",
    "FIELD_BRIDGE",
    "FIELD_KIND",
    "ITEM_SIZE",
    "RECORD_ANCHOR",
    "RECORD_HEADER",
    "STRING_BOM",
    "TABLE_OPEN_TAG",
    "TABLE_TRAILER",
    "TYPE_PREFIX",
    "DType",
    "FieldToken",
    "Mode",
    "SpanKind",
    "UnknownSpan",
    "decode_array",
    "decode_scalar",
    "decode_string",
    "ref_class_name",
    "ref_type_ref",
    "tokenize",
]

# -- Grammar constants -------------------------------------------------------

RECORD_ANCHOR: Final = b"\x18\xfc\xff\xff"
RECORD_HEADER: Final = RECORD_ANCHOR + b"\x03\x80\x01"
FIELD_BRIDGE: Final = b"\x00\x00\x01\x00\x00\x00"
FIELD_KIND: Final = b"\x0c\x00"  # u16 0x000C; constant across all records
TYPE_PREFIX: Final = b"\x17\xfc\xff\xff"
END_FIELD: Final = b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
TABLE_TRAILER: Final = b"\x00\x03\x00"
STRING_BOM: Final = b"\xff\xfe\xff"

# REF-scalar payload structure (MFC CArchive-style object serialization).
# A table-open payload ends with TABLE_OPEN_TAG + <type_ref u16> + 00 00 and
# starts with either a class back-reference (01 80) or an inline class
# definition (ff ff <schema u16> <name_len u16> <name>, e.g. "CDbTable").
CLASS_BACKREF: Final = b"\x01\x80"
CLASS_DEF: Final = b"\xff\xff"
TABLE_OPEN_TAG: Final = b"\x02\x00\x00\x80"

# Section prologues start with this object tag.
_PROLOGUE_MAGIC: Final = b"\x02\x00\x00\x80"

_MODE_SCALAR_BYTES: Final = b"\x80\x01"
_MODE_ARRAY_BYTES: Final = b"\xa0\x01"
_MODE_PREAMBLE_BYTES: Final = b"\x00\x01"

# REF payloads are short and structured (10 bytes for a back-reference open,
# ~22 for an inline class definition); the cap bounds the END_FIELD search.
_REF_PAYLOAD_CAP: Final = 256


class Mode(IntEnum):
    """Record payload mode (u16 LE of the two mode bytes)."""

    SCALAR = 0x0180  # 80 01
    ARRAY = 0x01A0  # a0 01
    PREAMBLE = 0x0100  # 00 01 - appears in the header position; never a token


class DType(IntEnum):
    """The nine data types observed across all fixtures and streams."""

    U16 = 0x02
    I32 = 0x03
    F32 = 0x04
    F64 = 0x05
    U8 = 0x10  # scalar u8/bool; arrays are raw byte blobs
    PACKED8 = 0x14  # 8-byte packed record, undecoded
    REF = 0x1A  # class/object reference (table opens, class defs)
    STRING = 0x1F
    HASH16 = 0x48  # 16-byte MD5/GUID-like value


ITEM_SIZE: Final[dict[int, int]] = {
    DType.U16: 2,
    DType.I32: 4,
    DType.F32: 4,
    DType.F64: 8,
    DType.U8: 1,
    DType.PACKED8: 8,
    DType.HASH16: 16,
    # REF and STRING have variable extents (END_FIELD-delimited / own header).
}

SpanKind = Literal[
    "prologue",
    "preamble",
    "table_trailer",
    "bare_record",
    "malformed",
    "truncated",
]


class FieldToken(NamedTuple):
    """One decoded record. ``raw`` is a zero-copy view of the payload bytes
    (for arrays: excluding the 4-byte count header)."""

    start: int
    end: int
    field_id: int
    dtype: DType
    mode: Mode
    raw: memoryview
    element_count: int | None  # arrays only, None for scalars


class UnknownSpan(NamedTuple):
    """A classified run of non-record bytes; see the module docstring."""

    start: int
    end: int
    kind: SpanKind


_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_I32 = struct.Struct("<i")
_F32 = struct.Struct("<f")
_F64 = struct.Struct("<d")
_read_u16 = _U16.unpack_from
_read_u32 = _U32.unpack_from

# Hot-path helpers: the 12 fixed bytes between field_id and dtype, checked in
# one comparison, and a plain-dict DType conversion (IntEnum's __call__ is far
# too slow for a per-record operation).
_FIXED_MIDDLE: Final = FIELD_BRIDGE + FIELD_KIND + TYPE_PREFIX
_DTYPE_OF: Final[dict[int, DType]] = {int(member): member for member in DType}

_NP_DTYPES: Final[dict[int, str]] = {
    DType.U16: "<u2",
    DType.I32: "<i4",
    DType.F32: "<f4",
    DType.F64: "<f8",
}


# -- Value decoders ----------------------------------------------------------


def decode_string(payload: bytes | memoryview) -> str | None:
    """Decode an NGB string payload (including its length header).

    Two formats exist in real files:

    1. NETZSCH proprietary: ``ff fe ff <char_count u8>`` + UTF-16LE data.
    2. Standard: ``<byte_len u32 LE>`` + UTF-8 data (UTF-16LE fallback).

    Returns None when the payload is not decodable. Semantics (null/space
    stripping) are ported verbatim from the legacy parser — the parity
    goldens depend on them.
    """
    data = bytes(payload)
    if len(data) < 4:
        return None
    try:
        if data.startswith(STRING_BOM):
            char_count = data[3]
            expected = 4 + 2 * char_count
            if len(data) >= expected:
                try:
                    decoded = data[4:expected].decode("utf-16le", errors="ignore")
                    decoded = decoded.strip("\x00")
                    if decoded:
                        return decoded
                except UnicodeDecodeError:
                    pass
            return None
        length = _U32.unpack_from(data)[0]
        if 0 < length <= len(data) - 4:
            raw = data[4 : 4 + length]
            # Strict UTF-8 first: UTF-16LE payloads contain invalid UTF-8
            # bytes, so they fall through instead of being mangled.
            try:
                decoded = raw.decode("utf-8").strip().replace("\x00", "")
                if decoded:
                    return decoded
            except UnicodeDecodeError:
                try:
                    decoded = raw.decode("utf-16le").strip("\x00")
                    if decoded:
                        return decoded
                except UnicodeDecodeError:
                    return None
    except struct.error:
        return None
    return None


def decode_scalar(
    dtype: int, payload: bytes | memoryview
) -> int | float | str | bytes | None:
    """Decode a scalar payload. Undecoded dtypes (REF/PACKED8/HASH16) and
    unknown ones return the raw bytes."""
    if dtype == DType.U16:
        return int(_U16.unpack(payload[:2])[0])
    if dtype == DType.I32:
        return int(_I32.unpack(payload[:4])[0])
    if dtype == DType.F32:
        return float(_F32.unpack(payload[:4])[0])
    if dtype == DType.F64:
        return float(_F64.unpack(payload[:8])[0])
    if dtype == DType.U8:
        return payload[0]
    if dtype == DType.STRING:
        return decode_string(payload)
    return bytes(payload)


def decode_array(
    dtype: int, payload: bytes | memoryview
) -> npt.NDArray[np.float64] | bytes:
    """Decode an array payload into float64 values.

    Numeric dtypes decode via ``np.frombuffer``; f32/i32/u16 widen to f64 in
    one vectorized copy (exactly the legacy widening — bitwise parity depends
    on it). Byte-oriented dtypes (u8, packed-8, hash-16) return raw bytes;
    their interpretation is the consumer's business (e.g. the temperature
    calibration coefficients are a dtype-0x10 blob reinterpreted as ``<f4``).
    """
    np_dtype = _NP_DTYPES.get(dtype)
    if np_dtype is None:
        return bytes(payload)
    array = np.frombuffer(payload, dtype=np_dtype)
    if dtype == DType.F64:
        return array
    return array.astype(np.float64)


def ref_type_ref(payload: bytes | memoryview) -> int | None:
    """The table type_ref if this REF payload opens a table, else None.

    Both open forms end with ``02 00 00 80 <type_ref u16> 00 00``: the class
    back-reference form (``01 80`` prefix, 10 bytes) and the inline
    class-definition form (``ff ff`` prefix, first table of each stream).
    """
    data = bytes(payload)
    if len(data) < 10:
        return None
    if not data.startswith((CLASS_BACKREF, CLASS_DEF)):
        return None
    tag = data.rfind(TABLE_OPEN_TAG)
    if tag == -1 or len(data) - tag != 8 or not data.endswith(b"\x00\x00"):
        return None
    return int(_U16.unpack_from(data, tag + 4)[0])


def ref_class_name(payload: bytes | memoryview) -> str | None:
    """The class name if this REF payload carries an inline class definition
    (``ff ff <schema u16> <name_len u16> <name>``), else None."""
    data = bytes(payload)
    if len(data) < 6 or not data.startswith(CLASS_DEF):
        return None
    name_len = _U16.unpack_from(data, 4)[0]
    if len(data) < 6 + name_len:
        return None
    try:
        return data[6 : 6 + name_len].decode("ascii")
    except UnicodeDecodeError:
        return None


# -- Tokenizer ---------------------------------------------------------------


class _Truncated:
    """Sentinel: structurally valid array header whose extent overruns."""


_TRUNCATED: Final = _Truncated()


def _parse_record(
    data: bytes,
    mv: memoryview,
    pos: int,
    end: int,
    max_bytes: int,
) -> tuple[int, FieldToken] | _Truncated | None:
    """Strictly parse one record at ``pos``; None on any grammar violation.

    Returns the truncation sentinel for an array whose declared extent
    overruns ``end``. Raises NGBResourceLimitError for a fully-validated
    array exceeding ``max_bytes`` — before its payload is ever decoded.
    """
    if not data.startswith(RECORD_HEADER, pos) or pos + 24 > end:
        return None
    if not data.startswith(_FIXED_MIDDLE, pos + 9):
        return None
    dtype = data[pos + 21]
    value_start = pos + 24

    if data.startswith(_MODE_SCALAR_BYTES, pos + 22):
        if dtype == 0x1F:  # DType.STRING
            if value_start + 4 > end:
                return None
            if data.startswith(STRING_BOM, value_start):
                payload_len = 4 + 2 * data[value_start + 3]
            else:
                payload_len = 4 + _read_u32(data, value_start)[0]
        elif dtype == 0x1A:  # DType.REF
            search_end = min(value_start + _REF_PAYLOAD_CAP + len(END_FIELD), end)
            field_end = data.find(END_FIELD, value_start, search_end)
            if field_end == -1:
                return None
            payload_len = field_end - value_start
        else:
            item_size = ITEM_SIZE.get(dtype)
            if item_size is None:
                return None
            payload_len = item_size
        payload_end = value_start + payload_len
        if payload_end + 9 > end or not data.startswith(END_FIELD, payload_end):
            return None
        return (
            payload_end + 9,
            FieldToken(
                pos,
                payload_end + 9,
                _read_u16(data, pos + 7)[0],
                _DTYPE_OF[dtype],
                Mode.SCALAR,
                mv[value_start:payload_end],
                None,
            ),
        )

    if data.startswith(_MODE_ARRAY_BYTES, pos + 22):
        item_size = ITEM_SIZE.get(dtype)
        if item_size is None or value_start + 4 > end:
            return None
        count = _read_u32(data, value_start)[0]
        payload_bytes = count * item_size
        payload_end = value_start + 4 + payload_bytes
        if payload_end + 9 > end:
            return _TRUNCATED
        if not data.startswith(END_FIELD, payload_end):
            return None
        if payload_bytes > max_bytes:
            raise NGBResourceLimitError(
                f"Array record at offset {pos} declares {payload_bytes:,} bytes "
                f"({count:,} elements), exceeding the max_array_size_mb limit",
                offset=pos,
                declared=payload_bytes,
                limit=max_bytes,
            )
        return (
            payload_end + 9,
            FieldToken(
                pos,
                payload_end + 9,
                _read_u16(data, pos + 7)[0],
                _DTYPE_OF[dtype],
                Mode.ARRAY,
                mv[value_start + 4 : payload_end],
                count,
            ),
        )

    return None


def _is_bare_record(data: bytes, pos: int, end: int) -> bool:
    """A fixed-size scalar record with no END_FIELD (observed variant)."""
    if not data.startswith(RECORD_HEADER, pos) or pos + 24 > end:
        return False
    if not data.startswith(_FIXED_MIDDLE, pos + 9):
        return False
    if not data.startswith(_MODE_SCALAR_BYTES, pos + 22):
        return False
    item_size = ITEM_SIZE.get(data[pos + 21])
    return item_size is not None and pos + 24 + item_size == end


def _classify_gap(data: bytes, start: int, end: int, saw_record: bool) -> SpanKind:
    """Name a run of non-record bytes; see the module docstring for forms."""
    if not saw_record and data.startswith(_PROLOGUE_MAGIC, start):
        return "prologue"
    if end - start == 3 and data.startswith(TABLE_TRAILER, start):
        return "table_trailer"
    # Preamble records carry one leading 0x00 in observed files.
    anchor_at = start + 1 if data.startswith(b"\x00" + RECORD_ANCHOR, start) else start
    if (
        data.startswith(RECORD_ANCHOR, anchor_at)
        and anchor_at + 7 <= end
        and data.startswith(_MODE_PREAMBLE_BYTES, anchor_at + 5)
    ):
        return "preamble"
    if _is_bare_record(data, start, end):
        return "bare_record"
    if data.startswith(TABLE_TRAILER, start) and _is_bare_record(data, start + 3, end):
        return "bare_record"
    return "malformed"


def tokenize(
    data: bytes,
    *,
    start: int = 0,
    end: int | None = None,
    limits: ParsingConfig | None = None,
) -> Iterator[FieldToken | UnknownSpan]:
    """Strict linear walk over one section of an NGB stream.

    Args:
        data: The full stream blob (offsets in emitted items are absolute
            within it, which keeps diagnostics meaningful).
        start: Section start offset.
        end: Section end offset (exclusive); defaults to ``len(data)``.
        limits: Resource limits; ``max_array_size_mb`` is enforced per array
            record before its payload is decoded.

    Yields:
        FieldToken and UnknownSpan items whose spans are non-overlapping,
        strictly increasing, and cover ``[start, end)`` exactly.

    Raises:
        NGBResourceLimitError: A valid array record declares a payload
            larger than ``max_array_size_mb``.
    """
    if end is None:
        end = len(data)
    max_bytes = (limits or ParsingConfig()).max_array_size_mb * 1024 * 1024
    mv = memoryview(data)
    find = data.find
    parse = _parse_record

    pos = start
    saw_record = False
    pending: tuple[int, FieldToken] | None = None
    while pos < end:
        parsed = pending or parse(data, mv, pos, end, max_bytes)
        pending = None
        if isinstance(parsed, _Truncated):
            yield UnknownSpan(pos, end, "truncated")
            return
        if parsed is not None:
            pos, token = parsed
            saw_record = True
            yield token
            continue

        # Fast path for the by-far-most-common gap: a table trailer sitting
        # directly before the next record (or the section end).
        if (
            saw_record
            and pos + 3 <= end
            and data.startswith(TABLE_TRAILER, pos)
            and (pos + 3 == end or data.startswith(RECORD_ANCHOR, pos + 3))
        ):
            yield UnknownSpan(pos, pos + 3, "table_trailer")
            pos += 3
            continue

        # Grammar violation at pos: resync to the next anchor that parses as
        # a full valid record, then classify the skipped span.
        gap_end = end
        probe_at = find(RECORD_ANCHOR, pos + 1, end)
        while probe_at != -1:
            probe = _parse_record(data, mv, probe_at, end, max_bytes)
            if probe is not None and not isinstance(probe, _Truncated):
                gap_end = probe_at
                pending = probe
                break
            probe_at = find(RECORD_ANCHOR, probe_at + 1, end)
        yield UnknownSpan(pos, gap_end, _classify_gap(data, pos, gap_end, saw_record))
        pos = gap_end
