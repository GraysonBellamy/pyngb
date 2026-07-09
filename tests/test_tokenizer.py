"""The strict record-grammar tokenizer, exercised through the builder.

The builder and tokenizer are duals: ``tokenize(build(x))`` must reproduce
``x`` exactly, and every corruption case first asserts the pristine input
parses. The one regression that must never come back: record-anchor bytes
inside an array payload were the regex era's core false-match failure mode —
here they must parse cleanly because END_FIELD is verified at the
count-computed position, never searched for.
"""

import struct
from pathlib import Path

import numpy as np
import pytest

from pyngb.config import ParsingConfig
from pyngb.exceptions import NGBResourceLimitError
from pyngb.format import (
    END_FIELD,
    DType,
    FieldToken,
    Mode,
    UnknownSpan,
    decode_array,
    decode_scalar,
    decode_string,
    open_ngb,
    ref_class_name,
    ref_type_ref,
    tokenize,
)
from support.ngb_builder import (
    PREAMBLE_BLOB,
    PROLOGUE_BLOB,
    assert_accounting,
    build_array,
    build_scalar,
    build_section,
    build_table,
    build_table_open,
    minimal_ngb,
)


def tokens_of(data: bytes, **kwargs) -> list:
    items = list(tokenize(data, **kwargs))
    assert_accounting(items, kwargs.get("start", 0), kwargs.get("end", len(data)))
    return items


def only_token(data: bytes) -> FieldToken:
    items = tokens_of(data)
    assert len(items) == 1
    assert isinstance(items[0], FieldToken)
    return items[0]


def span_kinds(items: list) -> list[str]:
    return [item.kind for item in items if isinstance(item, UnknownSpan)]


class TestScalarRoundTrip:
    @pytest.mark.parametrize(
        ("dtype", "value"),
        [
            (DType.U16, 0),
            (DType.U16, 65535),
            (DType.I32, -2_000_000_000),
            (DType.F32, 1.5),
            (DType.F64, 253.516),
            (DType.U8, 0),
            (DType.U8, 255),
        ],
    )
    def test_numeric(self, dtype: DType, value: float) -> None:
        token = only_token(build_scalar(0x0C9E, dtype, value))
        assert token.field_id == 0x0C9E
        assert token.dtype == dtype
        assert token.mode == Mode.SCALAR
        assert token.element_count is None
        assert decode_scalar(token.dtype, token.raw) == value

    @pytest.mark.parametrize("form", ["netzsch", "utf8", "utf16"])
    def test_string_forms(self, form: str) -> None:
        value = "Müller © STA 449"
        token = only_token(build_scalar(0x0840, DType.STRING, value, string_form=form))
        assert decode_scalar(token.dtype, token.raw) == value

    def test_string_max_netzsch_length(self) -> None:
        value = "x" * 254
        token = only_token(build_scalar(0x0840, DType.STRING, value))
        assert decode_scalar(token.dtype, token.raw) == value

    def test_undecoded_dtypes_return_raw_bytes(self) -> None:
        packed = only_token(build_scalar(0x0996, DType.PACKED8, bytes(range(8))))
        assert decode_scalar(packed.dtype, packed.raw) == bytes(range(8))
        hashed = only_token(build_scalar(0x0995, DType.HASH16, bytes(range(16))))
        assert decode_scalar(hashed.dtype, hashed.raw) == bytes(range(16))

    def test_f32_widening_matches_legacy(self) -> None:
        """f32 scalars widen through the exact same float32 value."""
        token = only_token(build_scalar(0x0999, DType.F32, 0.1))
        assert (
            decode_scalar(token.dtype, token.raw)
            == struct.unpack("<f", struct.pack("<f", 0.1))[0]
        )


class TestArrayRoundTrip:
    def test_f64_bitwise(self) -> None:
        values = [0.0, -1.5, 102.5 * 60, float("inf")]
        token = only_token(build_array(0x0F40, DType.F64, values))
        assert token.mode == Mode.ARRAY
        assert token.element_count == 4
        decoded = decode_array(token.dtype, token.raw)
        assert decoded.tobytes() == np.array(values, "<f8").tobytes()

    def test_count_is_elements_not_bytes(self) -> None:
        """A u16 array of 3 elements declares count=3 over 6 payload bytes."""
        token = only_token(build_array(0x0444, DType.U16, [1, 2, 3]))
        assert token.element_count == 3
        assert len(token.raw) == 6

    def test_u8_array_returns_raw_bytes(self) -> None:
        """dtype-0x10 arrays are byte blobs; consumers reinterpret them
        (e.g. temperature-calibration coefficients as <f4)."""
        blob = np.array([1.0, -2.5, 3.25], "<f4").tobytes()
        token = only_token(build_array(0x04BE, DType.U8, blob))
        assert token.element_count == len(blob)
        decoded = decode_array(token.dtype, token.raw)
        assert isinstance(decoded, bytes)
        assert np.frombuffer(decoded, "<f4").tolist() == [1.0, -2.5, 3.25]

    def test_f32_array_widens_exactly(self) -> None:
        values = [0.1, 2.5e-7, -1e30]
        token = only_token(build_array(0x0F3D, DType.F32, values))
        decoded = decode_array(token.dtype, token.raw)
        expected = np.array(values, "<f4").astype(np.float64)
        assert decoded.tobytes() == expected.tobytes()

    def test_empty_array(self) -> None:
        token = only_token(build_array(0x0F40, DType.F64, []))
        assert token.element_count == 0
        assert len(token.raw) == 0


class TestRefPayloads:
    def test_table_open_backref(self) -> None:
        token = only_token(build_table_open(0x7530, 0x2B23))
        assert token.dtype == DType.REF
        assert ref_type_ref(token.raw) == 0x2B23
        assert ref_class_name(token.raw) is None

    def test_table_open_with_inline_class_def(self) -> None:
        token = only_token(build_table_open(0x1772, 0x2AFA, class_def=True))
        assert ref_type_ref(token.raw) == 0x2AFA
        assert ref_class_name(token.raw) == "CDbTable"

    def test_non_open_ref_payload(self) -> None:
        token = only_token(build_scalar(0x0323, DType.REF, b"\x03\x80\x99\x00"))
        assert ref_type_ref(token.raw) is None


class TestSpanClassification:
    def test_prologue(self) -> None:
        data = build_section([build_table(0x7530, [])])
        items = tokens_of(data)
        assert isinstance(items[0], UnknownSpan)
        assert items[0].kind == "prologue"
        assert data[items[0].start : items[0].end] == PROLOGUE_BLOB

    def test_preamble_and_trailer(self) -> None:
        data = build_table(0x7530, [build_scalar(0x0998, DType.U16, 1)], preamble=True)
        items = tokens_of(data)
        kinds = span_kinds(items)
        assert kinds == ["preamble", "table_trailer"]
        preamble = items[1]
        assert data[preamble.start : preamble.end] == PREAMBLE_BLOB

    def test_bare_record_variant(self) -> None:
        """2022-vintage: trailer followed by a scalar record with no
        END_FIELD, then the next table."""
        data = (
            build_table(0x7530, [build_scalar(0x0998, DType.U16, 1)], trailer=True)
            + build_scalar(0x1165, DType.I32, 64, end_field=False)
            + build_table(0x7531, [])
        )
        items = tokens_of(data)
        assert "bare_record" in span_kinds(items)
        assert "malformed" not in span_kinds(items)

    def test_bare_record_at_end_of_section(self) -> None:
        data = build_table(0x7530, []) + build_scalar(
            0x0FDE, DType.I32, 111, end_field=False
        )
        items = tokens_of(data)
        assert span_kinds(items)[-1] == "bare_record"

    def test_garbage_is_malformed(self) -> None:
        items = tokens_of(b"this is not an NGB stream at all")
        assert span_kinds(items) == ["malformed"]

    def test_empty_input(self) -> None:
        assert tokens_of(b"") == []


class TestCorruption:
    """Each case first proves the uncorrupted bytes parse cleanly."""

    def pristine(self) -> bytes:
        return build_array(0x0F40, DType.F64, [1.0, 2.0, 3.0]) + build_scalar(
            0x0998, DType.U16, 7
        )

    def test_pristine_parses(self) -> None:
        items = tokens_of(self.pristine())
        assert span_kinds(items) == []
        assert len(items) == 2

    def test_truncated_array_stops_the_walk(self) -> None:
        data = self.pristine()[:40]  # cut inside the array payload
        items = tokens_of(data)
        assert len(items) == 1
        assert isinstance(items[0], UnknownSpan)
        assert items[0].kind == "truncated"
        assert items[0].end == len(data)

    def test_corrupt_end_field_resyncs(self) -> None:
        data = bytearray(self.pristine())
        first_end = data.find(END_FIELD)
        data[first_end : first_end + 9] = bytes(9)
        items = tokens_of(bytes(data))
        assert span_kinds(items) == ["malformed"]
        recovered = [t for t in items if isinstance(t, FieldToken)]
        assert len(recovered) == 1
        assert recovered[0].field_id == 0x0998

    def test_unknown_dtype_is_malformed(self) -> None:
        record = bytearray(build_scalar(0x0998, DType.U16, 7))
        record[21] = 0x99  # dtype byte
        items = tokens_of(bytes(record))
        assert span_kinds(items) == ["malformed"]

    def test_corrupt_kind_word_is_malformed(self) -> None:
        record = bytearray(build_scalar(0x0998, DType.U16, 7))
        record[15] = 0x0D  # kind word, always 0x000C in valid records
        items = tokens_of(bytes(record))
        assert span_kinds(items) == ["malformed"]

    def test_unknown_mode_bytes_is_malformed(self) -> None:
        record = bytearray(build_scalar(0x0998, DType.U16, 7))
        record[22] = 0xB0  # neither scalar (80 01) nor array (a0 01)
        items = tokens_of(bytes(record))
        assert span_kinds(items) == ["malformed"]

    def test_string_header_truncated_at_section_end(self) -> None:
        record = build_scalar(0x0840, DType.STRING, "hello")
        items = tokens_of(record[:26])  # cut inside the string length header
        assert span_kinds(items) == ["malformed"]

    def test_inflated_count_is_detected(self) -> None:
        """count+1 displaces the END_FIELD check into foreign bytes."""
        data = bytearray(self.pristine())
        count_at = 24  # first record's count header
        count = struct.unpack_from("<I", data, count_at)[0]
        struct.pack_into("<I", data, count_at, count + 1)
        items = tokens_of(bytes(data))
        assert items
        assert isinstance(items[0], UnknownSpan)
        assert items[0].kind in ("malformed", "truncated")
        assert not any(
            isinstance(t, FieldToken) and t.field_id == 0x0F40 for t in items
        )

    def test_oversized_array_declaration_raises_before_decode(self) -> None:
        big = build_array(0x0F40, DType.F64, np.zeros(300_000))  # 2.4 MB
        with pytest.raises(NGBResourceLimitError) as excinfo:
            list(tokenize(big, limits=ParsingConfig(max_array_size_mb=1)))
        assert excinfo.value.offset == 0
        assert excinfo.value.declared == 2_400_000
        assert excinfo.value.limit == 1024 * 1024

    def test_default_limits_allow_the_same_array(self) -> None:
        big = build_array(0x0F40, DType.F64, np.zeros(300_000))
        token = only_token(big)
        assert token.element_count == 300_000

    def test_record_anchor_inside_array_payload_parses_cleanly(self) -> None:
        """THE regression guard: a full fake record embedded in an f64
        payload must not split the array (the regex era's failure mode)."""
        inner = build_scalar(0x1234, DType.I32, 7)
        padded = inner + bytes(-len(inner) % 8)
        values = np.frombuffer(padded, "<f8")
        data = build_array(0x0F40, DType.F64, values) + build_scalar(
            0x0998, DType.U16, 7
        )
        items = tokens_of(data)
        assert span_kinds(items) == []
        array_token = items[0]
        assert isinstance(array_token, FieldToken)
        assert array_token.element_count == len(values)
        assert bytes(array_token.raw) == padded

    def test_garbage_prefix_then_valid_record(self) -> None:
        data = b"\xde\xad\xbe\xef" * 4 + build_scalar(0x0998, DType.U16, 7)
        items = tokens_of(data)
        assert span_kinds(items) == ["malformed"]
        assert isinstance(items[-1], FieldToken)


class TestBuilderDuality:
    """The conftest workhorse must achieve 100% tokenizer coverage."""

    def test_minimal_ngb_fully_tokenizes(self, tmp_path: Path) -> None:
        path = minimal_ngb(tmp_path / "minimal.ngb-ss3")
        streams = open_ngb(path)
        assert set(streams) == {1, 2}
        for stream in streams.values():
            for entry in stream.sections:
                items = tokens_of(stream.raw, start=entry.offset, end=entry.end)
                kinds = set(span_kinds(items))
                assert "malformed" not in kinds
                assert "truncated" not in kinds

    def test_minimal_ngb_values_survive(self, tmp_path: Path) -> None:
        path = minimal_ngb(tmp_path / "minimal.ngb-ss3")
        stream1 = open_ngb(path, streams=[1])[1]
        entry = stream1.main
        values = {
            token.field_id: decode_scalar(token.dtype, token.raw)
            for token in tokenize(stream1.raw, start=entry.offset, end=entry.end)
            if isinstance(token, FieldToken) and token.mode == Mode.SCALAR
        }
        assert values[0x083C] == "Test Project"
        assert values[0x0C9E] == 5.25
        assert values[0x0998] == 42

    def test_offsets_are_absolute_within_the_blob(self, tmp_path: Path) -> None:
        path = minimal_ngb(tmp_path / "minimal.ngb-ss3")
        stream2 = open_ngb(path, streams=[2])[2]
        entry = stream2.main
        first = next(iter(tokenize(stream2.raw, start=entry.offset, end=entry.end)))
        assert first.start == entry.offset


class TestDecodeStringEdgeCases:
    def test_too_short(self) -> None:
        assert decode_string(b"ab") is None

    def test_zero_length(self) -> None:
        assert decode_string(bytes(4)) is None

    def test_invalid_utf8_falls_back_to_utf16le(self) -> None:
        payload = struct.pack("<I", 4) + "hi".encode("utf-16le")
        assert decode_string(payload) == "hi"

    def test_length_beyond_payload(self) -> None:
        assert decode_string(struct.pack("<I", 100) + b"abc") is None

    def test_neither_utf8_nor_utf16le(self) -> None:
        # 0xFF is invalid UTF-8; an odd byte count is invalid UTF-16LE.
        assert decode_string(struct.pack("<I", 3) + b"\xff\xfe\x00") is None
