"""Property-based tests for the format layer.

Round-trip properties assert bitwise equality (floats included — no
tolerances, ever). Fuzz properties pin the tokenizer's total-coverage
contract: for ANY input it terminates, raises nothing, and accounts for
every byte as either a token or a classified span.
"""

from __future__ import annotations

import zipfile
from functools import lru_cache
from pathlib import Path

import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from pyngb.format import (
    DType,
    FieldToken,
    UnknownSpan,
    decode_array,
    decode_scalar,
    parse_container,
    tokenize,
)
from support.ngb_builder import assert_accounting, build_array, build_scalar

FIXTURE = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"


def exhaust(data: bytes) -> list:
    items = list(tokenize(data))
    assert_accounting(items, 0, len(data))
    return items


def decoded_single(record: bytes):
    items = exhaust(record)
    assert len(items) == 1
    assert isinstance(items[0], FieldToken)
    token = items[0]
    if token.element_count is None:
        return decode_scalar(token.dtype, token.raw)
    return decode_array(token.dtype, token.raw)


class TestScalarRoundTripProperties:
    @given(st.integers(min_value=0, max_value=0xFFFF))
    @settings(max_examples=100, deadline=1000)
    def test_u16(self, value: int) -> None:
        assert decoded_single(build_scalar(0x0998, DType.U16, value)) == value

    @given(st.integers(min_value=-(2**31), max_value=2**31 - 1))
    @settings(max_examples=100, deadline=1000)
    def test_i32(self, value: int) -> None:
        assert decoded_single(build_scalar(0x083E, DType.I32, value)) == value

    @given(st.floats(width=32, allow_nan=False))
    @settings(max_examples=200, deadline=1000)
    def test_f32(self, value: float) -> None:
        assert decoded_single(build_scalar(0x0999, DType.F32, value)) == value

    @given(st.floats(allow_nan=False))
    @settings(max_examples=200, deadline=1000)
    def test_f64(self, value: float) -> None:
        assert decoded_single(build_scalar(0x0C9E, DType.F64, value)) == value

    @given(
        st.text(
            alphabet=st.characters(
                blacklist_categories=("Cs",), blacklist_characters="\x00"
            ),
            min_size=1,
            max_size=120,
        )
    )
    @settings(max_examples=200, deadline=1000)
    def test_string_netzsch_form(self, value: str) -> None:
        # The decoder strips leading/trailing NULs; excluded by the alphabet.
        # Whitespace-only strings decode to falsy and return None by the
        # (parity-pinned) legacy semantics, so require some substance.
        if not value.strip("\x00"):
            return
        record = build_scalar(0x0840, DType.STRING, value)
        assert decoded_single(record) == value.strip("\x00")


class TestArrayRoundTripProperties:
    @given(st.lists(st.floats(allow_nan=False), max_size=64))
    @settings(max_examples=100, deadline=1000)
    def test_f64_values(self, values: list[float]) -> None:
        decoded = decoded_single(build_array(0x0F40, DType.F64, values))
        assert decoded.tobytes() == np.array(values, "<f8").tobytes()

    @given(st.binary(max_size=512).map(lambda b: b[: len(b) - len(b) % 8]))
    @settings(max_examples=100, deadline=1000)
    def test_f64_raw_bit_patterns(self, payload: bytes) -> None:
        """Arbitrary bit patterns (NaNs included) survive untouched."""
        values = np.frombuffer(payload, "<f8")
        items = exhaust(build_array(0x0F40, DType.F64, values))
        (token,) = items
        assert bytes(token.raw) == payload
        assert token.element_count == len(payload) // 8

    @given(st.binary(max_size=256))
    @settings(max_examples=100, deadline=1000)
    def test_u8_blobs(self, payload: bytes) -> None:
        items = exhaust(build_array(0x04BE, DType.U8, payload))
        (token,) = items
        assert bytes(token.raw) == payload
        assert token.element_count == len(payload)


class TestTokenizerFuzz:
    @given(st.binary(max_size=4096))
    @settings(max_examples=200, deadline=1000)
    def test_random_bytes_always_account(self, data: bytes) -> None:
        """Garbage in, classified spans out — never an exception."""
        items = exhaust(data)
        assert all(isinstance(i, (FieldToken, UnknownSpan)) for i in items)

    @given(
        st.integers(min_value=0, max_value=16383),
        st.integers(min_value=0, max_value=255),
    )
    @settings(max_examples=100, deadline=2000)
    def test_single_byte_mutation_of_real_stream(
        self, position: int, replacement: int
    ) -> None:
        """Flip any byte of a real data-stream slice: the walk still
        terminates with exact accounting."""
        base = _real_stream_slice()
        position %= len(base)
        mutated = base[:position] + bytes([replacement]) + base[position + 1 :]
        exhaust(mutated)

    @given(st.integers(min_value=1, max_value=16000))
    @settings(max_examples=50, deadline=2000)
    def test_truncation_of_real_stream(self, cut: int) -> None:
        base = _real_stream_slice()
        exhaust(base[: min(cut, len(base))])


@lru_cache(maxsize=1)
def _real_stream_slice() -> bytes:
    if not FIXTURE.exists():
        pytest.skip("real fixture not available")
    with zipfile.ZipFile(FIXTURE) as archive:
        raw = archive.read("Streams/stream_2.table")
    stream = parse_container(2, raw)
    entry = stream.main
    return raw[entry.offset : entry.offset + min(16384, entry.size)]
