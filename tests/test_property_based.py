"""Property-based tests using Hypothesis.

Every property here can actually fail: values are round-tripped through the
binary parser with exact-equality assertions, corrupted real-instrument data
must fail loudly with a typed exception, and DTG properties are checked
against closed-form expectations on valid input instead of being swallowed
by try/except. They replace previous vacuous properties (swapped
parse_value arguments, exception-swallowing DTG checks).
"""

from __future__ import annotations

import struct
import zipfile
from functools import lru_cache
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from pyngb.analysis import dtg
from pyngb.binary import BinaryParser
from pyngb.constants import DataType, PatternConfig
from pyngb.exceptions import NGBParseError
from pyngb.extractors import DataStreamProcessor
from pyngb.validation import ValidationResult

FIXTURE = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"


class TestSplitTablesProperties:
    """Structural invariants of the table splitter for arbitrary input."""

    @given(st.binary(min_size=0, max_size=10000))
    @settings(max_examples=100, deadline=1000)
    def test_split_tables_never_crashes(self, data: bytes) -> None:
        """split_tables should handle any input without crashing."""
        parser = BinaryParser()
        result = parser.split_tables(data)
        assert isinstance(result, list)
        assert all(isinstance(table, bytes) for table in result)

    @given(st.binary(min_size=0, max_size=1000))
    @settings(max_examples=100, deadline=1000)
    def test_split_tables_preserves_data_length(self, data: bytes) -> None:
        """Total length of split tables should not exceed original data."""
        parser = BinaryParser()
        result = parser.split_tables(data)
        total_length = sum(len(table) for table in result)
        assert total_length <= len(data)


class TestParseValueRoundTrip:
    """parse_value must decode exactly what struct.pack encoded."""

    @given(st.integers(min_value=-(2**31), max_value=2**31 - 1))
    @settings(max_examples=200, deadline=1000)
    def test_int32_round_trip(self, value: int) -> None:
        parser = BinaryParser()
        result = parser.parse_value(DataType.INT32.value, struct.pack("<i", value))
        assert isinstance(result, int)
        assert result == value

    @given(st.floats(width=32, allow_nan=False))
    @settings(max_examples=200, deadline=1000)
    def test_float32_round_trip(self, value: float) -> None:
        parser = BinaryParser()
        result = parser.parse_value(DataType.FLOAT32.value, struct.pack("<f", value))
        assert isinstance(result, float)
        assert result == value

    @given(st.floats(allow_nan=False))
    @settings(max_examples=200, deadline=1000)
    def test_float64_round_trip(self, value: float) -> None:
        parser = BinaryParser()
        result = parser.parse_value(DataType.FLOAT64.value, struct.pack("<d", value))
        assert isinstance(result, float)
        assert result == value

    @given(
        st.text(
            alphabet=st.characters(
                blacklist_categories=("Cs",), blacklist_characters="\x00"
            ),
            min_size=1,
            max_size=100,
        )
    )
    @settings(max_examples=200, deadline=1000)
    def test_fffeff_string_round_trip(self, text: str) -> None:
        """NETZSCH fffeff format: prefix + UTF-16LE code-unit count + data."""
        encoded = text.encode("utf-16le")
        code_units = len(encoded) // 2
        assume(code_units <= 255)  # single count byte
        payload = b"\xff\xfe\xff" + bytes([code_units]) + encoded

        parser = BinaryParser()
        assert parser.parse_value(DataType.STRING.value, payload) == text

    @given(
        st.text(
            alphabet=st.characters(
                blacklist_categories=("Cs",), blacklist_characters="\x00"
            ),
            min_size=1,
            max_size=100,
        )
    )
    @settings(max_examples=200, deadline=1000)
    def test_standard_string_round_trip(self, text: str) -> None:
        """Standard format: 4-byte length prefix + UTF-8 data."""
        # The parser strips surrounding whitespace, so only whitespace-free
        # boundaries round-trip exactly.
        assume(text == text.strip())
        encoded = text.encode("utf-8")
        payload = struct.pack("<I", len(encoded)) + encoded

        parser = BinaryParser()
        assert parser.parse_value(DataType.STRING.value, payload) == text

    @given(st.binary(min_size=0, max_size=16))
    @settings(max_examples=100, deadline=1000)
    def test_wrong_length_payload_returns_none(self, payload: bytes) -> None:
        """Fixed-width types reject payloads of the wrong size with None."""
        parser = BinaryParser()
        if len(payload) != 4:
            assert parser.parse_value(DataType.INT32.value, payload) is None
        if len(payload) != 8:
            assert parser.parse_value(DataType.FLOAT64.value, payload) is None

    @given(st.binary(min_size=1, max_size=1), st.binary(min_size=0, max_size=64))
    @settings(max_examples=100, deadline=1000)
    def test_unknown_type_returns_raw_bytes(
        self, data_type: bytes, payload: bytes
    ) -> None:
        known = {t.value for t in DataType}
        assume(data_type not in known)
        parser = BinaryParser()
        assert parser.parse_value(data_type, payload) == payload


@lru_cache(maxsize=1)
def _stream_2_bytes() -> bytes:
    with zipfile.ZipFile(FIXTURE) as z:
        return z.read("Streams/stream_2.table")


def _process(stream: bytes) -> pl.DataFrame:
    return DataStreamProcessor(PatternConfig(), BinaryParser()).process_stream_2(stream)


@pytest.mark.skipif(not FIXTURE.exists(), reason="real fixture not available")
class TestStreamCorruptionProperties:
    """Corrupted measurement data must parse cleanly or fail loudly.

    These lock in the loud-corruption contract: the
    only exception type the stream processor may leak is NGBParseError. A
    silent success is acceptable only as a well-formed DataFrame - truncation
    at a table boundary is indistinguishable from a file that legitimately
    records fewer channels.
    """

    @given(st.data())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_truncation_parses_or_raises_typed_error(self, data: st.DataObject) -> None:
        stream = _stream_2_bytes()
        cut = data.draw(st.integers(min_value=0, max_value=len(stream) - 1))
        try:
            result = _process(stream[:cut])
        except NGBParseError:
            return
        assert isinstance(result, pl.DataFrame)

    @given(st.data())
    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_byte_flip_parses_or_raises_typed_error(self, data: st.DataObject) -> None:
        stream = _stream_2_bytes()
        pos = data.draw(st.integers(min_value=0, max_value=len(stream) - 1))
        value = data.draw(st.integers(min_value=0, max_value=255))
        assume(stream[pos] != value)

        mutated = bytearray(stream)
        mutated[pos] = value
        try:
            result = _process(bytes(mutated))
        except NGBParseError:
            return
        assert isinstance(result, pl.DataFrame)


@st.composite
def time_and_mass(draw: st.DrawFn, min_size: int = 5, max_size: int = 100):
    """Strictly increasing time plus finite mass, both valid DTG input."""
    n = draw(st.integers(min_value=min_size, max_value=max_size))
    deltas = draw(
        st.lists(
            st.floats(min_value=0.05, max_value=10.0),
            min_size=n,
            max_size=n,
        )
    )
    time = np.cumsum(deltas)
    mass = np.array(
        draw(
            st.lists(
                st.floats(min_value=0.0, max_value=100.0),
                min_size=n,
                max_size=n,
            )
        )
    )
    return time, mass


class TestDTGProperties:
    """DTG properties on valid input, with no exception swallowing."""

    @given(time_and_mass())
    @settings(max_examples=50, deadline=2000)
    def test_output_same_length_and_finite(self, data) -> None:
        time, mass = data
        result = dtg(time, mass)
        assert len(result) == len(time)
        assert np.isfinite(result).all()

    @given(time_and_mass())
    @settings(max_examples=50, deadline=2000)
    def test_constant_mass_gives_zero_dtg(self, data) -> None:
        time, _ = data
        mass = np.full_like(time, 100.0)
        result = dtg(time, mass)
        assert np.allclose(result, 0.0, atol=1e-6)

    @given(
        time_and_mass(),
        st.floats(min_value=0.01, max_value=1.0),
    )
    @settings(max_examples=50, deadline=2000)
    def test_linear_mass_loss_recovers_rate(self, data, rate: float) -> None:
        """mass = m0 - rate*t must give DTG == rate*60 mg/min everywhere.

        Uses method="gradient": np.gradient with a coordinate array is exact
        for linear functions even on non-uniform time, and Savitzky-Golay
        smoothing of the resulting constant preserves it.
        """
        time, _ = data
        mass = 100.0 - rate * time
        result = dtg(time, mass, method="gradient")
        assert np.allclose(result, rate * 60.0, rtol=1e-6, atol=1e-8)

    @given(time_and_mass(min_size=6), st.data())
    @settings(max_examples=50, deadline=2000)
    def test_duplicate_timestamp_rejected(self, data, drawn: st.DataObject) -> None:
        """A single duplicated timestamp must be rejected, not smeared (NUM-05)."""
        time, mass = data
        k = drawn.draw(st.integers(min_value=1, max_value=len(time) - 1))
        time = time.copy()
        time[k] = time[k - 1]
        with pytest.raises(ValueError, match="strictly increasing"):
            dtg(time, mass)


class TestValidationResultProperties:
    """Property-based tests for ValidationResult."""

    @given(st.lists(st.text(min_size=1, max_size=100), max_size=50))
    @settings(max_examples=50, deadline=1000)
    def test_validation_result_error_count(self, errors: list[str]) -> None:
        """ValidationResult should accurately count errors."""
        result = ValidationResult()
        for error in errors:
            result.add_error(error)

        assert len(result.errors) == len(errors)
        assert result.summary()["error_count"] == len(errors)
        if errors:
            assert not result.is_valid

    @given(
        st.lists(st.text(min_size=1, max_size=100), max_size=20),
        st.lists(st.text(min_size=1, max_size=100), max_size=20),
        st.lists(st.text(min_size=1, max_size=100), max_size=20),
    )
    @settings(max_examples=30, deadline=1000)
    def test_validation_result_total_issues(
        self, errors: list[str], warnings: list[str], info: list[str]
    ) -> None:
        """Total issues should equal errors + warnings."""
        result = ValidationResult()
        for error in errors:
            result.add_error(error)
        for warning in warnings:
            result.add_warning(warning)
        for i in info:
            result.add_info(i)

        summary = result.summary()
        assert summary["total_issues"] == len(errors) + len(warnings)
        assert summary["error_count"] == len(errors)
        assert summary["warning_count"] == len(warnings)

    @given(st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=20))
    @settings(max_examples=30, deadline=1000)
    def test_validation_result_report_contains_all_errors(
        self, errors: list[str]
    ) -> None:
        """Generated report should contain all error messages."""
        result = ValidationResult()
        for error in errors:
            result.add_error(error)

        report = result.report()
        for error in errors:
            assert error in report
