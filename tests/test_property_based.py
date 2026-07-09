"""Property-based tests for analysis and validation using Hypothesis.

DTG properties are checked against closed-form expectations on valid input
instead of being swallowed by try/except. Binary-format properties
(scalar/array/string round-trips, random-bytes and real-stream mutation
fuzz) live in test_format_properties.py on the tokenizer.
"""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from pyngb.analysis import dtg
from pyngb.validation import ValidationResult


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
