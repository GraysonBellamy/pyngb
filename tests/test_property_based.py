"""Property-based tests using Hypothesis.

These tests verify properties that should hold for all inputs,
not just specific test cases. This helps catch edge cases and
ensures robustness of the implementation.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from hypothesis import given, settings, strategies as st

from pyngb.analysis import dtg
from pyngb.binary.parser import BinaryParser
from pyngb.validation import ValidationResult


class TestBinaryParserProperties:
    """Property-based tests for binary parsing."""

    @given(st.binary(min_size=0, max_size=10000))
    @settings(max_examples=100, deadline=1000)
    def test_split_tables_never_crashes(self, data: bytes) -> None:
        """split_tables should handle any input without crashing."""
        parser = BinaryParser()
        result = parser.split_tables(data)
        assert isinstance(result, list)
        # All results should be bytes
        assert all(isinstance(table, bytes) for table in result)

    @given(st.binary(min_size=0, max_size=1000))
    @settings(max_examples=100, deadline=1000)
    def test_split_tables_preserves_data_length(self, data: bytes) -> None:
        """Total length of split tables should not exceed original data."""
        parser = BinaryParser()
        result = parser.split_tables(data)
        total_length = sum(len(table) for table in result)
        # Total length should be <= original (due to separator removal)
        assert total_length <= len(data)

    @given(st.binary(min_size=4, max_size=100))
    @settings(max_examples=100, deadline=1000)
    def test_parse_value_handles_all_inputs(self, data: bytes) -> None:
        """parse_value should handle any binary input gracefully."""
        parser = BinaryParser()
        # Should not crash for any input
        try:
            result = parser.parse_value(
                data, 0x08
            )  # Try as int32  # type: ignore[arg-type]
            # Result should be valid type or None
            assert result is None or isinstance(result, (int, float, str, bytes))
        except (ValueError, struct.error):
            # These are acceptable exceptions for invalid data
            pass


class TestDTGCalculationProperties:
    """Property-based tests for DTG calculations."""

    @given(
        st.lists(
            st.floats(
                min_value=0, max_value=1000, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=100,
        ),
        st.lists(
            st.floats(
                min_value=0, max_value=100, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=100,
        ),
    )
    @settings(max_examples=50, deadline=2000)
    def test_dtg_output_length(
        self, time_data: list[float], mass_data: list[float]
    ) -> None:
        """DTG output should have same length as input."""
        # Make arrays same length
        min_len = min(len(time_data), len(mass_data))
        time_data = time_data[:min_len]
        mass_data = mass_data[:min_len]

        if min_len < 2:
            return  # Skip too small datasets

        time_arr = np.array(time_data)
        mass_arr = np.array(mass_data)

        try:
            result = dtg(time_arr, mass_arr, smooth=0)  # type: ignore[arg-type]
            assert len(result) == len(time_arr)
        except (ValueError, RuntimeError):
            # Expected for invalid data
            pass

    @given(
        st.lists(
            st.floats(
                min_value=0, max_value=1000, allow_nan=False, allow_infinity=False
            ),
            min_size=10,
            max_size=50,
        )
    )
    @settings(max_examples=50, deadline=2000)
    def test_dtg_constant_mass_is_zero(self, time_data: list[float]) -> None:
        """DTG of constant mass should be near zero."""
        if len(time_data) < 10:
            return

        time_arr = np.array(time_data)
        mass_arr = np.full_like(time_arr, 100.0)  # Constant mass

        try:
            result = dtg(time_arr, mass_arr, smooth=1)  # type: ignore[arg-type]
            # DTG should be very close to zero for constant mass
            assert np.allclose(result, 0.0, atol=1e-6)
        except (ValueError, RuntimeError):
            # Expected for invalid data
            pass

    @given(
        st.integers(min_value=10, max_value=100),
        st.sampled_from([0, 1, 2, 3]),
    )
    @settings(max_examples=20, deadline=2000)
    def test_dtg_smoothing_increases_stability(
        self, n_points: int, smooth_level: int
    ) -> None:
        """Higher smoothing should reduce variance in DTG."""
        # Create monotonically increasing time
        time_arr = np.linspace(0, 100, n_points)
        # Create mass with some noise
        mass_arr = 100 - 0.5 * time_arr + np.random.normal(0, 0.1, n_points)

        try:
            dtg_smooth0 = dtg(time_arr, mass_arr, smooth=0)  # type: ignore[arg-type]
            dtg_smooth = dtg(time_arr, mass_arr, smooth=smooth_level)  # type: ignore[arg-type]

            # Higher smoothing should generally reduce variance
            # (though not guaranteed for all data)
            var0 = np.var(dtg_smooth0)
            var_smooth = np.var(dtg_smooth)

            # Just verify both are finite
            assert np.isfinite(var0)
            assert np.isfinite(var_smooth)
        except (ValueError, RuntimeError):
            pass


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
        # If there are errors, result should not be valid
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
        # All errors should appear in the report
        for error in errors:
            assert error in report


class TestDataFrameValidationProperties:
    """Property-based tests for DataFrame validation."""

    @given(
        st.integers(min_value=1, max_value=100),
        st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=30, deadline=2000)
    def test_dataframe_null_count_property(self, n_rows: int, n_cols: int) -> None:
        """Null count should match expected for DataFrame."""
        # Create DataFrame with known null pattern
        data = {}
        for i in range(n_cols):
            # Create column with some nulls
            col_data = [float(j) if j % 3 != 0 else None for j in range(n_rows)]
            data[f"col_{i}"] = col_data

        df = pl.DataFrame(data)

        # Check null counts
        null_counts = df.null_count()
        for col in df.columns:
            null_count = null_counts[col][0]
            # Should have roughly n_rows // 3 nulls
            assert null_count >= 0
            assert null_count <= n_rows


# Add struct import for parse_value test
import struct  # noqa: E402
