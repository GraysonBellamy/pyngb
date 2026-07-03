"""Validators must report degenerate data as findings, never crash on it.

Empty frames, all-null columns, single rows, and null-poisoned columns are
exactly what a quality checker exists to flag; before these fixes each of them
raised TypeError/IndexError out of full_validation or silently disabled
checks.
"""

import json

import polars as pl

from pyngb.validation import QualityChecker


class TestDegenerateData:
    def test_empty_dataset_reports_error(self) -> None:
        df = pl.DataFrame({"time": [], "sample_temperature": [], "mass": []})
        result = QualityChecker(df).full_validation()
        assert not result.is_valid
        assert any("empty" in e.lower() for e in result.errors)

    def test_all_null_temperature_reports_error(self) -> None:
        df = pl.DataFrame(
            {"time": [1.0, 2.0], "sample_temperature": [None, None]},
            schema={"time": pl.Float64, "sample_temperature": pl.Float64},
        )
        result = QualityChecker(df).full_validation()
        assert not result.is_valid
        assert any("temperature has no valid" in e.lower() for e in result.errors)
        # The old code reported all-null as "Temperature is constant"
        assert not any("constant" in e.lower() for e in result.errors)

    def test_all_null_mass_and_dsc_report_errors(self) -> None:
        df = pl.DataFrame(
            {
                "time": [1.0, 2.0],
                "sample_temperature": [25.0, 30.0],
                "mass": [None, None],
                "dsc_signal": [None, None],
            },
            schema={
                "time": pl.Float64,
                "sample_temperature": pl.Float64,
                "mass": pl.Float64,
                "dsc_signal": pl.Float64,
            },
        )
        result = QualityChecker(df).full_validation()
        assert any("mass has no valid" in e.lower() for e in result.errors)
        assert any("dsc has no valid" in e.lower() for e in result.errors)

    def test_single_row_completes(self) -> None:
        df = pl.DataFrame(
            {"time": [1.0], "sample_temperature": [25.0], "dsc_signal": [0.5]}
        )
        result = QualityChecker(df).full_validation()  # must not raise
        assert isinstance(result.summary()["error_count"], int)

    def test_no_validator_crash_findings_on_degenerate_data(self) -> None:
        """The per-validator safety net should stay unused: degenerate inputs
        are handled by the validators themselves, not the crash catcher."""
        frames = [
            pl.DataFrame({"time": [], "sample_temperature": [], "mass": []}),
            pl.DataFrame(
                {"time": [1.0, None], "sample_temperature": [None, None]},
                schema={"time": pl.Float64, "sample_temperature": pl.Float64},
            ),
            pl.DataFrame({"time": [1.0], "sample_temperature": [25.0]}),
        ]
        for df in frames:
            result = QualityChecker(df).full_validation()
            assert not any("crashed" in e for e in result.errors), result.errors


class TestNullPoisoning:
    def test_null_in_time_does_not_fake_backwards_error(self) -> None:
        """One null used to produce 'Time goes backwards 0 times' (NUM-07)."""
        df = pl.DataFrame(
            {
                "time": [1.0, None, 2.0, 3.0],
                "sample_temperature": [25.0, 26.0, 27.0, 28.0],
            }
        )
        result = QualityChecker(df).full_validation()
        assert not any("goes backwards" in e for e in result.errors)
        assert any("null" in w.lower() for w in result.warnings)

    def test_null_in_time_still_detects_real_backwards(self) -> None:
        df = pl.DataFrame(
            {
                "time": [1.0, None, 3.0, 2.0],
                "sample_temperature": [25.0, 26.0, 27.0, 28.0],
            }
        )
        result = QualityChecker(df).full_validation()
        assert any("goes backwards 1 times" in e for e in result.errors)

    def test_null_does_not_disable_outlier_detection(self) -> None:
        """One null used to NaN the percentiles and skip outliers (NUM-07)."""
        values = [float(i) for i in range(20)] + [10000.0] * 3 + [None]
        df = pl.DataFrame(
            {
                "time": [float(i) for i in range(len(values))],
                "sample_temperature": [25.0 + i for i in range(len(values))],
                "mass": values,
            }
        )
        result = QualityChecker(df).full_validation()
        assert any("outlier" in w.lower() for w in result.warnings)


class TestCorruptEmbeddedMetadata:
    def test_corrupt_json_metadata_proceeds_without_metadata(self) -> None:
        """Malformed embedded JSON must not break checker construction (NUM-10)."""
        df = pl.DataFrame({"time": [1.0, 2.0], "sample_temperature": [25.0, 30.0]})
        table = df.to_arrow().replace_schema_metadata(
            {b"file_metadata": b"{not valid json"}
        )
        checker = QualityChecker(table)
        assert checker.metadata == {}
        result = checker.full_validation()
        assert isinstance(result.summary()["error_count"], int)

    def test_valid_json_metadata_still_extracted(self) -> None:
        df = pl.DataFrame({"time": [1.0, 2.0], "sample_temperature": [25.0, 30.0]})
        table = df.to_arrow().replace_schema_metadata(
            {b"file_metadata": json.dumps({"sample_name": "x"}).encode()}
        )
        assert QualityChecker(table).metadata == {"sample_name": "x"}


class TestQuickCheckDegenerate:
    def test_quick_check_all_null_temperature(self) -> None:
        df = pl.DataFrame(
            {"time": [1.0, 2.0], "sample_temperature": [None, None]},
            schema={"time": pl.Float64, "sample_temperature": pl.Float64},
        )
        issues = QualityChecker(df).quick_check()  # must not raise
        assert any("no valid" in i.lower() for i in issues)
        assert not any("constant" in i.lower() for i in issues)


def test_validators_still_catch_real_problems() -> None:
    """The rewrite must not weaken detection on plainly bad data."""
    df = pl.DataFrame(
        {
            "time": [1.0, 2.0, 1.5, 4.0],
            "sample_temperature": [-300.0, 25.0, 50.0, 75.0],
        }
    )
    result = QualityChecker(df).full_validation()
    assert not result.is_valid
    assert any("absolute zero" in e for e in result.errors)
    assert any("goes backwards" in e for e in result.errors)
