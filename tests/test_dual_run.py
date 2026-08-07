"""Sample + Correction (.ngb-ds3) support: run selection and embedded
baseline subtraction over real files.

The two fixtures are the files from issue #198: two different paper samples
measured the same day against the SAME stored correction measurement. That
shared correction is what makes them a sharp oracle — the sample runs must
differ while the embedded correction runs must be byte-identical. The
pre-fix parser overwrote the sample run with the correction run and returned
identical frames for both files.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pyngb import read_ngb, read_ngb_metadata
from pyngb.format import count_runs, load_document

FIXTURE_DIR = Path(__file__).parent / "test_files"
DS3_G = FIXTURE_DIR / "01_Messung_Probe_G_300_Grad_Ar.ngb-ds3"
DS3_E = FIXTURE_DIR / "02_Messung_Probe_E_300_Grad_Ar.ngb-ds3"
SS3 = FIXTURE_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
BS3 = FIXTURE_DIR / "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"


def frame(path: Path, **kwargs) -> pl.DataFrame:
    result = pl.from_arrow(read_ngb(path, **kwargs))
    assert isinstance(result, pl.DataFrame)
    return result


class TestRunSelection:
    def test_different_samples_yield_different_data(self) -> None:
        """The issue #198 regression: two different samples must never
        parse to identical frames."""
        assert not frame(DS3_G).equals(frame(DS3_E))

    def test_shared_correction_runs_are_identical(self) -> None:
        corr_g = frame(DS3_G, run="correction")
        corr_e = frame(DS3_E, run="correction")
        assert corr_g.equals(corr_e)

    def test_default_is_the_sample_run(self) -> None:
        assert frame(DS3_G).equals(frame(DS3_G, run="sample"))

    def test_sample_and_correction_differ(self) -> None:
        assert not frame(DS3_G).equals(frame(DS3_G, run="correction"))

    def test_sample_run_shows_the_sample_mass_loss(self) -> None:
        """The paper samples lose ~2-3 mg; an empty-crucible correction
        moves only within buoyancy drift (< 1 mg)."""
        assert frame(DS3_G)["mass"].min() < -2.0
        assert abs(frame(DS3_G, run="correction")["mass"].min()) < 1.0

    def test_both_runs_have_the_full_channel_set(self) -> None:
        sample, correction = frame(DS3_G), frame(DS3_G, run="correction")
        assert sample.columns == correction.columns
        assert sample.height == correction.height == 5801

    def test_run_counts(self) -> None:
        assert count_runs(load_document(DS3_G, streams=(2, 3))) == 2
        assert count_runs(load_document(SS3, streams=(2, 3))) == 1
        assert count_runs(load_document(BS3, streams=(2, 3))) == 1

    def test_correction_from_a_single_run_file_raises(self) -> None:
        with pytest.raises(ValueError, match="no embedded correction run"):
            read_ngb(SS3, run="correction")

    def test_unknown_run_raises(self) -> None:
        with pytest.raises(ValueError, match="run must be one of"):
            read_ngb(DS3_G, run="banana")  # type: ignore[arg-type]

    def test_correction_with_baseline_file_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be combined"):
            read_ngb(DS3_G, run="correction", baseline_file=DS3_G)

    def test_run_is_tagged_in_schema_metadata(self) -> None:
        """The file-level metadata describes the sample either way, so the
        run tag is what distinguishes a correction export from a sample
        export."""
        for run in ("sample", "correction", "corrected"):
            table = read_ngb(DS3_G, run=run)  # type: ignore[arg-type]
            assert table.schema.metadata[b"run"] == run.encode()


class TestCorrectedRun:
    def test_corrected_equals_self_baseline_subtraction(self) -> None:
        """run="corrected" is exactly the file baseline-subtracted against
        its own embedded correction."""
        assert frame(DS3_G, run="corrected").equals(frame(DS3_G, baseline_file=DS3_G))

    def test_corrected_marks_columns_baseline_corrected(self) -> None:
        table = read_ngb(DS3_G, run="corrected")
        assert table.schema.metadata[b"run"] == b"corrected"
        mass_field = table.schema.field("mass")
        assert b"baseline_corrected" in mass_field.metadata[b"processing_history"]

    def test_corrected_on_a_single_run_file_raises(self) -> None:
        with pytest.raises(ValueError, match="no embedded correction run"):
            read_ngb(SS3, run="corrected")

    def test_corrected_with_baseline_file_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be combined"):
            read_ngb(DS3_G, run="corrected", baseline_file=BS3)


class TestMetadata:
    def test_metadata_describes_the_sample_measurement(self) -> None:
        metadata_g = read_ngb_metadata(DS3_G)
        metadata_e = read_ngb_metadata(DS3_E)
        assert metadata_g["sample_name"] == "Papier G"
        assert metadata_e["sample_name"] == "Papier E"
        assert metadata_g["sample_mass"] == pytest.approx(10.848)
        assert metadata_e["sample_mass"] == pytest.approx(13.856)

    def test_correction_provenance_is_recorded(self) -> None:
        assert "correction_file_path" in read_ngb_metadata(DS3_G)

    def test_timezone_is_the_runs_not_a_calibration_snapshot(self) -> None:
        """File 01's calibration-context block carries winter (standard-
        time) snapshots from the referenced calibration files; the run
        itself was measured in August (CEST). Both files were measured the
        same day, so their run environments must agree."""
        metadata_g = read_ngb_metadata(DS3_G)
        metadata_e = read_ngb_metadata(DS3_E)
        assert metadata_g["timezone"] == "Mitteleuropäische Sommerzeit"
        assert metadata_e["timezone"] == "Mitteleuropäische Sommerzeit"
        assert metadata_g["utc_offset_minutes"] == 120
        assert metadata_e["utc_offset_minutes"] == 120

    def test_correction_run_carries_the_file_level_metadata(self) -> None:
        metadata, _ = read_ngb(DS3_G, run="correction", return_metadata=True)
        assert metadata["sample_name"] == "Papier G"


class TestEmbeddedBaseline:
    def test_subtracting_the_embedded_correction(self) -> None:
        """read_ngb(f, baseline_file=f) reproduces Proteus' displayed
        Sample + Correction curves: mass and DSC change, the row count and
        the axis channels do not."""
        raw = frame(DS3_G)
        corrected = frame(DS3_G, baseline_file=DS3_G)
        assert corrected.height == raw.height
        assert corrected.columns == raw.columns
        assert np.array_equal(corrected["time"].to_numpy(), raw["time"].to_numpy())
        assert np.array_equal(
            corrected["sample_temperature"].to_numpy(),
            raw["sample_temperature"].to_numpy(),
        )
        assert not np.array_equal(
            corrected["dsc_signal"].to_numpy(), raw["dsc_signal"].to_numpy()
        )
        assert not np.array_equal(corrected["mass"].to_numpy(), raw["mass"].to_numpy())

    def test_subtraction_removes_the_instrument_offset(self) -> None:
        """The raw sample DSC carries the same instrument baseline as the
        correction (~-3 to -4 uV at start); subtracting must collapse the
        initial signal toward zero."""
        raw = frame(DS3_G)
        corrected = frame(DS3_G, baseline_file=DS3_G)
        assert abs(raw["dsc_signal"][0]) > 3.0
        assert abs(corrected["dsc_signal"][0]) < 1.0

    def test_ds3_as_baseline_for_another_file_uses_its_correction(self) -> None:
        """A ds3 passed as baseline_file contributes its embedded correction
        run — the two issue files share one, so subtracting either file's
        correction from the same sample gives identical results."""
        via_g = frame(DS3_G, baseline_file=DS3_G)
        via_e = frame(DS3_G, baseline_file=DS3_E)
        assert via_g.equals(via_e)

    def test_a_bs3_baseline_still_works(self) -> None:
        corrected = frame(SS3, baseline_file=BS3)
        assert corrected.height == frame(SS3).height
