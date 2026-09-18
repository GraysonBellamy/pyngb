"""Push-rod dilatometer support: NETZSCH DIL 402 Expedis (.ngb-dla / .ngb-cla).

The fixtures are two red-oak runs (Proteus 8.0.3, 10 K/min to 900 °C in
N2): the RO_Para R1 Sample + Correction ``.ngb-dla`` with the blank
correction ``.ngb-cla`` it embeds, and the RO_Perp R1 ``.ngb-dla``. The
investigation used four runs; the other files are in the lab's data and
add nothing these three do not cover. They exercise the same container, grammar and
dual-run machinery as the STA files; what is new is the channel set
(``length_change``, ``force``, ``force_setpoint``), the dilatometer metadata
(sample length and geometry, reference-standard expansion curve, force
setpoint) and the correction, which after subtracting the blank run restores
the reference standard's literature expansion over the sample length::

    corrected dL = dL_sample - dL_correction(t) + L0 * curve(T_sample)

That formula was verified against Proteus's own CSV exports of all four runs
(residual ~2e-6 in dL/L0). The exports are the user's lab data and are not
committed; ``TestAgainstProteusExport`` runs only when
``PYNGB_DIL_CSV_DIR`` points at them.
"""

from __future__ import annotations

import math
import os
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pyngb import (
    apply_expansion_standard,
    normalize_to_initial_length,
    normalize_to_initial_mass,
    read_ngb,
    read_ngb_metadata,
)
from pyngb.api.cli import main as cli_main
from pyngb.api.metadata import (
    get_column_baseline_status,
    get_column_units,
    get_processing_history,
)
from pyngb.format import count_runs, load_document
from pyngb.format.maps import (
    CHANNEL_DEF_NAME_FIELD,
    CHANNEL_HEADER_TYPE,
    STREAM_3_CHANNEL_DEF_TYPE,
    STREAM_3_CHANNEL_LABELS,
)

FIXTURE_DIR = Path(__file__).parent / "test_files"
DLA = FIXTURE_DIR / "RO_Para_N2_10K_260910_R1.ngb-dla"
CLA = FIXTURE_DIR / "RO_Para_N2_10K_260910_Correction_R1.ngb-cla"
DLA_2 = FIXTURE_DIR / "RO_Perp_N2_10K_260917_R1.ngb-dla"
SS3 = FIXTURE_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
ALL_DLA = sorted(FIXTURE_DIR.glob("*.ngb-dla"))
ALL_STA = sorted(FIXTURE_DIR.glob("*.ngb-*s3"))
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*"))

DIL_COLUMNS = [
    "time",
    "sample_temperature",
    "length_change",
    "82",
    "force",
    "force_setpoint",
    "purge_flow_2",
    "protective_flow",
    "furnace_temperature",
    "cooling_power",
    "furnace_power",
    "h_foil_temperature",
]

# The fixtures: initial length L0 (mm) and diameter (mm) as typed into Proteus.
SAMPLE_LENGTH_MM = {
    "RO_Para_N2_10K_260910_R1.ngb-dla": 15.99,
    "RO_Para_N2_10K_260915_R2.ngb-dla": 16.02,
    "RO_Para_N2_10K_260916_R3.ngb-dla": 16.12,
    "RO_Perp_N2_10K_260917_R1.ngb-dla": 16.65,
}


def correction_of(dla: Path) -> Path:
    return dla.with_name(dla.name.replace("_R", "_Correction_R")).with_suffix(
        ".ngb-cla"
    )


# Sample + Correction files whose stand-alone correction is also present.
PAIRED_DLA = [dla for dla in ALL_DLA if correction_of(dla).exists()]


def frame(path: Path, **kwargs) -> pl.DataFrame:
    result = pl.from_arrow(read_ngb(path, **kwargs))
    assert isinstance(result, pl.DataFrame)
    return result


class TestChannels:
    def test_column_set(self) -> None:
        assert frame(DLA).columns == DIL_COLUMNS
        assert frame(CLA).columns == DIL_COLUMNS

    def test_length_change_is_the_f64_displacement_in_micrometres(self) -> None:
        df = frame(DLA)
        assert df["length_change"].dtype == pl.Float64
        assert df["length_change"][0] == 0.0
        # Red oak shrinks by roughly a quarter of its length by 900 °C.
        assert -5000 < df["length_change"].min() < -3000

    def test_force_tracks_the_constant_setpoint(self) -> None:
        df = frame(DLA)
        assert df["force_setpoint"].unique().to_list() == pytest.approx([0.2])
        assert (df["force"] - 0.2).abs().max() < 0.005

    def test_unmapped_and_idle_channels(self) -> None:
        df = frame(DLA)
        assert df["82"].abs().max() == 0.0
        assert df["cooling_power"].abs().max() == 0.0

    def test_column_units(self) -> None:
        table = read_ngb(DLA)
        assert get_column_units(table, "length_change") == "µm"
        assert get_column_units(table, "force") == "N"
        assert get_column_units(table, "force_setpoint") == "N"
        assert get_column_units(table, "furnace_power") == "%"
        assert get_column_units(table, "cooling_power") == "%"
        assert get_column_baseline_status(table, "length_change") is False

    @pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
    def test_stream_3_channel_names_match_the_file(self, fixture: Path) -> None:
        """Stream-3 channels are self-described in stream 1; the pinned
        labels (and therefore CHANNEL_MAP's stream-3 names) must agree with
        every fixture — STA and DIL alike."""
        doc = load_document(fixture, streams=(1, 3))
        definitions = {
            0x30 + (table.category - 0x7530): table.value(CHANNEL_DEF_NAME_FIELD)
            for table in doc.find(1, type_ref=STREAM_3_CHANNEL_DEF_TYPE)
        }
        assert definitions, "no stream-3 channel definitions found"
        for channel_id, label in definitions.items():
            assert STREAM_3_CHANNEL_LABELS[channel_id] == label
        if 3 in doc.streams:
            headers = {
                table.category & 0xFF
                for table in doc.find(3, type_ref=CHANNEL_HEADER_TYPE)
            }
            assert headers <= set(definitions)


class TestRuns:
    def test_run_counts(self) -> None:
        assert count_runs(load_document(DLA, streams=(2, 3))) == 2
        assert count_runs(load_document(CLA, streams=(2, 3))) == 1

    @pytest.mark.parametrize("dla", PAIRED_DLA, ids=lambda p: p.name)
    def test_embedded_correction_equals_the_cla_file(self, dla: Path) -> None:
        assert frame(dla, run="correction").equals(frame(correction_of(dla)))

    def test_sample_and_correction_differ(self) -> None:
        sample, correction = frame(DLA), frame(DLA, run="correction")
        assert sample.height == correction.height + 1
        assert abs(correction["length_change"].min()) < 50
        assert sample["length_change"].min() < -3000

    def test_correction_file_was_measured_earlier(self) -> None:
        assert (
            read_ngb_metadata(CLA)["date_performed"]
            < (read_ngb_metadata(DLA)["date_performed"])
        )

    def test_schema_tags(self) -> None:
        table = read_ngb(DLA, run="corrected")
        assert table.schema.metadata[b"type"] == b"DIL"
        assert table.schema.metadata[b"run"] == b"corrected"
        assert read_ngb(SS3).schema.metadata[b"type"] == b"STA"


class TestMetadata:
    @pytest.fixture(scope="class")
    def md(self) -> dict:
        return dict(read_ngb_metadata(DLA))

    def test_sample_descriptors(self, md: dict) -> None:
        assert md["sample_length"] == pytest.approx(15.99)
        assert md["sample_diameter"] == pytest.approx(8.1)
        assert md["sample_cross_section"] == pytest.approx(math.pi * 8.1**2 / 4)
        assert md["material"] == "Red Oak"
        for key in ("sample_mass", "crucible_type", "crucible_mass"):
            assert key not in md

    @pytest.mark.parametrize("dla", ALL_DLA, ids=lambda p: p.name)
    def test_sample_length_per_fixture(self, dla: Path) -> None:
        assert read_ngb_metadata(dla)["sample_length"] == pytest.approx(
            SAMPLE_LENGTH_MM[dla.name]
        )

    def test_instrument_and_measurement_type(self, md: dict) -> None:
        assert md["instrument_model"] == "NETZSCH DIL 402 Expedis Select"
        assert md["instrument"].startswith("DIL402")
        assert md["measurement_type"] == "sample_correction"
        assert read_ngb_metadata(CLA)["measurement_type"] == "correction"

    def test_expansion_standard(self, md: dict) -> None:
        standard = md["expansion_standard"]
        assert standard["name"] == "FUSED SILICA"
        assert standard["source"] == "NBS 739/1971"
        assert standard["temperature_min"] == -200.0
        assert standard["temperature_max"] == 1100.0
        assert standard["record_path"].lower().endswith("fused_si.scl")
        assert standard["date"].startswith("1994-08-31")
        curve = standard["curve"]
        temps, expansion = curve["temperature_c"], curve["expansion"]
        assert len(temps) == len(expansion) == 53
        assert temps == sorted(temps)
        assert temps[0] == standard["temperature_min"]
        assert temps[-1] == standard["temperature_max"]
        # Stored relative to 20 °C: the curve passes (almost) through zero there.
        assert abs(np.interp(20.0, temps, expansion)) < 1e-6
        # Fused silica: ~5e-4 at 1100 °C, slightly negative below room temperature.
        assert expansion[-1] == pytest.approx(4.99e-4, rel=1e-2)
        assert expansion[0] < 0

    def test_expansion_standard_on_the_correction_file(self) -> None:
        cla = read_ngb_metadata(CLA)
        assert (
            cla["expansion_standard"]["curve"]
            == (read_ngb_metadata(DLA)["expansion_standard"]["curve"])
        )
        # A blank run: no length, and the stale 7.98 mm diameter left in the
        # Proteus form is not reported as a sample geometry.
        for key in ("sample_length", "sample_diameter", "sample_cross_section"):
            assert key not in cla

    def test_force_setpoint(self, md: dict) -> None:
        assert md["force_setpoint"] == pytest.approx(0.2)
        program = md["temperature_program"]
        assert len(program) == 5
        for stage in program.values():
            assert stage["force_setpoint"] == pytest.approx(0.2)
            assert stage["purge_2_mfc_flow"] == 20.0

    def test_measuring_range(self, md: dict) -> None:
        assert md["length_change_range"] == 20000.0
        for key in ("mass_range", "dsc_range"):
            assert key not in md

    def test_temperature_calibration_is_the_identity_record(self, md: dict) -> None:
        cal = md["temperature_calibration"]
        assert cal["record_path"].endswith("TCALZERO.TMX")
        assert cal["coefficients"] == [0.0, 0.0, 0.0]
        assert len(cal["fixpoints"]) == 12
        assert all(fp["measured_c"] == fp["corrected_c"] for fp in cal["fixpoints"])
        assert "sensitivity_calibration" not in md
        assert "calibration_constants" not in md

    def test_gas_metadata_still_extracts(self, md: dict) -> None:
        assert md["purge_2_mfc_gas"] == "NITROGEN"
        assert md["purge_2_mfc_flow"] == 20.0
        assert md["protective_mfc_flow"] == 50.0
        assert md["purge_1_mfc_flow"] == 0.0


class TestNewKeysOnSTAFiles:
    """The findings that apply to both instrument families."""

    @pytest.mark.parametrize("fixture", ALL_STA, ids=lambda p: p.name)
    def test_measurement_type_matches_the_file_kind(self, fixture: Path) -> None:
        expected = {
            ".ngb-ss3": "sample",
            ".ngb-bs3": "correction",
            ".ngb-ds3": "sample_correction",
        }[fixture.suffix]
        assert read_ngb_metadata(fixture)["measurement_type"] == expected

    @pytest.mark.parametrize("fixture", ALL_STA, ids=lambda p: p.name)
    def test_measuring_ranges(self, fixture: Path) -> None:
        md = read_ngb_metadata(fixture)
        assert md["mass_range"] == 35000.0
        assert md["dsc_range"] == 5000.0
        assert "length_change_range" not in md

    @pytest.mark.parametrize("fixture", ALL_STA, ids=lambda p: p.name)
    def test_instrument_model(self, fixture: Path) -> None:
        md = read_ngb_metadata(fixture)
        if fixture.suffix == ".ngb-ds3":
            assert "instrument_model" not in md  # not written by that Proteus
        else:
            assert md["instrument_model"].startswith("NETZSCH STA 449")

    @pytest.mark.parametrize("fixture", ALL_STA, ids=lambda p: p.name)
    def test_calibration_records_are_found_by_type(self, fixture: Path) -> None:
        """Identity records (TCALZERO.TCX, SENSZERO.EXX) used to be missed
        by the .ngb-ts3/.ngb-es3 suffix rule."""
        md = read_ngb_metadata(fixture)
        assert "record_path" in md["temperature_calibration"]
        assert "record_path" in md["sensitivity_calibration"]
        assert "date_measured" in md["temperature_calibration"]


class TestCorrectedRun:
    @pytest.mark.parametrize("dla", PAIRED_DLA, ids=lambda p: p.name)
    def test_corrected_equals_subtracting_the_cla_file(self, dla: Path) -> None:
        assert frame(dla, run="corrected").equals(
            frame(dla, baseline_file=correction_of(dla))
        )

    def test_corrected_matches_the_formula(self) -> None:
        """Inside the heating ramp the correction is exactly: subtract the
        blank run interpolated on time, add L0 * curve(T_sample)."""
        md = read_ngb_metadata(DLA)
        sample, blank = frame(DLA), frame(DLA, run="correction")
        corrected = frame(DLA, run="corrected", dynamic_axis="time")
        t, temp = sample["time"].to_numpy(), sample["sample_temperature"].to_numpy()
        curve = md["expansion_standard"]["curve"]
        expected = (
            sample["length_change"].to_numpy()
            - np.interp(t, blank["time"].to_numpy(), blank["length_change"].to_numpy())
            + md["sample_length"]
            * 1000.0
            * np.interp(temp, curve["temperature_c"], curve["expansion"])
        )
        # Strictly inside stage 2 (300 s hold, then the 5250 s ramp).
        inside = (t > 310.0) & (t < 300.0 + 5250.0 - 10.0)
        assert inside.sum() > 5000
        np.testing.assert_allclose(
            corrected["length_change"].to_numpy()[inside], expected[inside], atol=1e-6
        )
        # And the other columns are untouched.
        for column in ("time", "sample_temperature", "force"):
            assert np.array_equal(
                corrected[column].to_numpy(), sample[column].to_numpy()
            )

    def test_default_alignment_is_time(self) -> None:
        """Time alignment reproduces Proteus's dL/L0 far more closely than
        temperature alignment on dilatometer runs, so it is the default."""
        assert frame(DLA, run="corrected").equals(
            frame(DLA, run="corrected", dynamic_axis="time")
        )
        assert not frame(DLA, run="corrected").equals(
            frame(DLA, run="corrected", dynamic_axis="sample_temperature")
        )

    def test_corrected_column_metadata(self) -> None:
        table = read_ngb(DLA, run="corrected")
        assert get_column_baseline_status(table, "length_change") is True
        assert get_processing_history(table, "length_change") == [
            "raw",
            "baseline_corrected",
            "expansion_standard_applied",
        ]
        assert get_column_units(table, "length_change") == "µm"

    def test_end_of_run_shrinkage_is_plausible(self) -> None:
        relative = pl.from_arrow(
            normalize_to_initial_length(read_ngb(DLA, run="corrected"))
        )
        assert isinstance(relative, pl.DataFrame)
        assert -0.35 < relative["length_change"][-1] < -0.15

    def test_corrected_on_a_correction_file_raises(self) -> None:
        with pytest.raises(ValueError, match="no embedded correction run"):
            read_ngb(CLA, run="corrected")

    def test_another_dla_as_baseline_uses_its_blank_correction(self) -> None:
        """A .ngb-dla passed as baseline contributes its embedded (blank)
        correction; the sample's own length must not be mistaken for a
        reference-sample length."""
        corrected = frame(DLA, baseline_file=DLA_2)
        assert corrected.height == frame(DLA).height

    def test_reference_sample_corrections_are_refused(self) -> None:
        md = read_ngb_metadata(DLA)
        df = frame(DLA)
        with pytest.raises(ValueError, match="reference sample"):
            apply_expansion_standard(df, md, {"sample_length": 12.0})

    def test_missing_length_or_curve_is_refused(self) -> None:
        md = dict(read_ngb_metadata(DLA))
        df = frame(DLA)
        without_length = {k: v for k, v in md.items() if k != "sample_length"}
        with pytest.raises(ValueError, match="sample_length"):
            apply_expansion_standard(df, without_length)  # type: ignore[arg-type]
        without_curve = {k: v for k, v in md.items() if k != "expansion_standard"}
        with pytest.raises(ValueError, match="expansion_standard"):
            apply_expansion_standard(df, without_curve)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="sample_temperature"):
            apply_expansion_standard(df.drop("sample_temperature"), md)

    def test_sta_files_are_unaffected(self) -> None:
        bs3 = FIXTURE_DIR / "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
        table = read_ngb(SS3, baseline_file=bs3)
        assert get_processing_history(table, "mass") == ["raw", "baseline_corrected"]
        assert "length_change" not in table.column_names


class TestNormalizeToInitialLength:
    def test_relative_length_change(self) -> None:
        table = read_ngb(DLA, run="corrected")
        relative = normalize_to_initial_length(table)
        before = pl.from_arrow(table)["length_change"].to_numpy()
        after = pl.from_arrow(relative)["length_change"].to_numpy()
        np.testing.assert_allclose(after, before / (15.99 * 1000.0))
        assert get_column_units(relative, "length_change") == "µm/µm"
        assert get_processing_history(relative, "length_change")[-1] == "normalized"
        # The source table is untouched.
        assert get_column_units(table, "length_change") == "µm"

    def test_requires_sample_length(self) -> None:
        with pytest.raises(ValueError, match="sample_length"):
            normalize_to_initial_length(read_ngb(SS3))
        with pytest.raises(ValueError, match="sample_length"):
            normalize_to_initial_length(read_ngb(CLA))

    def test_mass_normalization_needs_a_mass(self) -> None:
        with pytest.raises(ValueError, match="sample_mass"):
            normalize_to_initial_mass(read_ngb(DLA))

    def test_explicit_missing_column(self) -> None:
        with pytest.raises(KeyError):
            normalize_to_initial_length(read_ngb(DLA), columns=["mass"])


class TestCLI:
    def test_convert_corrected(self, tmp_path: Path) -> None:
        assert (
            cli_main(["convert", str(DLA), "--run", "corrected", "-o", str(tmp_path)])
            == 0
        )
        out = tmp_path / f"{DLA.stem}_corrected.parquet"
        assert out.exists()
        import pyarrow.parquet as pq

        table = pq.read_table(out)
        assert table.schema.metadata[b"type"] == b"DIL"
        assert "length_change" in table.column_names

    def test_dil_extensions_raise_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from pyngb.api.cli import validate_baseline_file, validate_input_file

        with caplog.at_level("WARNING"):
            validate_input_file(DLA)
            validate_baseline_file(CLA)
        assert "may not be a standard NGB format" not in caplog.text

    def test_validate_reports_on_a_dilatometer_file(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        cli_main(["validate", str(DLA)])
        out = capsys.readouterr().out
        assert "NGB Data Validation Report" in out
        assert "Overall Status: VALID" in out


def _load_export(path: Path) -> tuple[dict[str, str], np.ndarray, list[str]]:
    """Proteus 'ExpDat' CSV: latin-1, '#KEY: ,value' header, '##' column line."""
    header: dict[str, str] = {}
    columns: list[str] = []
    rows: list[list[float]] = []
    for line in path.read_text(encoding="latin-1").splitlines():
        if line.startswith("##"):
            columns = [c.strip() for c in line[2:].split(",")]
        elif line.startswith("#"):
            key, _, value = line[1:].partition(",")
            header[key.strip().rstrip(":").strip()] = value.strip()
        elif line.strip():
            rows.append([float(x) for x in line.split(",")])
    return header, np.array(rows), columns


CSV_DIR = os.environ.get("PYNGB_DIL_CSV_DIR")


@pytest.mark.skipif(
    not CSV_DIR or not Path(CSV_DIR).is_dir(),
    reason="PYNGB_DIL_CSV_DIR (Proteus CSV exports of the fixtures) not set",
)
class TestAgainstProteusExport:
    """The corrected dL/L0 reproduces Proteus's own export of the heating
    ramp (segment 2, resampled at 1 °C) to a few 1e-6."""

    @pytest.mark.parametrize(
        "export", sorted(Path(CSV_DIR or ".").glob("ExpDat_*.csv"))
    )
    def test_corrected_relative_length_change(self, export: Path) -> None:
        header, rows, columns = _load_export(export)
        fixture = FIXTURE_DIR / header["FILE"]
        if not fixture.exists():
            pytest.skip(f"{header['FILE']} is not a committed fixture")
        assert float(header["SAMPLE LENGTH /mm"]) == pytest.approx(
            read_ngb_metadata(fixture)["sample_length"]
        )
        time_s = rows[:, columns.index("Time/min")] * 60.0
        exported = rows[:, columns.index("dL/Lo")]
        for axis, tolerance in (
            (None, 1e-5),
            ("time", 1e-5),
            ("sample_temperature", 1e-4),
        ):
            table = normalize_to_initial_length(
                read_ngb(fixture, run="corrected", dynamic_axis=axis)
            )
            df = pl.from_arrow(table)
            assert isinstance(df, pl.DataFrame)
            ours = np.interp(
                time_s, df["time"].to_numpy(), df["length_change"].to_numpy()
            )
            assert np.abs(exported - ours).max() < tolerance
        force = rows[:, columns.index("Force/N")]
        raw = frame(fixture)
        ours_force = np.interp(time_s, raw["time"].to_numpy(), raw["force"].to_numpy())
        assert np.abs(force - ours_force).max() < 5e-4
