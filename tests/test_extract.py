"""Metadata extraction over the document model.

The decisive check is golden parity: on every fixture, the NEW stack's
``build_metadata`` must reproduce the C0 parity golden (captured from the
legacy regex backbone) exactly — zero tolerances, minus only ``file_hash``,
which the API loaders add from the file rather than the document.

The synthetic halves pin each extraction rule in isolation on the builder,
including the legacy quirks the goldens depend on (neighbor-table crucible
classification, DST offset arithmetic) and the structural rules that
replaced the legacy heuristics (category-ordinal stage keying, the MFC
device tree, per-stage flow states).
"""

import json
from pathlib import Path

import numpy as np
import pytest

from pyngb.format import DType, build_metadata, load_document
from support.ngb_builder import (
    build_array,
    build_scalar,
    build_section,
    build_stream,
    build_table,
    write_ngb,
)

FIXTURE_DIR = Path(__file__).parent / "test_files"
GOLDEN_DIR = Path(__file__).parent / "goldens"
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*")) if FIXTURE_DIR.exists() else []


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_build_metadata_matches_the_parity_golden(fixture: Path) -> None:
    """The new stack reproduces the legacy metadata bit-for-bit."""
    golden_path = GOLDEN_DIR / f"{fixture.name}.parity.json"
    assert golden_path.exists(), f"missing parity golden for {fixture.name}"
    golden = json.loads(golden_path.read_text(encoding="utf-8"))["metadata"]
    golden.pop("file_hash")  # added by the API loaders, not the extraction

    metadata = build_metadata(load_document(fixture, streams=[1]))
    # Snapshot comparison happens on JSON round-tripped objects, exactly as
    # the golden was written: float round-trip through JSON is exact.
    assert json.loads(json.dumps(dict(metadata), ensure_ascii=False)) == golden


def doc_of(tmp_path: Path, tables: list[bytes]):
    path = write_ngb(
        tmp_path / "synth.ngb-ss3", {1: build_stream(1, body=build_section(tables))}
    )
    return load_document(path)


def opener(records: list[bytes] | None = None, category: int = 0x0323) -> bytes:
    """A leading class-definition table, as every real stream has."""
    return build_table(category, records or [], class_def=True)


class TestMasses:
    def sample_neighbor(self) -> bytes:
        # A table whose trailing field is the f32 0x0C83 (sample side).
        return build_table(
            0x7530,
            [
                build_scalar(0x0C9E, DType.F64, -1000.0),
                build_scalar(0x0C83, DType.F32, -1000.0),
            ],
        )

    def ref_neighbor(self) -> bytes:
        # Reference side: trailing u16 0x10C4 after the reference mass.
        return build_table(
            0x1774,
            [
                build_scalar(0x0C9E, DType.F64, 0.125),
                build_scalar(0x10C4, DType.U16, 0),
            ],
        )

    def crucible(self, mass: float) -> bytes:
        return build_table(0x177E, [build_scalar(0x0C9E, DType.F64, mass)])

    def test_classification_by_neighbor_tables(self, tmp_path: Path) -> None:
        # Reference first in stream order, as in real files.
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.ref_neighbor(),
                self.crucible(256.298),
                self.sample_neighbor(),
                self.crucible(253.516),
            ],
        )
        metadata = build_metadata(doc)
        assert metadata["crucible_mass"] == 253.516
        assert metadata["reference_crucible_mass"] == 256.298
        # reference_mass = last numeric of the table preceding the reference
        # crucible table (u16 fields don't count as numeric).
        assert metadata["reference_mass"] == 0.125

    def test_sample_mass_fallback_has_no_positivity_check(self, tmp_path: Path) -> None:
        """Baseline files: the -1000.0 sentinel comes from the neighbor walk."""
        doc = doc_of(
            tmp_path, [opener(), self.sample_neighbor(), self.crucible(253.516)]
        )
        metadata = build_metadata(doc)
        assert metadata["crucible_mass"] == 253.516
        # The field-map value (-1000.0 in field 0x0C9E) is rejected as
        # non-positive, then the structural fallback accepts the trailing
        # -1000.0 of the neighbor table.
        assert metadata["sample_mass"] == -1000.0

    def test_zero_occurrence_stands_in_for_the_reference(self, tmp_path: Path) -> None:
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.sample_neighbor(),
                self.crucible(253.516),
                self.crucible(0.0),  # unclassified, zero
            ],
        )
        metadata = build_metadata(doc)
        assert metadata["crucible_mass"] == 253.516
        assert metadata["reference_crucible_mass"] == 0.0

    def test_first_occurrence_fallback_when_unclassified(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), self.crucible(7.5), self.crucible(9.5)])
        metadata = build_metadata(doc)
        assert metadata["crucible_mass"] == 7.5
        assert "reference_crucible_mass" not in metadata


def stage_table(
    ordinal: int,
    temperature: float,
    minutes: float,
    *,
    stage_type: int = 1,
) -> bytes:
    """A stage table; the category encodes the program ordinal."""
    return build_table(
        0x7530 + ordinal,
        [
            build_scalar(0x083F, DType.I32, stage_type),  # stage_type is i32
            build_scalar(0x0E17, DType.F32, temperature),
            build_scalar(0x0E13, DType.F32, 10.0),
            build_scalar(0x0E14, DType.F32, 100.0),
            build_scalar(0x0E15, DType.F32, minutes),
        ],
        type_ref=0x2B0C,
    )


class TestTemperatureProgram:
    def test_stage_keys_and_times_in_seconds(self, tmp_path: Path) -> None:
        doc = doc_of(
            tmp_path,
            [opener(), stage_table(0, 25.0, 0.0), stage_table(1, 700.0, 67.5)],
        )
        program = build_metadata(doc)["temperature_program"]
        assert list(program) == ["stage_0", "stage_1"]
        assert program["stage_1"]["temperature"] == 700.0
        assert program["stage_1"]["time"] == 67.5 * 60.0

    def test_stage_keys_follow_the_category_not_stream_order(
        self, tmp_path: Path
    ) -> None:
        """Edited programs serialize out of order (FILED fixtures: 0,2,3,4,1);
        the category ordinal, verified against the recorded data, wins."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                stage_table(0, 25.0, 0.0, stage_type=0),
                stage_table(2, 700.0, 67.5),
                stage_table(1, 25.0, 20.0),
            ],
        )
        program = build_metadata(doc)["temperature_program"]
        assert list(program) == ["stage_0", "stage_1", "stage_2"]
        assert program["stage_1"]["temperature"] == 25.0
        assert program["stage_1"]["time"] == 20.0 * 60.0
        assert program["stage_2"]["temperature"] == 700.0

    def test_stage_type_is_extracted_as_int(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), stage_table(0, 25.0, 0.0, stage_type=2)])
        program = build_metadata(doc)["temperature_program"]
        assert program["stage_0"]["stage_type"] == 2
        assert set(program["stage_0"]) == {
            "stage_type",
            "temperature",
            "heating_rate",
            "acquisition_rate",
            "time",
        }

    def test_table_missing_a_stage_field_is_not_a_stage(self, tmp_path: Path) -> None:
        partial = build_table(0x7530, [build_scalar(0x0E17, DType.F32, 500.0)])
        doc = doc_of(tmp_path, [opener(), partial])
        assert "temperature_program" not in build_metadata(doc)

    def test_duplicate_stage_ordinal_keeps_the_first(self, tmp_path: Path) -> None:
        doc = doc_of(
            tmp_path,
            [opener(), stage_table(0, 25.0, 5.0), stage_table(0, 900.0, 1.0)],
        )
        program = build_metadata(doc)["temperature_program"]
        assert list(program) == ["stage_0"]
        assert program["stage_0"]["temperature"] == 25.0

    def test_non_stage_category_is_ignored(self, tmp_path: Path) -> None:
        """A stage-typed table with all five fields but a category below the
        stage base cannot be assigned an ordinal and is dropped."""
        rogue = build_table(
            0x1789,
            [
                build_scalar(0x083F, DType.I32, 1),
                build_scalar(0x0E17, DType.F32, 500.0),
                build_scalar(0x0E13, DType.F32, 10.0),
                build_scalar(0x0E14, DType.F32, 100.0),
                build_scalar(0x0E15, DType.F32, 5.0),
            ],
            type_ref=0x2B0C,
        )
        doc = doc_of(tmp_path, [opener(), rogue])
        assert "temperature_program" not in build_metadata(doc)

    def test_non_stage_type_ref_is_ignored(self, tmp_path: Path) -> None:
        """The five stage field ids on a non-stage-typed table (0x083F is
        the device id elsewhere) do not make it a stage."""
        rogue = build_table(
            0x7530,
            [
                build_scalar(0x083F, DType.I32, 1),
                build_scalar(0x0E17, DType.F32, 500.0),
                build_scalar(0x0E13, DType.F32, 10.0),
                build_scalar(0x0E14, DType.F32, 100.0),
                build_scalar(0x0E15, DType.F32, 5.0),
            ],
        )
        doc = doc_of(tmp_path, [opener(), rogue])
        assert "temperature_program" not in build_metadata(doc)


class TestPID:
    def pid_table(self, xp: float, tn: float, tv: float) -> bytes:
        return build_table(
            0x1788,
            [
                build_scalar(0x0FE7, DType.F32, xp),
                build_scalar(0x0FE8, DType.F32, tn),
                build_scalar(0x0FE9, DType.F32, tv),
            ],
        )

    def test_first_is_furnace_second_is_sample(self, tmp_path: Path) -> None:
        doc = doc_of(
            tmp_path,
            [opener(), self.pid_table(6.0, 8.0, 2.0), self.pid_table(5.0, 60.0, 15.0)],
        )
        metadata = build_metadata(doc)
        assert metadata["furnace_xp"] == 6.0
        assert metadata["furnace_tn"] == 8.0
        assert metadata["furnace_tv"] == 2.0
        assert metadata["sample_xp"] == 5.0
        assert metadata["sample_tn"] == 60.0
        assert metadata["sample_tv"] == 15.0

    def test_single_occurrence_sets_only_furnace(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), self.pid_table(6.0, 8.0, 2.0)])
        metadata = build_metadata(doc)
        assert metadata["furnace_xp"] == 6.0
        assert "sample_xp" not in metadata


GUID_N2 = "292a050d-2f6c-490e-8385-7c743538a5a3"
GUID_O2 = "51324bb0-7cf9-46e4-bb5a-0d4f9bf06cf9"


class TestMFC:
    def device_def(
        self,
        category: int,
        device_id: int,
        gas: str,
        guid: str,
        *,
        kind: int = 2,
    ) -> bytes:
        return build_table(
            category,
            [
                build_scalar(0x083F, DType.I32, device_id),
                build_scalar(0x0840, DType.STRING, gas),
                build_scalar(0x0C8F, DType.STRING, guid),
                build_scalar(0x104B, DType.I32, kind),
            ],
            type_ref=0x2B07,
        )

    def range_table(self, full_scale: float) -> bytes:
        return build_table(
            0x1780, [build_scalar(0x1048, DType.F32, full_scale)], type_ref=0x2B0A
        )

    def gas_record(self, gas: str, formula: str, guid: str) -> bytes:
        return build_table(
            0x1BE4,
            [
                build_scalar(0x17FC, DType.STRING, guid),
                build_scalar(0x0840, DType.STRING, gas),
                build_scalar(0x0C88, DType.STRING, formula),
            ],
            type_ref=0x2B81,
        )

    def state(self, category: int, device_id: int, flow: float) -> list[bytes]:
        """A per-stage device-state pair: state table + its range table."""
        return [
            build_table(
                category,
                [build_scalar(0x083F, DType.I32, device_id)],
                type_ref=0x2B11,
            ),
            build_table(
                0x1780,
                [
                    build_scalar(0x1047, DType.F32, flow),
                    build_scalar(0x1048, DType.F32, 250.0),
                ],
                type_ref=0x2B0A,
            ),
        ]

    def full_tree(self) -> list[bytes]:
        return [
            self.device_def(0x1BAC, 30, "NITROGEN", GUID_N2),
            self.range_table(250.0),
            self.gas_record("NITROGEN", "N2", GUID_N2),
            self.device_def(0x1BAD, 31, "OXYGEN", GUID_O2),
            self.range_table(252.5),
            self.gas_record("OXYGEN", "O2", GUID_O2),
            self.device_def(0x1BAE, 32, "NITROGEN", GUID_N2),
            self.range_table(250.0),
            self.gas_record("NITROGEN", "N2", GUID_N2),
        ]

    def test_device_tree_yields_gas_formula_and_range(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), *self.full_tree()])
        metadata = build_metadata(doc)
        assert metadata["purge_1_mfc_gas"] == "NITROGEN"
        assert metadata["purge_1_mfc_gas_formula"] == "N2"
        assert metadata["purge_1_mfc_range"] == 250.0
        assert metadata["purge_2_mfc_gas"] == "OXYGEN"
        assert metadata["purge_2_mfc_gas_formula"] == "O2"
        assert metadata["purge_2_mfc_range"] == 252.5
        assert metadata["protective_mfc_gas"] == "NITROGEN"
        assert metadata["protective_mfc_gas_formula"] == "N2"
        assert metadata["protective_mfc_range"] == 250.0

    def test_non_mfc_device_kinds_are_ignored(self, tmp_path: Path) -> None:
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.device_def(0x1BB4, 37, "NITROGEN", GUID_N2, kind=10),
                self.range_table(250.0),
            ],
        )
        metadata = build_metadata(doc)
        assert not any("mfc" in key for key in metadata)

    def test_unmapped_mfc_device_id_is_skipped_loudly(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A fourth MFC (the real-Purge-3 trigger) must warn, never map."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.device_def(0x1BAF, 33, "ARGON", "some-guid"),
                self.range_table(100.0),
                self.device_def(0x1BAC, 30, "NITROGEN", GUID_N2),
                self.range_table(250.0),
            ],
        )
        with caplog.at_level("WARNING", logger="pyngb.format.extract"):
            metadata = build_metadata(doc)
        assert any("33" in record.message for record in caplog.records)
        assert "purge_1_mfc_gas" in metadata
        assert not any(v == "ARGON" for v in metadata.values())

    def test_calibration_context_gas_records_never_leak(self, tmp_path: Path) -> None:
        """Gas records outside the definition block (calibration contexts)
        and GUID-mismatched records are ignored."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.gas_record("HELIUM", "He", "cal-context-guid"),
                self.device_def(0x1BAC, 30, "NITROGEN", GUID_N2),
                self.range_table(250.0),
                self.gas_record("OXYGEN", "O2", GUID_O2),  # mismatched GUID
            ],
        )
        metadata = build_metadata(doc)
        assert metadata["purge_1_mfc_gas"] == "NITROGEN"
        assert "purge_1_mfc_gas_formula" not in metadata

    def test_run_flow_from_uniform_body_stages(self, tmp_path: Path) -> None:
        """The initial stage's gas-off state must not defeat uniformity."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                *self.full_tree(),
                stage_table(0, 25.0, 0.0, stage_type=0),
                *self.state(0x1BAC, 30, 0.0),
                *self.state(0x1BAD, 31, 0.0),
                stage_table(1, 25.0, 20.0),
                *self.state(0x1BAC, 30, 35.0),
                *self.state(0x1BAD, 31, 15.0),
                stage_table(2, 700.0, 67.5),
                *self.state(0x1BAC, 30, 35.0),
                *self.state(0x1BAD, 31, 15.0),
            ],
        )
        metadata = build_metadata(doc)
        assert metadata["purge_1_mfc_flow"] == 35.0
        assert metadata["purge_2_mfc_flow"] == 15.0
        assert "protective_mfc_flow" not in metadata  # no states for it
        program = metadata["temperature_program"]
        assert program["stage_0"]["purge_1_mfc_flow"] == 0.0
        assert program["stage_1"]["purge_1_mfc_flow"] == 35.0
        assert program["stage_2"]["purge_2_mfc_flow"] == 15.0

    def test_varying_body_flows_omit_the_scalar_key(self, tmp_path: Path) -> None:
        """Gas-switching programs keep per-stage flows only."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                *self.full_tree(),
                stage_table(0, 25.0, 20.0),
                *self.state(0x1BAC, 30, 35.0),
                stage_table(1, 700.0, 67.5),
                *self.state(0x1BAC, 30, 20.0),
            ],
        )
        metadata = build_metadata(doc)
        assert "purge_1_mfc_flow" not in metadata
        program = metadata["temperature_program"]
        assert program["stage_0"]["purge_1_mfc_flow"] == 35.0
        assert program["stage_1"]["purge_1_mfc_flow"] == 20.0

    def test_body_stage_without_a_snapshot_suppresses_the_scalar(
        self, tmp_path: Path
    ) -> None:
        """A truncated/missing state group must not let partial coverage
        masquerade as a uniform run-level flow."""
        doc = doc_of(
            tmp_path,
            [
                opener(),
                *self.full_tree(),
                stage_table(1, 25.0, 20.0),
                *self.state(0x1BAC, 30, 35.0),
                stage_table(2, 700.0, 67.5),  # no state group parsed
            ],
        )
        metadata = build_metadata(doc)
        assert "purge_1_mfc_flow" not in metadata
        assert metadata["temperature_program"]["stage_1"]["purge_1_mfc_flow"] == 35.0

    def test_guidless_definition_takes_no_formula(self, tmp_path: Path) -> None:
        """Without a definition GUID there is nothing to match against —
        a GUID-less gas record must not pair by None == None."""
        definition = build_table(
            0x1BAC,
            [
                build_scalar(0x083F, DType.I32, 30),
                build_scalar(0x0840, DType.STRING, "NITROGEN"),
                build_scalar(0x104B, DType.I32, 2),
            ],
            type_ref=0x2B07,
        )
        guidless_record = build_table(
            0x1BE4,
            [
                build_scalar(0x0840, DType.STRING, "HELIUM"),
                build_scalar(0x0C88, DType.STRING, "He"),
            ],
            type_ref=0x2B81,
        )
        doc = doc_of(tmp_path, [opener(), definition, guidless_record])
        metadata = build_metadata(doc)
        assert metadata["purge_1_mfc_gas"] == "NITROGEN"
        assert "purge_1_mfc_gas_formula" not in metadata

    def test_range_requires_the_range_table_type(self, tmp_path: Path) -> None:
        """Field 0x1048 on a non-range-type follower is not a range."""
        rogue = build_table(0x1780, [build_scalar(0x1048, DType.F32, 99.0)])
        doc = doc_of(
            tmp_path,
            [
                opener(),
                self.device_def(0x1BAC, 30, "NITROGEN", GUID_N2),
                rogue,
                self.range_table(250.0),
            ],
        )
        assert build_metadata(doc)["purge_1_mfc_range"] == 250.0


class TestCalibration:
    def test_constants_from_first_yielding_table(self, tmp_path: Path) -> None:
        empty = build_table(0x01F5, [build_scalar(0x0999, DType.U16, 1)])
        full = build_table(
            0x01F5,
            [
                build_scalar(0x044F, DType.F64, 1.5),
                build_scalar(0x04C3, DType.F64, -0.25),
            ],
        )
        doc = doc_of(tmp_path, [opener(), empty, full])
        constants = build_metadata(doc)["calibration_constants"]
        assert constants == {"p0": 1.5, "p5": -0.25}

    def test_temperature_calibration_block(self, tmp_path: Path) -> None:
        coefficients = np.array([0.1, -0.2, 0.3], dtype="<f4")
        coeff_table = build_table(
            0x01F7, [build_array(0x04BE, DType.U8, coefficients.tobytes())]
        )
        fixpoint = build_table(
            0x7531,
            [
                build_scalar(0x0443, DType.STRING, "Indium"),
                build_scalar(0x0444, DType.F32, 156.6),
                build_scalar(0x0445, DType.F32, 156.1),
                build_scalar(0x0446, DType.F32, 1.0),
                build_scalar(0x0447, DType.F32, 156.5),
            ],
        )
        source = build_table(
            0x01F5,
            [
                build_scalar(0x07D4, DType.STRING, "C:\\cal\\tcal.ngb-ts3"),
                build_scalar(0x083E, DType.I32, 1_600_000_000),
                build_scalar(0x0431, DType.STRING, "NITROGEN"),
                build_scalar(0x0433, DType.STRING, "DSC/TG pan"),
                build_scalar(0x0435, DType.F32, 10.0),
            ],
        )
        doc = doc_of(tmp_path, [opener(), coeff_table, fixpoint, source])
        cal = build_metadata(doc)["temperature_calibration"]
        assert cal["coefficients"] == [
            float(np.float32(0.1)),
            float(np.float32(-0.2)),
            float(np.float32(0.3)),
        ]
        assert cal["record_path"] == "C:\\cal\\tcal.ngb-ts3"
        assert cal["date_measured"] == "2020-09-13T12:26:40+00:00"
        assert cal["gas"] == "NITROGEN"
        assert cal["crucible_type"] == "DSC/TG pan"
        assert cal["heating_rate"] == 10.0
        row = cal["fixpoints"][0]
        assert row["name"] == "Indium"
        assert row["actual_c"] == float(np.float32(156.6))
        assert row["corrected_c"] == float(np.float32(156.5))

    def test_sensitivity_calibration_from_es3_table(self, tmp_path: Path) -> None:
        source = build_table(
            0x01F5,
            [
                build_scalar(0x07D4, DType.STRING, "C:\\cal\\sens.ngb-es3"),
                build_scalar(0x0431, DType.STRING, "ARGON"),
            ],
        )
        doc = doc_of(tmp_path, [opener(), source])
        sensitivity = build_metadata(doc)["sensitivity_calibration"]
        assert sensitivity["record_path"] == "C:\\cal\\sens.ngb-es3"
        assert sensitivity["gas"] == "ARGON"


class TestRunEnvironment:
    def tz_table(self, state: int) -> bytes:
        return build_table(
            0x1859,
            [
                build_scalar(0x1135, DType.STRING, "Eastern Daylight Time"),
                build_scalar(0x1134, DType.I32, 300),
                build_scalar(0x1137, DType.I32, -60),
                build_scalar(0x1138, DType.I32, state),
            ],
        )

    def test_daylight_offset_arithmetic(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), self.tz_table(state=2)])
        metadata = build_metadata(doc)
        assert metadata["timezone"] == "Eastern Daylight Time"
        assert metadata["utc_offset_minutes"] == -240  # -(300) - (-60)

    def test_standard_time_ignores_dst_bias(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener(), self.tz_table(state=1)])
        assert build_metadata(doc)["utc_offset_minutes"] == -300

    def test_correction_link(self, tmp_path: Path) -> None:
        link = build_table(
            0x1770, [build_scalar(0x0843, DType.STRING, "D:\\runs\\base.ngb-bs3")]
        )
        doc = doc_of(tmp_path, [opener(), link])
        assert build_metadata(doc)["correction_file_path"] == "D:\\runs\\base.ngb-bs3"


class TestAppLicense:
    def test_version_and_license_selection(self, tmp_path: Path) -> None:
        table = build_table(
            0x0300,
            [
                build_scalar(0x0999, DType.STRING, "Version 8.0.3 (2022-06-21)"),
                build_scalar(0x0998, DType.STRING, "Some Lab\nSome University"),
                build_scalar(0x0997, DType.STRING, "short"),
            ],
        )
        doc = doc_of(tmp_path, [opener(), table])
        metadata = build_metadata(doc)
        assert metadata["application_version"] == "Version 8.0.3 (2022-06-21)"
        assert metadata["licensed_to"] == "Some Lab\nSome University"

    def test_fallback_scans_all_strings_when_category_absent(
        self, tmp_path: Path
    ) -> None:
        table = build_table(
            0x1234, [build_scalar(0x0999, DType.STRING, "Version 9.1.0 build")]
        )
        doc = doc_of(tmp_path, [opener(), table])
        assert build_metadata(doc)["application_version"] == "Version 9.1.0 build"


class TestRobustness:
    def test_empty_document_yields_empty_metadata(self, tmp_path: Path) -> None:
        doc = doc_of(tmp_path, [opener()])
        assert build_metadata(doc) == {}

    def test_extraction_never_raises_on_defective_stream_1(
        self, tmp_path: Path
    ) -> None:
        table = build_table(
            0x1772, [build_scalar(0x0834, DType.STRING, "lab")], class_def=True
        )
        body = build_section([table]) + b"\xba\xdf\x00\x0d" * 4
        path = write_ngb(tmp_path / "bad.ngb-ss3", {1: build_stream(1, body=body)})
        doc = load_document(path)
        assert doc.has_defect(1)
        assert build_metadata(doc)["lab"] == "lab"
