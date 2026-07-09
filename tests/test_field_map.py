"""Integrity contracts for the declarative format maps.

These pin the semantic constants (channel names, calibration ids, metadata
key spelling) so an accidental edit to maps.py fails here before it shows up
as a golden diff.
"""

from pyngb.constants import FileMetadata
from pyngb.format.maps import (
    CAL_CONSTANTS,
    CHANNEL_MAP,
    DATA_FIELDS,
    FIELD_MAP,
    FIXPOINT_FIELDS,
    KNOWN_FIELD_IDS,
    MFC_FLOW_PARAM_NAMES,
    PID_FIELDS,
    STAGE_FIELDS,
    channel_name,
)
from pyngb.format.grammar import DType


class TestFieldMap:
    def test_keys_are_file_metadata_keys(self) -> None:
        valid = set(FileMetadata.__annotations__)
        assert {meta.key for meta in FIELD_MAP} <= valid
        assert set(MFC_FLOW_PARAM_NAMES) <= valid

    def test_keys_are_unique(self) -> None:
        keys = [meta.key for meta in FIELD_MAP]
        assert len(keys) == len(set(keys))

    def test_ids_are_u16(self) -> None:
        for meta in FIELD_MAP:
            assert 0 <= meta.category <= 0xFFFF
            assert 0 <= meta.field_id <= 0xFFFF

    def test_converts_shape_values(self) -> None:
        by_key = {meta.key: meta for meta in FIELD_MAP}
        assert by_key["date_performed"].convert(1_600_000_000) == (
            "2020-09-13T12:26:40+00:00"
        )
        assert by_key["date_performed"].convert("not a timestamp") is None
        assert by_key["sample_mass"].convert(5.25) == 5.25
        assert by_key["sample_mass"].convert(-1000.0) is None
        assert by_key["sample_mass"].convert(0.0) is None
        assert by_key["operator"].convert("  padded  ") == "padded"
        assert by_key["operator"].convert("   ") is None


class TestChannelMap:
    def test_stream_2_channels(self) -> None:
        assert CHANNEL_MAP[0x8C] == "time"
        assert CHANNEL_MAP[0x8D] == "sample_temperature"
        assert CHANNEL_MAP[0x8E] == "dsc_signal"
        assert CHANNEL_MAP[0x90] == "mass"
        assert CHANNEL_MAP[0x9C] == "purge_flow_1"
        assert CHANNEL_MAP[0x9D] == "purge_flow_2"
        assert CHANNEL_MAP[0x9E] == "protective_flow"

    def test_stream_3_channels(self) -> None:
        assert CHANNEL_MAP[0x30] == "furnace_temperature"
        assert CHANNEL_MAP[0x32] == "furnace_power"
        assert CHANNEL_MAP[0x33] == "h_foil_temperature"
        assert CHANNEL_MAP[0x34] == "uc_module"
        assert CHANNEL_MAP[0x35] == "environmental_pressure"
        assert CHANNEL_MAP[0x36] == "environmental_acceleration_x"
        assert CHANNEL_MAP[0x37] == "environmental_acceleration_y"
        assert CHANNEL_MAP[0x38] == "environmental_acceleration_z"

    def test_unmapped_ids_pass_through_as_hex(self) -> None:
        assert channel_name(0x7531) == "31"
        assert channel_name(0x1787) == "87"  # data-less trailer, unmapped
        assert 0x87 not in CHANNEL_MAP

    def test_channel_name_uses_low_byte(self) -> None:
        assert channel_name(0x178C) == "time"
        assert channel_name(0x758D) == "sample_temperature"


class TestNamedIds:
    def test_calibration_constants_p0_to_p5(self) -> None:
        assert CAL_CONSTANTS == {
            "p0": 0x044F,
            "p1": 0x0450,
            "p2": 0x0451,
            "p3": 0x0452,
            "p4": 0x0453,
            "p5": 0x04C3,
        }

    def test_pid_and_stage_fields(self) -> None:
        assert list(PID_FIELDS) == ["xp", "tn", "tv"]
        assert set(STAGE_FIELDS) == {
            "stage_type",
            "temperature",
            "heating_rate",
            "acquisition_rate",
            "time",
        }

    def test_fixpoint_fields_are_the_proteus_columns(self) -> None:
        assert list(FIXPOINT_FIELDS) == [
            "name",
            "actual_c",
            "measured_c",
            "weight",
            "corrected_c",
        ]

    def test_data_fields_cover_both_widths(self) -> None:
        expected = {(0x0F40, DType.F64), (0x0F3D, DType.F32)}
        assert expected == DATA_FIELDS

    def test_known_field_ids_cover_every_map(self) -> None:
        assert {meta.field_id for meta in FIELD_MAP} <= KNOWN_FIELD_IDS
        assert set(PID_FIELDS.values()) <= KNOWN_FIELD_IDS
        assert set(STAGE_FIELDS.values()) <= KNOWN_FIELD_IDS
        assert set(CAL_CONSTANTS.values()) <= KNOWN_FIELD_IDS
        assert set(FIXPOINT_FIELDS.values()) <= KNOWN_FIELD_IDS
        assert {fid for fid, _ in DATA_FIELDS} <= KNOWN_FIELD_IDS
