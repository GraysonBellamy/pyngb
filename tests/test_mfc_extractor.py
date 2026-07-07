"""Tests for MFC (Mass Flow Controller) metadata extraction.

The MFC range value is anchored on the full record layout
(``TEMP_PROG_TYPE_PREFIX + <0x1048 u16> + FIELD_VALUE_BRIDGE_F32 + <f32>``),
the same structure the PID extractor scans for. These tests pin the extracted
gas/range values for every shipped fixture and exercise the anchoring against
synthetic tables.
"""

import struct
import zipfile
from pathlib import Path

import pytest

from pyngb.binary import BinaryParser
from pyngb.constants import (
    FIELD_VALUE_BRIDGE_F32,
    TEMP_PROG_TYPE_PREFIX,
    PatternConfig,
    PatternOffsets,
)
from pyngb.extractors.base import StreamTables
from pyngb.extractors.specialized import MFCExtractor

TEST_DIR = Path(__file__).parent / "test_files"

# Every shipped fixture was recorded on the same instrument configuration:
# purge 1 = N2 (250 ml/min full scale), purge 2 = O2 (252.5), protective = N2 (250).
EXPECTED_MFC = {
    "purge_1_mfc_gas": "NITROGEN",
    "purge_1_mfc_range": 250.0,
    "purge_2_mfc_gas": "OXYGEN",
    "purge_2_mfc_range": 252.5,
    "protective_mfc_gas": "NITROGEN",
    "protective_mfc_range": 250.0,
}

MFC_SIGNATURE = TEMP_PROG_TYPE_PREFIX + struct.pack(
    "<H", PatternOffsets().MFC_SIGNATURE
)
MFC_RANGE_RECORD = MFC_SIGNATURE + FIELD_VALUE_BRIDGE_F32


def _extractor() -> MFCExtractor:
    return MFCExtractor(PatternConfig(), BinaryParser())


def _stream1_tables(path: Path) -> StreamTables:
    parser = BinaryParser()
    with zipfile.ZipFile(path) as z:
        return StreamTables(parser.split_tables(z.read("Streams/stream_1.table")))


@pytest.mark.parametrize(
    "fixture_name",
    sorted(p.name for p in TEST_DIR.glob("*.ngb-*s3")),
)
def test_fixture_mfc_metadata(fixture_name: str) -> None:
    """Every fixture yields the exact known gas types and range values."""
    tables = _stream1_tables(TEST_DIR / fixture_name)
    extractor = _extractor()

    assert extractor.can_extract(tables)

    metadata: dict = {}
    extractor.extract(tables, metadata)

    for key, expected in EXPECTED_MFC.items():
        assert metadata.get(key) == expected, f"{fixture_name}: {key}"


def test_range_value_anchored_on_record() -> None:
    """The value is read from the anchored record position, not a nearby float."""
    # A plausible flow-rate float *before* the record must not win.
    decoy = struct.pack("<f", 42.0)
    value = struct.pack("<f", 250.0)
    table = b"\x00" * 8 + decoy + MFC_RANGE_RECORD + value + b"\x00" * 8

    assert _extractor()._extract_mfc_range_value(table) == 250.0


def test_signature_without_bridge_yields_nothing() -> None:
    """A bare signature with no value record is not treated as a range table."""
    table = b"\x00" * 8 + MFC_SIGNATURE + struct.pack("<f", 250.0) + b"\x00" * 8

    assert _extractor()._extract_mfc_range_value(table) is None


def test_out_of_bounds_value_rejected() -> None:
    """Values outside the plausible flow-rate window are discarded."""
    for bad in (0.0, 1e6, float("nan")):
        table = MFC_RANGE_RECORD + struct.pack("<f", bad)
        assert _extractor()._extract_mfc_range_value(table) is None


def test_truncated_record_yields_nothing() -> None:
    """A record cut off before its value bytes is ignored."""
    table = b"\x00" * 8 + MFC_RANGE_RECORD + b"\x00\x00"  # only 2 of 4 value bytes

    assert _extractor()._extract_mfc_range_value(table) is None


# Configured flow setpoints (ml/min) read from the *_LastUsedFlow
# device-parameter tables. These vary per run, unlike the ranges.
EXPECTED_FLOWS = {
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3": (35.0, 15.0, 20.0),
    "Douglas_Fir_STA_10K_250730_R13.ngb-ss3": (50.0, 20.0, 20.0),
    "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3": (50.0, 20.0, 20.0),
    "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3": (50.0, 20.0, 20.0),
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3": (50.0, 14.0, 20.0),
    "Red_Oak_STA_10K_250731_R7.ngb-ss3": (50.0, 20.0, 20.0),
}


@pytest.mark.parametrize(
    "fixture_name",
    sorted(p.name for p in TEST_DIR.glob("*.ngb-*s3")),
)
def test_fixture_flow_setpoints(fixture_name: str) -> None:
    """Every fixture yields the configured MFC flow setpoints."""
    tables = _stream1_tables(TEST_DIR / fixture_name)
    metadata: dict = {}
    _extractor().extract(tables, metadata)

    expected = EXPECTED_FLOWS.get(fixture_name)
    if expected is None:
        pytest.skip(f"no pinned flows for {fixture_name}")
    purge_1, purge_2, protective = expected
    assert metadata["purge_1_mfc_flow"] == purge_1
    assert metadata["purge_2_mfc_flow"] == purge_2
    assert metadata["protective_mfc_flow"] == protective


def test_flow_setpoint_absent_yields_no_field() -> None:
    """Tables without *_LastUsedFlow parameters produce no flow fields."""
    tables = StreamTables([b"\x30\x75" + b"\x00" * 64])
    metadata: dict = {}
    _extractor()._extract_flow_setpoints(tables, metadata)
    assert not any(k.endswith("_mfc_flow") for k in metadata)
