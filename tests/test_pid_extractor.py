"""Tests for PID control-parameter extraction.

PID records share the layout the MFC extractor anchors on
(``TEMP_PROG_TYPE_PREFIX + <signature u16> + FIELD_VALUE_BRIDGE_F32 + <f32>``).
Each signature occurs exactly twice per file: the furnace controller first,
the sample controller second. These tests pin the extracted values for every
shipped fixture and exercise the occurrence-order rule against synthetic
tables.
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
)
from pyngb.extractors.base import StreamTables
from pyngb.extractors.specialized import PIDParameterExtractor

TEST_DIR = Path(__file__).parent / "test_files"

# Two instrument configurations exist across the shipped fixtures: the 2022
# DF_FILED run used 4.0 for every parameter; the 2025 runs use xp=tn=5.25,
# tv=4.0. Furnace and sample controllers are configured identically in all.
EXPECTED_2022 = {
    "furnace_xp": 4.0,
    "furnace_tn": 4.0,
    "furnace_tv": 4.0,
    "sample_xp": 4.0,
    "sample_tn": 4.0,
    "sample_tv": 4.0,
}
EXPECTED_2025 = {
    "furnace_xp": 5.25,
    "furnace_tn": 5.25,
    "furnace_tv": 4.0,
    "sample_xp": 5.25,
    "sample_tn": 5.25,
    "sample_tv": 4.0,
}
EXPECTED_PID = {
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3": EXPECTED_2022,
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3": EXPECTED_2022,
    "Douglas_Fir_STA_10K_250730_R13.ngb-ss3": EXPECTED_2025,
    "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3": EXPECTED_2025,
    "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3": EXPECTED_2025,
    "Red_Oak_STA_10K_250731_R7.ngb-ss3": EXPECTED_2025,
}

XP_SIG, TN_SIG, TV_SIG = 0x0FE7, 0x0FE8, 0x0FE9


def _pid_record(signature: int, value: float) -> bytes:
    return (
        TEMP_PROG_TYPE_PREFIX
        + struct.pack("<H", signature)
        + FIELD_VALUE_BRIDGE_F32
        + struct.pack("<f", value)
    )


def _extractor() -> PIDParameterExtractor:
    return PIDParameterExtractor(PatternConfig(), BinaryParser())


def _stream1_tables(path: Path) -> StreamTables:
    parser = BinaryParser()
    with zipfile.ZipFile(path) as z:
        return StreamTables(parser.split_tables(z.read("Streams/stream_1.table")))


@pytest.mark.parametrize(
    ("fixture_name", "expected"),
    sorted(EXPECTED_PID.items()),
)
def test_fixture_pid_parameters(fixture_name: str, expected: dict) -> None:
    """Every fixture yields the exact known PID values for both controllers."""
    path = TEST_DIR / fixture_name
    if not path.exists():
        pytest.skip("real fixture not available")

    tables = _stream1_tables(path)
    extractor = _extractor()

    assert extractor.can_extract(tables)

    metadata: dict = {}
    extractor.extract(tables, metadata)

    assert metadata == expected, fixture_name


def test_first_occurrence_is_furnace_second_is_sample() -> None:
    """Occurrence order maps to controller: furnace first, sample second."""
    table = (
        b"\x00" * 4
        + _pid_record(XP_SIG, 7.5)
        + b"\x00" * 4
        + _pid_record(XP_SIG, 2.25)
        + b"\x00" * 4
    )

    metadata: dict = {}
    _extractor().extract(StreamTables([table]), metadata)

    assert metadata == {"furnace_xp": 7.5, "sample_xp": 2.25}


def test_single_occurrence_fills_furnace_only() -> None:
    """One record yields the furnace parameter and no sample parameter."""
    table = _pid_record(TN_SIG, 3.0)

    metadata: dict = {}
    _extractor().extract(StreamTables([table]), metadata)

    assert metadata == {"furnace_tn": 3.0}


def test_signature_without_bridge_yields_nothing() -> None:
    """A signature not followed by the f32 value bridge is not a PID record."""
    table = TEMP_PROG_TYPE_PREFIX + struct.pack("<H", TV_SIG) + struct.pack("<f", 4.0)

    extractor = _extractor()
    metadata: dict = {}
    extractor.extract(StreamTables([table]), metadata)

    assert metadata == {}


def test_truncated_record_yields_nothing() -> None:
    """A record cut off before its value bytes is ignored."""
    full = _pid_record(XP_SIG, 5.25)
    table = full[:-2]  # only 2 of 4 value bytes

    metadata: dict = {}
    _extractor().extract(StreamTables([table]), metadata)

    assert metadata == {}


def test_no_signatures_cannot_extract() -> None:
    tables = StreamTables([b"\x00" * 64])
    assert not _extractor().can_extract(tables)
