"""Tests for run-environment metadata: timezone and the linked correction file.

The timezone comes from the ``59 18`` snapshot table (Windows
TIME_ZONE_INFORMATION-style fields); the correction-file link from the
``70 17`` measurement-definition table (field ``43 08``). Both are pinned for
every shipped fixture.
"""

import json
from pathlib import Path

import pytest

from pyngb import read_ngb

TEST_DIR = Path(__file__).parent / "test_files"

# (timezone name, utc offset minutes) per fixture. The 250813 baseline was
# recorded with the instrument PC set to a European timezone - real data,
# and a useful check that values are read per-file rather than assumed.
EXPECTED_TIMEZONE = {
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3": ("Eastern Standard Time", -300),
    "Douglas_Fir_STA_10K_250730_R13.ngb-ss3": ("Eastern Daylight Time", -240),
    "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3": (
        "Eastern Daylight Time",
        -240,
    ),
    "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3": (
        "Mitteleuropäische Sommerzeit",
        120,
    ),
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3": ("Eastern Standard Time", -300),
    "Red_Oak_STA_10K_250731_R7.ngb-ss3": ("Eastern Daylight Time", -240),
}

# Basename of the linked correction/measurement file per fixture. Sample runs
# link their correction file; correction runs may link the related sample or a
# prior correction run.
EXPECTED_LINK_BASENAME = {
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3": (
        "DF_FILED_STA_Correction_21O2_10K_220222_R1.ngb-bs3"
    ),
    "Douglas_Fir_STA_10K_250730_R13.ngb-ss3": (
        "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
    ),
    "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3": (
        "Douglas_Fir_STA_Baseline_10K_250729_R12.ngb-bs3"
    ),
    "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3": (
        "Douglas_Fir_STA_10K_250813_R15.ngb-ss3"
    ),
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3": (
        "RO_FILED_STA_Correction_N2_10K_250129_R29.ngb-bs3"
    ),
    "Red_Oak_STA_10K_250731_R7.ngb-ss3": ("Red_Oak_STA_Baseline_10K_250731_R7.ngb-bs3"),
}


def _metadata(path: Path) -> dict:
    table = read_ngb(str(path))
    return json.loads(table.schema.metadata[b"file_metadata"])


@pytest.mark.parametrize(
    "fixture_name",
    sorted(p.name for p in TEST_DIR.glob("*.ngb-*s3")),
)
def test_fixture_timezone(fixture_name: str) -> None:
    md = _metadata(TEST_DIR / fixture_name)
    expected = EXPECTED_TIMEZONE.get(fixture_name)
    if expected is None:
        pytest.skip(f"no pinned timezone for {fixture_name}")
    name, offset = expected
    assert md["timezone"] == name
    assert md["utc_offset_minutes"] == offset


@pytest.mark.parametrize(
    "fixture_name",
    sorted(p.name for p in TEST_DIR.glob("*.ngb-*s3")),
)
def test_fixture_correction_link(fixture_name: str) -> None:
    md = _metadata(TEST_DIR / fixture_name)
    expected = EXPECTED_LINK_BASENAME.get(fixture_name)
    if expected is None:
        pytest.skip(f"no pinned link for {fixture_name}")
    link = md["correction_file_path"]
    assert link.replace("\\", "/").rsplit("/", 1)[-1] == expected
