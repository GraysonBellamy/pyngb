"""Regression tests for stream_2 channel attribution (audit CORR-01).

Stream_2 data tables carry no channel identity: a channel's header table
precedes its data tables, so data must be named by the *preceding* header.
A positional off-by-one (flush under the next header's name, compensated by
a shifted column_map) used to mislabel channels whenever a mid-sequence
channel was absent: files without a purge-2 MFC channel published their
50 ml/min nitrogen purge-1 flow as ``purge_flow_2`` — whose MFC metadata
says OXYGEN — turning pyrolysis runs into apparent combustion runs.

These tests pin the flow-channel labels against the physically consistent
reading (validated against MFC gas metadata and run chemistry) for every
shipped fixture, plus golden values for the always-correct channels so the
fix provably did not move them.
"""

from pathlib import Path

import polars as pl
import pytest

from pyngb import read_ngb

TEST_DIR = Path(__file__).parent / "test_files"

# Fixture with all three MFC channels present (9c, 9d, 9e). 21 % O2 in N2:
# purge_1 = 35 N2, purge_2 = 15 O2, protective = 20 N2 -> 15/70 = 21.4 % O2,
# matching the "21O2" in the filename.
ALL_CHANNELS_FILE = TEST_DIR / "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3"

# Fixtures without the purge-2 channel (9d): pure-N2 pyrolysis runs and
# baselines. purge_1 = 50 N2, protective = 20 N2, purge_flow_2 absent.
NO_PURGE2_FILES = [
    TEST_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3",
    TEST_DIR / "Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3",
    TEST_DIR / "Douglas_Fir_STA_Baseline_10K_250813_R15.ngb-bs3",
    TEST_DIR / "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3",
    TEST_DIR / "Red_Oak_STA_10K_250731_R7.ngb-ss3",
]

GOLDEN_FILE = TEST_DIR / "Red_Oak_STA_10K_250731_R7.ngb-ss3"

# First/last values of the channels that were labeled correctly before the
# CORR-01 fix; they must not move.
GOLDEN_ENDPOINTS = {
    "time": (0.0, 8381.95),
    "sample_temperature": (27.023000717163086, 807.4819946289062),
    "dsc_signal": (-8.8516263961792, 12.857059478759766),
    "mass": (0.0, -3.086),
}


def _load(path: Path) -> tuple[dict, pl.DataFrame]:
    metadata, table = read_ngb(str(path), return_metadata=True)
    return metadata, pl.from_arrow(table)


@pytest.mark.skipif(not ALL_CHANNELS_FILE.exists(), reason="fixture missing")
class TestAllChannelsPresent:
    """File with purge_1, purge_2, and protective MFC channels."""

    @pytest.fixture(scope="class")
    def loaded(self) -> tuple[dict, pl.DataFrame]:
        return _load(ALL_CHANNELS_FILE)

    def test_flow_labels_and_values(self, loaded: tuple[dict, pl.DataFrame]) -> None:
        _, df = loaded
        assert df["purge_flow_1"].median() == pytest.approx(35.0, abs=0.5)
        assert df["purge_flow_2"].median() == pytest.approx(15.0, abs=0.5)
        assert df["protective_flow"].median() == pytest.approx(20.0, abs=0.5)

    def test_oxygen_fraction_matches_filename(
        self, loaded: tuple[dict, pl.DataFrame]
    ) -> None:
        metadata, df = loaded
        assert metadata["purge_2_mfc_gas"] == "OXYGEN"
        total = (
            df["purge_flow_1"].median()
            + df["purge_flow_2"].median()
            + df["protective_flow"].median()
        )
        o2_fraction = df["purge_flow_2"].median() / total
        assert o2_fraction == pytest.approx(0.214, abs=0.01)


class TestPurge2ChannelAbsent:
    """Pure-N2 runs: the oxygen MFC channel must not appear in the output."""

    @pytest.mark.parametrize(
        "path", NO_PURGE2_FILES, ids=lambda p: p.name.rsplit(".", 1)[0]
    )
    def test_flow_labels_and_values(self, path: Path) -> None:
        if not path.exists():
            pytest.skip("fixture missing")
        metadata, df = _load(path)

        # The 50 ml/min flow is nitrogen through purge 1...
        assert metadata["purge_1_mfc_gas"] == "NITROGEN"
        assert df["purge_flow_1"].median() == pytest.approx(50.0, abs=0.5)
        assert df["protective_flow"].median() == pytest.approx(20.0, abs=0.5)

        # ...and the oxygen MFC (purge 2) recorded no channel at all. Labeling
        # a nitrogen flow as purge_flow_2 was the CORR-01 bug.
        assert metadata["purge_2_mfc_gas"] == "OXYGEN"
        assert "purge_flow_2" not in df.columns


@pytest.mark.skipif(not GOLDEN_FILE.exists(), reason="fixture missing")
class TestUnaffectedChannelsUnchanged:
    """Channels that preceded the flows were always labeled correctly."""

    @pytest.fixture(scope="class")
    def df(self) -> pl.DataFrame:
        return _load(GOLDEN_FILE)[1]

    def test_shape(self, df: pl.DataFrame) -> None:
        assert df.height == 9171

    @pytest.mark.parametrize("column", sorted(GOLDEN_ENDPOINTS))
    def test_endpoints(self, df: pl.DataFrame, column: str) -> None:
        first, last = GOLDEN_ENDPOINTS[column]
        assert df[column][0] == pytest.approx(first, rel=1e-9, abs=1e-12)
        assert df[column][-1] == pytest.approx(last, rel=1e-6)

    def test_temperature_and_time_ascend(self, df: pl.DataFrame) -> None:
        # Time must be strictly increasing; a swapped label would break this.
        assert (df["time"].diff().drop_nulls() > 0).all()
