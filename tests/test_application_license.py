"""Regression tests for application/license metadata extraction.

The extractor's filters used ``r"\\s"``-style double-escaped literals, so both
fields were ``None`` on every file ever parsed. These tests pin the extracted
values on real fixtures so the filters can never silently die again.
"""

import re
from pathlib import Path

import pytest

from pyngb import read_ngb

TEST_DIR = Path(__file__).parent / "test_files"

REG_FILE = TEST_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3"

ALL_FIXTURES = sorted(TEST_DIR.glob("*.ngb-*s3"))


def _metadata(path: Path) -> dict:
    metadata, _ = read_ngb(str(path), return_metadata=True)
    return metadata


@pytest.mark.skipif(not REG_FILE.exists(), reason="regression file missing")
class TestApplicationLicenseRegression:
    @pytest.fixture(scope="class")
    def md(self) -> dict:
        return _metadata(REG_FILE)

    def test_application_version_exact(self, md: dict) -> None:
        assert md["application_version"] == "Version 8.0.3  (10.12.2024) 24345.303"

    def test_licensed_to_exact(self, md: dict) -> None:
        # Real newlines, not the two-character sequence backslash-n
        assert md["licensed_to"] == "University of Maryland\nCollege Park\nUSA"


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: p.name.rsplit(".", 1)[0])
def test_extracted_on_every_fixture(path: Path) -> None:
    md = _metadata(path)
    assert re.match(r"^Version \d+\.\d+\.\d+\b", md["application_version"])
    assert "\n" in md["licensed_to"]
    assert "\\n" not in md["licensed_to"]
