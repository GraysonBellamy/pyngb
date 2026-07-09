"""Tests for the ``pyngb validate`` subcommand."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from pyngb.api.cli import main

TEST_DIR = Path(__file__).parent / "test_files"
FIXTURE = TEST_DIR / "Red_Oak_STA_10K_250731_R7.ngb-ss3"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="real fixtures not available"
)


def test_validate_healthy_fixture_passes(capsys) -> None:
    exit_code = main(["validate", str(FIXTURE)])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert str(FIXTURE) in out
    assert "Overall Status: VALID" in out


def test_validate_json_shape(capsys) -> None:
    exit_code = main(["validate", str(FIXTURE), "--json"])
    out = capsys.readouterr().out

    assert exit_code == 0
    reports = json.loads(out)
    assert len(reports) == 1
    report = reports[0]
    assert report["file"] == str(FIXTURE)
    assert report["is_valid"] is True
    assert report["error_count"] == 0
    assert "checks_passed" in report


def test_validate_multiple_files_aggregates(capsys) -> None:
    fixtures = sorted(TEST_DIR.glob("*.ngb-ss3"))
    exit_code = main(["validate", *map(str, fixtures), "--json"])
    out = capsys.readouterr().out

    reports = json.loads(out)
    assert len(reports) == len(fixtures)
    assert exit_code == 0 if all(r.get("is_valid") for r in reports) else 1


def test_validate_unparseable_file_fails(tmp_path: Path, capsys) -> None:
    bad = tmp_path / "junk.ngb-ss3"
    bad.write_bytes(b"not a zip archive")

    exit_code = main(["validate", str(bad), "--json"])
    out = capsys.readouterr().out

    assert exit_code == 1
    reports = json.loads(out)
    assert "parse_error" in reports[0]


def test_validate_missing_file_fails() -> None:
    assert main(["validate", "does_not_exist.ngb-ss3"]) == 1


def test_validate_subprocess_entry_point() -> None:
    """The installed entry point routes to the subcommand dispatcher."""
    result = subprocess.run(
        [sys.executable, "-m", "pyngb", "validate", str(FIXTURE)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
