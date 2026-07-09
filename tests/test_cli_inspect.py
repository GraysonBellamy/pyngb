"""Tests for the ``pyngb inspect`` subcommand.

Structural views come straight from the document layer / census, so these
tests cross-check the CLI output against ``load_document`` and
``document_census`` on the same fixtures. Most run in-process through
``main([...])`` for speed; one subprocess test pins the entry-point wiring.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from pyngb.api.cli import main
from pyngb.format import load_document
from pyngb.format.census import document_census

TEST_DIR = Path(__file__).parent / "test_files"
FIXTURE = TEST_DIR / "Red_Oak_STA_10K_250731_R7.ngb-ss3"
OTHER_FIXTURE = TEST_DIR / "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="real fixtures not available"
)


def test_inspect_header_and_table_listing(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE)])
    out = capsys.readouterr().out

    assert exit_code == 0
    doc = load_document(FIXTURE)
    # Every stream appears in the header view
    for stream_id in doc.streams:
        assert f"stream_{stream_id}:" in out
    # The default table listing covers stream 1
    assert f"{len(doc.tables_of(1))} tables in stream_1" in out


def test_inspect_stream_selection_with_values(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), "--stream", "2", "--values"])
    out = capsys.readouterr().out

    assert exit_code == 0
    doc = load_document(FIXTURE)
    assert f"{len(doc.tables_of(2))} tables in stream_2" in out
    # The channel data arrays show up as element-counted arrays
    assert "array[" in out


def test_inspect_missing_stream_reported(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), "--stream", "9"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "stream_9 not present" in out


def test_inspect_json_matches_census(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), "--json"])
    out = capsys.readouterr().out

    assert exit_code == 0
    payload = json.loads(out)
    census = document_census(load_document(FIXTURE))
    assert payload["file"] == str(FIXTURE)
    assert payload["streams"] == census["streams"]
    assert payload["unknown_fields"] == census["unknown_fields"]


def test_inspect_coverage_view(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), "--coverage"])
    out = capsys.readouterr().out

    assert exit_code == 0
    census = document_census(load_document(FIXTURE))
    for stream_id, stream_census in census["streams"].items():
        assert f"stream_{stream_id}: {stream_census['tables']} tables" in out
        assert f"gap_bytes={stream_census['gap_bytes']:,}" in out


def test_inspect_unknown_view(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), "--unknown"])
    out = capsys.readouterr().out

    assert exit_code == 0
    census = document_census(load_document(FIXTURE))
    total = sum(len(entries) for entries in census["unknown_fields"].values())
    assert f"{total} unknown category/field/dtype triple(s)" in out


def test_inspect_crossref_marks_varying_fields(capsys) -> None:
    """Two vintages compared: the sample-name field must vary between them."""
    exit_code = main(["inspect", str(FIXTURE), str(OTHER_FIXTURE)])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "across 2 files" in out
    # Sample name (category 0x7530, field 0x0840, string) differs per file
    assert "0x7530/0x0840/0x1f VARIES" in out
    assert "const" in out  # and plenty of fields agree


def test_inspect_crossref_json(capsys) -> None:
    exit_code = main(["inspect", str(FIXTURE), str(OTHER_FIXTURE), "--json"])
    out = capsys.readouterr().out

    assert exit_code == 0
    payload = json.loads(out)
    assert payload["stream"] == 1
    per_file = payload["fields"]["0x7530/0x0840/0x1f"]
    assert len(per_file) == 2


def test_inspect_missing_file_fails() -> None:
    assert main(["inspect", "does_not_exist.ngb-ss3"]) == 1


def test_inspect_subprocess_entry_point() -> None:
    """The installed entry point routes to the subcommand dispatcher."""
    result = subprocess.run(
        [sys.executable, "-m", "pyngb", "inspect", str(FIXTURE), "--coverage"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "stream_1:" in result.stdout
