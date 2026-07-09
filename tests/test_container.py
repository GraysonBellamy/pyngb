"""Container layer: ZIP access and section-directory parsing/integrity.

Corruption cases assert exception types and structured attributes, never
message prose. Every corruption test first proves the uncorrupted input
parses, so a failure means the corruption was detected — not that the
builder produced garbage.
"""

import zipfile
from pathlib import Path

import pytest

from pyngb.config import ParsingConfig
from pyngb.exceptions import (
    NGBCorruptedFileError,
    NGBResourceLimitError,
    NGBStreamNotFoundError,
)
from pyngb.format import DType, open_ngb, parse_container
from support.ngb_builder import (
    build_scalar,
    build_section,
    build_stream,
    build_table,
    corrupt_directory,
    write_ngb,
)

FIXTURE_DIR = Path(__file__).parent / "test_files"
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*")) if FIXTURE_DIR.exists() else []


def one_section_stream(stream_id: int = 1, *, terminator: bool = True) -> bytes:
    table = build_table(0x1772, [build_scalar(0x083C, DType.STRING, "x")])
    return build_stream(stream_id, body=build_section([table]), terminator=terminator)


def two_section_stream(stream_id: int = 2) -> bytes:
    main = build_section([build_table(0x7530, [build_scalar(0x0998, DType.U16, 1)])])
    toc = build_section([build_table(0x0323, [build_scalar(0x0001, DType.U16, 1)])])
    return build_stream(stream_id, [(stream_id, main), (1, toc)])


class TestParseContainerFixtures:
    """The directory invariants hold for every stream of every real fixture."""

    @pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
    def test_all_streams_parse_and_validate(self, fixture: Path) -> None:
        streams = open_ngb(fixture)
        assert set(streams) >= {1, 2, 3}
        for stream_id, stream in streams.items():
            assert stream.stream_id == stream_id
            assert stream.sections
            assert stream.main.section_id == stream_id
            assert stream.sections[-1].end == len(stream.raw)
            for previous, current in zip(stream.sections, stream.sections[1:]):
                assert current.offset == previous.end
            assert len(stream.main_view) == stream.main.size


class TestParseContainerSynthetic:
    def test_single_section_round_trip(self) -> None:
        stream = parse_container(1, one_section_stream())
        assert stream.main.section_id == 1
        assert stream.sections[-1].end == len(stream.raw)

    def test_directory_without_terminator_entry(self) -> None:
        """2025-vintage directories end without an all-zero entry."""
        stream = parse_container(1, one_section_stream(terminator=False))
        assert len(stream.sections) == 1

    def test_multi_section_stream(self) -> None:
        stream = parse_container(2, two_section_stream())
        assert [entry.section_id for entry in stream.sections] == [2, 1]
        assert stream.main.section_id == 2

    def test_too_small_blob(self) -> None:
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, b"tiny")
        assert excinfo.value.stream == 1
        assert excinfo.value.available == 4

    def test_missing_magic(self) -> None:
        blob = bytearray(one_section_stream())
        blob[2] ^= 0xFF
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, bytes(blob))
        assert excinfo.value.stream == 1
        assert excinfo.value.offset == 2

    def test_missing_format_tag(self) -> None:
        blob = bytearray(one_section_stream())
        blob[28] ^= 0xFF
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, bytes(blob))
        assert excinfo.value.offset == 28

    def test_missing_main_section(self) -> None:
        blob = build_stream(2, [(7, build_section([]))])
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(2, blob)
        assert excinfo.value.stream == 2


class TestDirectoryCorruption:
    def test_pristine_parses(self) -> None:
        parse_container(1, one_section_stream())
        parse_container(2, two_section_stream())

    def test_cleared_entry_prefix(self) -> None:
        blob = corrupt_directory(one_section_stream(), "prefix")
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, blob)
        assert excinfo.value.stream == 1
        assert excinfo.value.offset == 0x50

    def test_inflated_size_breaks_contiguity(self) -> None:
        blob = corrupt_directory(two_section_stream(), "size")
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(2, blob)
        assert excinfo.value.declared is not None
        assert excinfo.value.available is not None
        assert excinfo.value.declared != excinfo.value.available

    def test_inflated_size_breaks_eof(self) -> None:
        blob = corrupt_directory(one_section_stream(), "size")
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, blob)
        assert excinfo.value.declared == excinfo.value.available + 4

    def test_truncated_stream(self) -> None:
        blob = corrupt_directory(one_section_stream(), "truncate")
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            parse_container(1, blob)
        assert excinfo.value.declared == excinfo.value.available + 8


class TestOpenNgb:
    def test_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            open_ngb("does_not_exist.ngb-ss3")

    def test_not_a_zip(self, tmp_path: Path) -> None:
        bogus = tmp_path / "bogus.ngb-ss3"
        bogus.write_bytes(b"not a zip archive")
        with pytest.raises(zipfile.BadZipFile):
            open_ngb(bogus)

    def test_requested_stream_subset(self, tmp_path: Path) -> None:
        path = write_ngb(
            tmp_path / "two.ngb-ss3",
            {1: one_section_stream(1), 2: two_section_stream(2)},
        )
        assert set(open_ngb(path)) == {1, 2}
        assert set(open_ngb(path, streams=[1])) == {1}

    def test_requested_stream_missing(self, tmp_path: Path) -> None:
        path = write_ngb(tmp_path / "one.ngb-ss3", {1: one_section_stream(1)})
        with pytest.raises(NGBStreamNotFoundError):
            open_ngb(path, streams=[1, 2])

    def test_oversized_stream_rejected_before_decompression(
        self, tmp_path: Path
    ) -> None:
        """The declared decompressed size is checked against the limit before
        any decompression happens (decompression-bomb guard)."""
        path = write_ngb(tmp_path / "bomb.ngb-ss3", {1: b"\x00" * (2 * 1024 * 1024)})
        with pytest.raises(NGBResourceLimitError) as excinfo:
            open_ngb(path, limits=ParsingConfig(max_stream_size_mb=1))
        assert excinfo.value.stream == 1
        assert excinfo.value.declared == 2 * 1024 * 1024
        assert excinfo.value.limit == 1024 * 1024

    def test_within_limit_parses(self, tmp_path: Path) -> None:
        path = write_ngb(tmp_path / "ok.ngb-ss3", {1: one_section_stream(1)})
        assert set(open_ngb(path, limits=ParsingConfig(max_stream_size_mb=1))) == {1}
