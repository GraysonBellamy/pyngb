"""Hostile or damaged input must fail loudly, never parse silently.

These tests rebuild a real fixture with one surgically corrupted stream and
assert the parser refuses it loudly. Before this behavior existed, a truncated
stream_2 parsed "successfully" as a well-formed table with columns silently
missing (AUDIT CORR-02). Resource-limit tests cover the decompression-bomb
guard the same way (AUDIT CORR-04).

Known limitation: truncation exactly at a table boundary that removes whole
trailing channels is indistinguishable from a file that legitimately records
fewer channels, so the parser cannot detect it. Column-presence checks belong
to validation, not parsing.
"""

import io
import struct
import zipfile
from pathlib import Path

import pytest

from pyngb import read_ngb
from pyngb.binary import BinaryParser
from pyngb.config import ParsingConfig
from pyngb.constants import StreamMarkers
from pyngb.core import NGBParser
from pyngb.exceptions import NGBCorruptedFileError, NGBResourceLimitError

FIXTURE = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="real fixture not available"
)

_MARKERS = BinaryParser().markers
_STREAM_MARKERS = StreamMarkers()


def read_stream(path: Path, member: str) -> bytes:
    with zipfile.ZipFile(path) as z:
        return z.read(member)


def rewrite_stream(src: Path, dst: Path, member: str, new_bytes: bytes) -> None:
    """Copy an NGB archive, replacing one stream's contents."""
    with (
        zipfile.ZipFile(src) as zin,
        zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout,
    ):
        for item in zin.infolist():
            data = new_bytes if item.filename == member else zin.read(item.filename)
            zout.writestr(item.filename, data)


def data_table_offsets(stream: bytes) -> list[tuple[int, bytes]]:
    """(absolute_offset, table) for each table carrying a data payload."""
    tables = BinaryParser().split_tables(stream)
    out = []
    pos = 0
    for table in tables:
        offset = stream.find(table, pos)
        assert offset != -1
        pos = offset + len(table)
        marker_pos = _STREAM_MARKERS.DATA_MARKER_POS
        has_data_marker = (
            table[marker_pos : marker_pos + 1] == _STREAM_MARKERS.STREAM2_DATA
        )
        if has_data_marker and _MARKERS.START_DATA in table:
            out.append((offset, table))
    return out


class TestStream2Corruption:
    def test_truncation_mid_payload_raises(self, tmp_path: Path) -> None:
        stream = read_stream(FIXTURE, "Streams/stream_2.table")
        offset, table = data_table_offsets(stream)[0]
        cut = offset + table.find(_MARKERS.START_DATA) + 40  # inside the payload
        corrupted = tmp_path / "truncated.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_2.table", stream[:cut])

        with pytest.raises(NGBCorruptedFileError, match="START_DATA without END_DATA"):
            read_ngb(corrupted)

    def test_count_field_mismatch_raises(self, tmp_path: Path) -> None:
        stream = read_stream(FIXTURE, "Streams/stream_2.table")
        offset, table = data_table_offsets(stream)[0]
        count_pos = offset + table.find(_MARKERS.START_DATA) + 2
        count = int.from_bytes(stream[count_pos : count_pos + 4], "little")
        mutated = (
            stream[:count_pos]
            + (count + 1).to_bytes(4, "little")
            + stream[count_pos + 4 :]
        )
        corrupted = tmp_path / "bad_count.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_2.table", mutated)

        with pytest.raises(NGBCorruptedFileError, match="count field declares"):
            read_ngb(corrupted)

    def test_unknown_data_type_raises(self, tmp_path: Path) -> None:
        stream = read_stream(FIXTURE, "Streams/stream_2.table")
        offset, table = data_table_offsets(stream)[0]
        dtype_pos = offset + table.find(_MARKERS.START_DATA) - 1
        mutated = stream[:dtype_pos] + b"\x99" + stream[dtype_pos + 1 :]
        corrupted = tmp_path / "bad_dtype.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_2.table", mutated)

        with pytest.raises(NGBCorruptedFileError):
            read_ngb(corrupted)

    def test_missing_data_table_raises(self, tmp_path: Path) -> None:
        """Deleting one channel's data table makes its length inconsistent."""
        stream = read_stream(FIXTURE, "Streams/stream_2.table")
        offset, table = data_table_offsets(stream)[0]
        mutated = stream[:offset] + stream[offset + len(table) :]
        corrupted = tmp_path / "missing_table.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_2.table", mutated)

        with pytest.raises(NGBCorruptedFileError, match="values but the frame has"):
            read_ngb(corrupted)


class TestStream3Corruption:
    def test_truncation_mid_payload_raises(self, tmp_path: Path) -> None:
        stream = read_stream(FIXTURE, "Streams/stream_3.table")
        offset, table = data_table_offsets(stream)[0]
        cut = offset + table.find(_MARKERS.START_DATA) + 40
        corrupted = tmp_path / "truncated_s3.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_3.table", stream[:cut])

        with pytest.raises(NGBCorruptedFileError, match="START_DATA without END_DATA"):
            read_ngb(corrupted)


def lie_about_member_size(archive: bytes, member: str, declared: int) -> bytes:
    """Patch a member's declared uncompressed size in both the local file
    header and the central directory, leaving the compressed payload intact.
    """
    data = bytearray(archive)
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        header_offset = z.getinfo(member).header_offset
    struct.pack_into("<I", data, header_offset + 22, declared)

    name = member.encode()
    pos = 0
    while True:
        pos = data.find(b"PK\x01\x02", pos)
        assert pos != -1, "central directory entry not found"
        (name_len,) = struct.unpack_from("<H", data, pos + 28)
        if data[pos + 46 : pos + 46 + name_len] == name:
            struct.pack_into("<I", data, pos + 24, declared)
            return bytes(data)
        pos += 4


class TestResourceLimits:
    def test_oversized_stream_rejected_before_decompression(
        self, tmp_path: Path
    ) -> None:
        """A stream declaring more than max_stream_size_mb must be refused.

        The rejection happens on the ZIP directory's declared size, before
        any decompression — this is the decompression-bomb guard.
        """
        bomb = tmp_path / "bomb.ngb-ss3"
        rewrite_stream(
            FIXTURE, bomb, "Streams/stream_2.table", b"\x00" * (2 * 1024 * 1024)
        )
        parser = NGBParser(parsing_config=ParsingConfig(max_stream_size_mb=1))

        with pytest.raises(NGBResourceLimitError, match="max_stream_size_mb"):
            parser.parse(bomb)

    def test_member_lying_about_size_fails_loudly(self, tmp_path: Path) -> None:
        """Pin the invariant the size guard relies on: zipfile never
        decompresses past a member's declared size, so a member that lies to
        sneak under the limit fails its CRC check instead of bombing.
        """
        stream = read_stream(FIXTURE, "Streams/stream_2.table")
        rebuilt = tmp_path / "rebuilt.ngb-ss3"
        rewrite_stream(FIXTURE, rebuilt, "Streams/stream_2.table", stream)

        liar = tmp_path / "liar.ngb-ss3"
        liar.write_bytes(
            lie_about_member_size(
                rebuilt.read_bytes(), "Streams/stream_2.table", declared=100
            )
        )

        with pytest.raises(zipfile.BadZipFile):
            read_ngb(liar)


def test_pristine_fixture_still_parses(tmp_path: Path) -> None:
    """The zip-rebuild helper itself must not break a healthy file."""
    stream = read_stream(FIXTURE, "Streams/stream_2.table")
    rebuilt = tmp_path / "rebuilt.ngb-ss3"
    rewrite_stream(FIXTURE, rebuilt, "Streams/stream_2.table", stream)

    table = read_ngb(rebuilt)
    assert table.num_rows > 0
    assert "mass" in table.column_names
