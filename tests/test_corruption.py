"""Hostile or damaged input must fail loudly, never parse silently.

These tests rebuild a real fixture with one surgically corrupted stream and
assert the parser refuses it loudly. Before this behavior existed, a truncated
stream_2 parsed "successfully" as a well-formed table with columns silently
missing. Corruption targets are located through the document layer (field
spans), not by pattern-hunting, so the tests stay honest about what they
damage. Resource-limit tests cover the decompression-bomb guard the same way.

Builder-based (synthetic) corruption coverage lives in test_tokenizer.py,
test_container.py, and test_channels.py; this module pins the same policies
end-to-end on a real fixture through the public API.
"""

import io
import struct
import zipfile
from pathlib import Path

import pytest

from pyngb import read_ngb
from pyngb.config import ParsingConfig
from pyngb.exceptions import NGBCorruptedFileError, NGBResourceLimitError
from pyngb.format import DType, Mode, load_document
from pyngb.format.maps import SEGMENT_VALUES_TYPE

FIXTURE = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="real fixture not available"
)


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


def first_data_array_span(stream_id: int) -> tuple[int, int]:
    """Absolute (start, end) of the first segment-value data array record."""
    doc = load_document(FIXTURE, streams=(stream_id,))
    for table in doc.tables_of(stream_id):
        if table.type_ref != SEGMENT_VALUES_TYPE:
            continue
        for field in table.fields.values():
            if field.mode is Mode.ARRAY and field.dtype in (DType.F64, DType.F32):
                return field.span
    raise AssertionError(f"no data array found in stream_{stream_id}")


class TestStream2Corruption:
    MEMBER = "Streams/stream_2.table"

    def test_truncation_mid_payload_raises(self, tmp_path: Path) -> None:
        """Cutting the stream inside a data payload breaks the section
        directory's sections-end-at-EOF invariant and must be refused."""
        stream = read_stream(FIXTURE, self.MEMBER)
        start, _end = first_data_array_span(2)
        corrupted = tmp_path / "truncated.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, self.MEMBER, stream[: start + 40])

        with pytest.raises(NGBCorruptedFileError):
            read_ngb(corrupted)

    def test_count_field_mismatch_raises(self, tmp_path: Path) -> None:
        """Inflating an array's element count declares more data than the
        record holds; the damaged stream must not assemble."""
        stream = read_stream(FIXTURE, self.MEMBER)
        start, end = first_data_array_span(2)
        count_pos = stream.index(b"\xa0\x01", start, end) + 2
        count = int.from_bytes(stream[count_pos : count_pos + 4], "little")
        mutated = (
            stream[:count_pos]
            + (count + 1).to_bytes(4, "little")
            + stream[count_pos + 4 :]
        )
        corrupted = tmp_path / "bad_count.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, self.MEMBER, mutated)

        with pytest.raises(NGBCorruptedFileError) as excinfo:
            read_ngb(corrupted)
        assert excinfo.value.stream == 2

    def test_unknown_data_type_raises(self, tmp_path: Path) -> None:
        """An unknown dtype byte in a data record is a malformed span, which
        is fatal in a measurement stream."""
        stream = read_stream(FIXTURE, self.MEMBER)
        start, end = first_data_array_span(2)
        dtype_pos = stream.index(b"\x17\xfc\xff\xff", start, end) + 4
        mutated = stream[:dtype_pos] + b"\x99" + stream[dtype_pos + 1 :]
        corrupted = tmp_path / "bad_dtype.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, self.MEMBER, mutated)

        with pytest.raises(NGBCorruptedFileError) as excinfo:
            read_ngb(corrupted)
        assert excinfo.value.stream == 2


class TestStream3Corruption:
    def test_count_field_mismatch_raises(self, tmp_path: Path) -> None:
        stream = read_stream(FIXTURE, "Streams/stream_3.table")
        start, end = first_data_array_span(3)
        count_pos = stream.index(b"\xa0\x01", start, end) + 2
        count = int.from_bytes(stream[count_pos : count_pos + 4], "little")
        mutated = (
            stream[:count_pos]
            + (count + 1).to_bytes(4, "little")
            + stream[count_pos + 4 :]
        )
        corrupted = tmp_path / "bad_count_s3.ngb-ss3"
        rewrite_stream(FIXTURE, corrupted, "Streams/stream_3.table", mutated)

        with pytest.raises(NGBCorruptedFileError) as excinfo:
            read_ngb(corrupted)
        assert excinfo.value.stream == 3


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
        any decompression — this is the decompression-bomb guard, exercised
        end-to-end through read_ngb's limits parameter.
        """
        bomb = tmp_path / "bomb.ngb-ss3"
        rewrite_stream(
            FIXTURE, bomb, "Streams/stream_2.table", b"\x00" * (2 * 1024 * 1024)
        )

        with pytest.raises(NGBResourceLimitError, match="max_stream_size_mb"):
            read_ngb(bomb, limits=ParsingConfig(max_stream_size_mb=1))

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
