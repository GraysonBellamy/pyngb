"""NGB container layer: ZIP archive access and the per-stream section directory.

Every ``Streams/stream_N.table`` member is a small database file::

    offset 2:    "Netzsch TA file"
    offset 28:   "_db_format_1"
    offset 0x50: section directory, 14-byte entries
                 ff ff | <section id u16> | <offset u32 LE> | <size u32 LE> | 00 00

The directory ends at the first entry not prefixed ``ff ff`` (2025 vintage)
or at an all-zero terminator entry (2022 vintage). Sections are contiguous
and the last one ends exactly at EOF in every valid file, which makes the
directory a free integrity check. Each stream has a *main* section whose id
equals the stream number, plus (usually) a small section-1 table of contents;
stream 1 folds its TOC into the main section.

This module carries no requiredness policy — which streams must exist is the
API loaders' decision.
"""

from __future__ import annotations

import itertools
import logging
import struct
import zipfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from ..config import ParsingConfig
from ..exceptions import (
    NGBCorruptedFileError,
    NGBParseError,
    NGBResourceLimitError,
    NGBStreamNotFoundError,
)

__all__ = [
    "DIRECTORY_OFFSET",
    "FORMAT_TAG",
    "MAGIC",
    "SectionEntry",
    "StreamData",
    "open_ngb",
    "parse_container",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MAGIC = b"Netzsch TA file"
MAGIC_OFFSET = 2
FORMAT_TAG = b"_db_format_1"
FORMAT_TAG_OFFSET = 28
DIRECTORY_OFFSET = 0x50
_ENTRY_PREFIX = b"\xff\xff"
_ENTRY = struct.Struct("<HII")
_ENTRY_SIZE = 14  # prefix (2) + id (2) + offset (4) + size (4) + pad (2)

_STREAM_PREFIX = "Streams/stream_"
_STREAM_SUFFIX = ".table"


@dataclass(frozen=True, slots=True)
class SectionEntry:
    """One section directory entry."""

    section_id: int
    offset: int
    size: int

    @property
    def end(self) -> int:
        return self.offset + self.size


@dataclass(frozen=True, slots=True)
class StreamData:
    """One stream's raw bytes plus its parsed, validated section directory."""

    stream_id: int
    raw: bytes
    sections: tuple[SectionEntry, ...]

    @property
    def main(self) -> SectionEntry:
        """The main section (id == stream number); existence is validated
        by :func:`parse_container`."""
        for entry in self.sections:
            if entry.section_id == self.stream_id:
                return entry
        raise NGBCorruptedFileError(
            f"stream_{self.stream_id} has no main section",
            stream=self.stream_id,
        )

    @property
    def main_view(self) -> memoryview:
        """Zero-copy view of the main section's bytes."""
        entry = self.main
        return memoryview(self.raw)[entry.offset : entry.end]


def parse_container(stream_id: int, raw: bytes) -> StreamData:
    """Parse and hard-validate one stream blob's header and section directory.

    Raises:
        NGBCorruptedFileError: Missing magic, empty/garbled directory,
            non-contiguous sections, sections not ending at EOF, or a
            missing main section.
    """
    if len(raw) < DIRECTORY_OFFSET + _ENTRY_SIZE:
        raise NGBCorruptedFileError(
            f"stream_{stream_id} is {len(raw)} bytes, too small for a container header",
            stream=stream_id,
            available=len(raw),
        )
    if raw[MAGIC_OFFSET : MAGIC_OFFSET + len(MAGIC)] != MAGIC:
        raise NGBCorruptedFileError(
            f"stream_{stream_id} lacks the 'Netzsch TA file' magic",
            stream=stream_id,
            offset=MAGIC_OFFSET,
        )
    if raw[FORMAT_TAG_OFFSET : FORMAT_TAG_OFFSET + len(FORMAT_TAG)] != FORMAT_TAG:
        raise NGBCorruptedFileError(
            f"stream_{stream_id} lacks the '_db_format_1' format tag",
            stream=stream_id,
            offset=FORMAT_TAG_OFFSET,
        )

    sections: list[SectionEntry] = []
    pos = DIRECTORY_OFFSET
    while pos + _ENTRY_SIZE <= len(raw) and raw[pos : pos + 2] == _ENTRY_PREFIX:
        section_id, offset, size = _ENTRY.unpack_from(raw, pos + 2)
        if section_id == 0 and offset == 0 and size == 0:
            break  # 2022-vintage null terminator entry
        sections.append(SectionEntry(section_id, offset, size))
        pos += _ENTRY_SIZE
    if not sections:
        raise NGBCorruptedFileError(
            f"stream_{stream_id} has an empty section directory",
            stream=stream_id,
            offset=DIRECTORY_OFFSET,
        )

    if sections[0].offset < pos:
        raise NGBCorruptedFileError(
            f"stream_{stream_id} section 0 overlaps the directory",
            stream=stream_id,
            offset=sections[0].offset,
        )
    for previous, current in itertools.pairwise(sections):
        if current.offset != previous.end:
            raise NGBCorruptedFileError(
                f"stream_{stream_id} sections are not contiguous: section "
                f"{previous.section_id} ends at {previous.end} but section "
                f"{current.section_id} starts at {current.offset}",
                stream=stream_id,
                offset=current.offset,
                declared=current.offset,
                available=previous.end,
            )
    if sections[-1].end != len(raw):
        raise NGBCorruptedFileError(
            f"stream_{stream_id} last section ends at {sections[-1].end} "
            f"but the stream is {len(raw)} bytes",
            stream=stream_id,
            offset=sections[-1].offset,
            declared=sections[-1].end,
            available=len(raw),
        )
    if all(entry.section_id != stream_id for entry in sections):
        raise NGBCorruptedFileError(
            f"stream_{stream_id} has no main section (id == {stream_id}); "
            f"directory ids: {[entry.section_id for entry in sections]}",
            stream=stream_id,
        )

    return StreamData(stream_id, raw, tuple(sections))


@contextmanager
def _translated_errors(path: Path) -> Iterator[None]:
    """Translate common failure modes to pyngb exceptions (ported verbatim
    from the legacy parser so the error contract survives the rewrite)."""
    try:
        yield
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid ZIP archive {path}: {e}")
        raise
    except NGBParseError:
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"Failed to parse NGB file {path}: {e}")
        raise NGBParseError(f"Parsing failed: {e}") from e
    except OSError as e:
        logger.error(f"I/O error while parsing NGB file {path}: {e}")
        raise NGBParseError(f"I/O error: {e}") from e


def _member_name(stream_id: int) -> str:
    return f"{_STREAM_PREFIX}{stream_id}{_STREAM_SUFFIX}"


def _available_streams(archive: zipfile.ZipFile) -> list[int]:
    ids = []
    for name in archive.namelist():
        if name.startswith(_STREAM_PREFIX) and name.endswith(_STREAM_SUFFIX):
            middle = name[len(_STREAM_PREFIX) : -len(_STREAM_SUFFIX)]
            if middle.isdigit():
                ids.append(int(middle))
    return sorted(ids)


def open_ngb(
    path: str | Path,
    *,
    streams: Iterable[int] | None = None,
    limits: ParsingConfig | None = None,
) -> dict[int, StreamData]:
    """Read stream blobs from an NGB archive and validate their containers.

    Args:
        path: Path to the ``.ngb-*`` file.
        streams: Stream numbers to load; None loads every stream present.
            Explicitly requested streams must exist.
        limits: Resource limits; each member's declared decompressed size is
            checked against ``max_stream_size_mb`` before decompression (the
            ZIP directory's declared size is authoritative: zipfile never
            decompresses past it — a lying member fails its CRC check).

    Raises:
        FileNotFoundError: The file does not exist.
        zipfile.BadZipFile: The file is not a ZIP archive.
        NGBStreamNotFoundError: An explicitly requested stream is missing.
        NGBCorruptedFileError: A stream's container structure is invalid.
        NGBResourceLimitError: A stream declares more than
            ``max_stream_size_mb`` decompressed.
    """
    limits = limits or ParsingConfig()
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    max_bytes = limits.max_stream_size_mb * 1024 * 1024

    with _translated_errors(path), zipfile.ZipFile(path, "r") as archive:
        available = _available_streams(archive)
        wanted = available if streams is None else sorted(set(streams))
        missing = [sid for sid in wanted if sid not in available]
        if missing:
            raise NGBStreamNotFoundError(
                f"Missing required streams: {[_member_name(sid) for sid in missing]}"
            )
        loaded: dict[int, StreamData] = {}
        for stream_id in wanted:
            name = _member_name(stream_id)
            info = archive.getinfo(name)
            if info.file_size > max_bytes:
                raise NGBResourceLimitError(
                    f"{name} declares {info.file_size:,} bytes decompressed, "
                    f"exceeding max_stream_size_mb limit of "
                    f"{limits.max_stream_size_mb}",
                    stream=stream_id,
                    declared=info.file_size,
                    limit=max_bytes,
                )
            with archive.open(name) as member:
                loaded[stream_id] = parse_container(stream_id, member.read())
        return loaded
