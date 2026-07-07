"""Deep-inspection toolkit for NGB binary streams.

Tokenizes the low-level record grammar shared by all streams and provides
three subcommands for reverse-engineering work:

    header   - parse the _db_format_1 container header + section directory
    census   - enumerate every table and field (id, dtype, value) in a stream
    crossref - compare every (category, field) key across multiple files

The record grammar (see docs/binary-format.md and FORMAT_FINDINGS.md):

    table  := <category u16> <TABLE_SEPARATOR> record*
    record := 18 fc ff ff 03 80 01 <field_id u16> 00 00 01 00 00 00 0c 00
              17 fc ff ff <dtype 1B>
              (80 01 <scalar> | a0 01 <count u32> <array>)
              END_FIELD

Usage:
    uv run python scripts/ngb_deep_inspect.py header file.ngb-ss3
    uv run python scripts/ngb_deep_inspect.py census file.ngb-ss3 --stream 1
    uv run python scripts/ngb_deep_inspect.py census file.ngb-ss3 --stream 6 --values
    uv run python scripts/ngb_deep_inspect.py crossref *.ngb-ss3 --stream 1 --varying
"""

from __future__ import annotations

import argparse
import json
import struct
import zipfile
from collections import defaultdict
from pathlib import Path

TYPE_PREFIX = b"\x17\xfc\xff\xff"
END_FIELD = b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
TABLE_SEP = (
    b"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x1a\x80\x01\x01\x80\x02\x00\x00"
)

DTYPE_NAMES = {
    0x02: "u16",
    0x03: "i32",
    0x04: "f32",
    0x05: "f64",
    0x10: "u8",
    0x14: "b8",
    0x1A: "ref",
    0x1F: "str",
    0x48: "b16",
}


def load_stream(path: Path, stream: int) -> bytes:
    """Read one stream from an NGB archive, or the file directly if raw."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            return z.read(f"Streams/stream_{stream}.table")
    return path.read_bytes()


def split_tables(data: bytes) -> list[bytes]:
    """Split on TABLE_SEP with offset -2 so the 2 category bytes lead."""
    idxs = []
    pos = 0
    while True:
        i = data.find(TABLE_SEP, pos)
        if i == -1:
            break
        idxs.append(max(0, i - 2))
        pos = i + len(TABLE_SEP)
    if not idxs:
        return [data]
    ends = [*idxs[1:], len(data)]
    return [data[a:b] for a, b in zip(idxs, ends) if data[a:b]]


def parse_string(value: bytes) -> str | None:
    """Decode an NGB string payload (length-prefixed UTF-8/UTF-16LE or fffeff)."""
    if len(value) < 4:
        return None
    if value.startswith(b"\xff\xfe\xff"):
        n = value[3]
        try:
            return value[4 : 4 + 2 * n].decode("utf-16le").strip("\x00")
        except UnicodeDecodeError:
            return None
    n = struct.unpack("<I", value[:4])[0]
    if 0 < n <= len(value) - 4:
        raw = value[4 : 4 + n]
        try:
            s = raw.decode("utf-8").strip().replace("\x00", "")
            if s:
                return s
        except UnicodeDecodeError:
            pass
        try:
            return raw.decode("utf-16le").strip("\x00")
        except UnicodeDecodeError:
            return None
    return None


def decode_value(dtype: int, payload: bytes) -> int | float | str | None:
    try:
        if dtype == 0x02 and len(payload) >= 2:
            return struct.unpack("<H", payload[:2])[0]
        if dtype == 0x10 and len(payload) >= 1:
            return payload[0]
        if dtype == 0x03 and len(payload) >= 4:
            return struct.unpack("<i", payload[:4])[0]
        if dtype == 0x04 and len(payload) >= 4:
            return round(struct.unpack("<f", payload[:4])[0], 6)
        if dtype == 0x05 and len(payload) >= 8:
            return round(struct.unpack("<d", payload[:8])[0], 9)
        if dtype == 0x1F:
            return parse_string(payload)
    except struct.error:
        return None
    return payload[:24].hex()


def tokenize_table(table: bytes) -> list[tuple]:
    """Yield (offset, field_id, dtype, mode, value, payload_len) records."""
    recs = []
    pos = 0
    while True:
        tp = table.find(TYPE_PREFIX, pos)
        if tp == -1:
            break
        dtype = table[tp + 4] if tp + 4 < len(table) else 0
        mode_b = table[tp + 5 : tp + 7]
        # layout: <field_id u16> 00 00 01 00 00 00 <kind u16> TYPE_PREFIX
        back = table[max(0, tp - 10) : tp]
        field_id = None
        if len(back) == 10 and back[2:8] == b"\x00\x00\x01\x00\x00\x00":
            field_id = struct.unpack("<H", back[0:2])[0]
        if mode_b == b"\x80\x01":
            val_start = tp + 7
            end = table.find(END_FIELD, val_start)
            payload = (
                table[val_start:end] if end != -1 else table[val_start : val_start + 32]
            )
            recs.append(
                (
                    tp,
                    field_id,
                    dtype,
                    "scalar",
                    decode_value(dtype, payload),
                    len(payload),
                )
            )
            pos = tp + 7
        elif mode_b == b"\xa0\x01":
            cnt = (
                struct.unpack("<I", table[tp + 7 : tp + 11])[0]
                if tp + 11 <= len(table)
                else 0
            )
            recs.append((tp, field_id, dtype, f"array[{cnt}]", None, cnt))
            pos = tp + 7
        else:
            recs.append((tp, field_id, dtype, f"mode={mode_b.hex()}", None, 0))
            pos = tp + 5
    return recs


def container_header(data: bytes) -> dict:
    """Parse the _db_format_1 header and its section directory at 0x50."""
    hdr = {
        "magic": data[2:17].decode("latin1"),
        "format": data[28:40].decode("latin1"),
        "sections": [],
    }
    pos = 0x50
    while pos + 14 <= len(data):
        if data[pos : pos + 2] != b"\xff\xff":
            break
        sid, off, size = struct.unpack("<HII", data[pos + 2 : pos + 12])
        if sid == 0 and off == 0:
            break
        hdr["sections"].append({"id": sid, "offset": off, "size": size})
        pos += 14
    return hdr


def cmd_header(args: argparse.Namespace) -> None:
    for path in args.files:
        path = Path(path)
        streams = []
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as z:
                streams = sorted(n for n in z.namelist() if n.endswith(".table"))
                blobs = {n: z.read(n) for n in streams}
        else:
            blobs = {path.name: path.read_bytes()}
        print(f"== {path.name}")
        for name, data in blobs.items():
            hdr = container_header(data)
            print(f"  {name}: {hdr['magic']!r} {hdr['format']!r}")
            for s in hdr["sections"]:
                print(
                    f"    section id={s['id']} offset={s['offset']:#x} "
                    f"size={s['size']:#x} ({s['size']:,})"
                )


def cmd_census(args: argparse.Namespace) -> None:
    data = load_stream(Path(args.files[0]), args.stream)
    tables = split_tables(data)
    print(f"{len(tables)} tables in stream_{args.stream}")
    for ti, t in enumerate(tables):
        cat = t[0:2].hex()
        recs = [r for r in tokenize_table(t) if r[1] is not None]
        if not args.values:
            ids = ",".join(f"{r[1]:04x}" for r in recs)
            print(f"T{ti:03d} cat={cat} len={len(t):6d} ids=[{ids[:160]}]")
            continue
        print(f"T{ti:03d} cat={cat} len={len(t)}")
        for off, fid, dt, mode, val, plen in recs:
            dt_name = DTYPE_NAMES.get(dt, f"{dt:02x}")
            vs = repr(val)
            if len(vs) > 80:
                vs = vs[:80] + "..."
            print(f"    +{off:06x} id={fid:04x} {dt_name:3s} {mode:10s} {vs}")


def cmd_crossref(args: argparse.Namespace) -> None:
    census: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    order: list[tuple] = []
    names = []
    for f in args.files:
        path = Path(f)
        names.append(path.name)
        data = load_stream(path, args.stream)
        for t in split_tables(data):
            cat = t[0:2].hex()
            for _off, fid, dt, mode, val, plen in tokenize_table(t):
                if fid is None:
                    continue
                key = (cat, f"{fid:04x}", DTYPE_NAMES.get(dt, f"{dt:02x}"))
                if key not in census:
                    order.append(key)
                v = val if val is not None else f"<{mode}:{plen}>"
                census[key][path.name].append(v)

    shown = 0
    for key in order:
        cat, fid, dt = key
        per_file = census[key]
        uniq = {json.dumps(v, default=str) for vals in per_file.values() for v in vals}
        varies = len(uniq) > 1
        if args.varying and not varies:
            continue
        if args.strings and dt != "str":
            continue
        shown += 1
        flag = "VARIES" if varies else "const "
        sample = next(iter(per_file.values()))[:3]
        print(
            f"cat={cat} fid={fid} {dt:3s} {flag} n_files={len(per_file)} "
            f"sample={sample!r}"
        )
    print(f"-- {shown} keys shown ({len(order)} total) across {len(names)} files")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("header", help="parse container header + section directory")
    p.add_argument("files", nargs="+")
    p.set_defaults(func=cmd_header)

    p = sub.add_parser("census", help="enumerate tables and fields in one stream")
    p.add_argument("files", nargs=1)
    p.add_argument("--stream", type=int, default=1)
    p.add_argument("--values", action="store_true", help="decode field values")
    p.set_defaults(func=cmd_census)

    p = sub.add_parser("crossref", help="compare field keys across files")
    p.add_argument("files", nargs="+")
    p.add_argument("--stream", type=int, default=1)
    p.add_argument("--varying", action="store_true", help="only keys that vary")
    p.add_argument("--strings", action="store_true", help="only string fields")
    p.set_defaults(func=cmd_crossref)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
