"""
Quick dev utility to inspect metadata in stream_1.table using a general regex.

Usage examples:
    uv run python scripts/inspect_stream1_metadata.py /path/to/file.ngb-ss3 --json
    uv run python scripts/inspect_stream1_metadata.py /path/to/Streams/stream_1.table --limit 20

Notes:
- This script does NOT modify library code. It calls private extractors for
  temperature program and calibration constants to enrich the output.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any


def _ensure_src_on_path() -> None:
    # Add src/ to sys.path for src-layout imports when running as a script.
    repo_root = Path(__file__).resolve().parents[1]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


_ensure_src_on_path()

from pyngb.binary import BinaryParser  # noqa: E402
from pyngb.constants import PatternConfig  # noqa: E402
from pyngb.extractors import MetadataExtractor  # noqa: E402


def extract_all_metadata_generic(stream_data: bytes) -> dict[str, Any]:
    """Universal metadata extractor using a general regex to capture all fields.

    Returns a dict where keys are composite identifiers like "7517_5910" and
    values are parsed scalars or strings. Adds temperature program and
    calibration constants when present.
    """
    parser = BinaryParser()
    markers = parser.markers

    # General pattern for standard metadata fields:
    # category (2B) ... field (2B) ... TYPE_PREFIX + dt (1B) + TYPE_SEPARATOR + value (.+?) + END_FIELD
    general_pattern = re.compile(
        rb"(.{2}).+?(.{2}).+?"
        + re.escape(markers.TYPE_PREFIX)
        + rb"(.)"
        + re.escape(markers.TYPE_SEPARATOR)
        + rb"(.*?)"
        + re.escape(markers.END_FIELD),
        re.DOTALL,
    )

    metadata: dict[str, list[Any]] = {}

    for match in general_pattern.finditer(stream_data):
        category_bytes = match.group(1)
        field_bytes = match.group(2)
        data_type = match.group(3)
        value_bytes = match.group(4)

        key = f"{category_bytes.hex()}_{field_bytes.hex()}"
        parsed_value = parser.parse_value(data_type, value_bytes)

        if key not in metadata:
            metadata[key] = []
        metadata[key].append(parsed_value)

    # Enrich with temperature program and calibration constants using existing extractors
    config = PatternConfig()
    extractor = MetadataExtractor(config, parser)
    # Split tables for section-specific extraction
    tables = parser.split_tables(stream_data)

    # Temperature program: works on combined data or single tables; we call on combined
    extractor._extract_temperature_program(stream_data, metadata)  # type: ignore[arg-type]

    # Calibration constants: per-table
    for table in tables:
        extractor._extract_calibration_constants(table, metadata)  # type: ignore[arg-type]

    # Flatten lists where there are no duplicates
    for k in list(metadata.keys()):
        v = metadata[k]
        if isinstance(v, list) and len(v) == 1:
            metadata[k] = v[0]

    return metadata


def _load_stream1_bytes(input_path: Path) -> bytes:
    # If pointing at a ZIP-like NGB, open and read Streams/stream_1.table.
    # Otherwise treat as raw binary table file.
    lower = input_path.name.lower()
    if lower.endswith((".ngb-ss3", ".ngb", ".zip")):
        with zipfile.ZipFile(str(input_path), "r") as z:
            return z.read("Streams/stream_1.table")
    return input_path.read_bytes()


def _to_serializable(obj: Any) -> Any:
    """Recursively convert values to JSON-serializable forms.

    - bytes => hex string with 0x prefix
    - tuples => lists
    - dict/list => recurse
    """
    if isinstance(obj, bytes):
        return "0x" + obj.hex()
    if isinstance(obj, tuple):
        return [_to_serializable(x) for x in obj]
    if isinstance(obj, list):
        return [_to_serializable(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    return obj


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Inspect metadata in stream_1.table")
    p.add_argument("input", help="Path to .ngb-ss3 file or to Streams/stream_1.table")
    p.add_argument("--json", action="store_true", help="Print full JSON output")
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of top-level keys printed in text mode (0 = no limit)",
    )
    args = p.parse_args(argv)

    path = Path(args.input)
    if not path.exists():
        print(f"Input not found: {path}")
        return 2

    stream = _load_stream1_bytes(path)
    meta = extract_all_metadata_generic(stream)

    if args.json:
        print(json.dumps(_to_serializable(meta), indent=2, ensure_ascii=False))
        return 0

    # Human-readable preview
    print("Discovered metadata keys:")
    keys = list(meta.keys())
    keys.sort()
    for shown, k in enumerate(keys, start=1):
        v = meta[k]
        if isinstance(v, (dict, list)):
            sv = _to_serializable(v)
            s = json.dumps(sv, ensure_ascii=False)
            preview = s[:200] + ("…" if len(s) > 200 else "")
        else:
            sv = _to_serializable(v)
            preview = str(sv)
        print(f"- {k}: {preview}")
        if args.limit and shown >= args.limit:
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
