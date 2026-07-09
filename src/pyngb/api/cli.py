"""Command-line interface for pyNGB.

Three subcommands under the ``pyngb`` entry point:

- ``pyngb convert`` — parse NGB files to Parquet/CSV (optionally
  baseline-subtracted); the workhorse for data export.
- ``pyngb inspect`` — structural view of the parsed document: container
  sections, tables, field values, byte coverage, and the unknown-field
  census; with several files, a cross-file field comparison.
- ``pyngb validate`` — data-quality checks over the parsed measurement data.
"""

import argparse
import json
import logging
import sys
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..exceptions import NGBParseError
from ..format import DType, Mode, load_document
from ..format.census import document_census
from ..format.document import NGBDocument, Table
from ..validation import QualityChecker
from .loaders import read_ngb

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Create and configure the subcommand argument parser."""
    parser = argparse.ArgumentParser(
        prog="pyngb", description="Work with NETZSCH STA NGB files"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    convert = sub.add_parser("convert", help="Parse NGB files and export Parquet/CSV")
    convert.add_argument("input", nargs="+", help="Input NGB file path(s)")
    convert.add_argument("-o", "--output", help="Output directory", default=".")
    convert.add_argument(
        "-f",
        "--format",
        choices=["parquet", "csv", "both"],
        default="parquet",
        help="Output format",
    )
    convert.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    convert.add_argument(
        "-b", "--baseline", help="Baseline file path for baseline subtraction"
    )
    convert.add_argument(
        "--dynamic-axis",
        choices=["time", "sample_temperature", "furnace_temperature"],
        default="sample_temperature",
        help="Axis for dynamic segment alignment during baseline subtraction (default: sample_temperature)",
    )

    inspect = sub.add_parser(
        "inspect",
        help="Show document structure: sections, tables, coverage, unknown fields",
    )
    inspect.add_argument("input", nargs="+", help="Input NGB file path(s)")
    inspect.add_argument(
        "--stream",
        type=int,
        default=1,
        help="Stream number for the table listing / cross-file comparison (default: 1)",
    )
    inspect.add_argument(
        "--values",
        action="store_true",
        help="Show decoded field values in the table listing",
    )
    inspect.add_argument(
        "--unknown",
        action="store_true",
        help="Show only the unknown-field census (the format-mapping to-do list)",
    )
    inspect.add_argument(
        "--coverage",
        action="store_true",
        help="Show only byte-coverage accounting (gap bytes and span kinds)",
    )
    inspect.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON"
    )

    validate = sub.add_parser("validate", help="Run data-quality checks on NGB files")
    validate.add_argument("input", nargs="+", help="Input NGB file path(s)")
    validate.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON"
    )

    return parser


def validate_input_file(input_path: Path) -> None:
    """Validate input NGB file exists and is valid.

    Args:
        input_path: Path to input NGB file

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path is not a file
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    if not input_path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")

    # Check if it's a valid NGB file extension
    valid_extensions = {".ngb-ss3", ".ngb-bs3"}
    if input_path.suffix.lower() not in valid_extensions:
        logger.warning(
            f"File extension '{input_path.suffix}' may not be a standard NGB format. Proceeding anyway."
        )


def validate_baseline_file(baseline_path: Path) -> None:
    """Validate baseline file exists and is valid.

    Args:
        baseline_path: Path to baseline NGB file

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path is not a file
    """
    if not baseline_path.exists():
        raise FileNotFoundError(f"Baseline file does not exist: {baseline_path}")

    if not baseline_path.is_file():
        raise ValueError(f"Baseline path is not a file: {baseline_path}")

    valid_extensions = {".ngb-ss3", ".ngb-bs3"}
    if baseline_path.suffix.lower() not in valid_extensions:
        logger.warning(
            f"Baseline file extension '{baseline_path.suffix}' may not be a standard NGB format. Proceeding anyway."
        )


def validate_output_directory(output_path: Path) -> None:
    """Validate output directory is writable.

    Args:
        output_path: Path to output directory

    Raises:
        PermissionError: If directory is not writable
        OSError: If directory cannot be created
    """
    # Create directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Test write permissions by creating a temporary file
    test_file = output_path / ".write_test"
    try:
        test_file.touch()
        test_file.unlink()
    except (PermissionError, OSError) as e:
        raise PermissionError(
            f"Cannot write to output directory {output_path}: {e}"
        ) from e


def load_data(
    input_file: str, baseline_file: str | None, dynamic_axis: str
) -> pa.Table:
    """Load NGB data with optional baseline subtraction.

    Args:
        input_file: Path to input NGB file
        baseline_file: Optional path to baseline NGB file
        dynamic_axis: Axis for dynamic segment alignment

    Returns:
        PyArrow Table with loaded data
    """
    if baseline_file:
        logger.info(
            f"Loading data with baseline subtraction (dynamic_axis={dynamic_axis})"
        )
        return read_ngb(
            input_file, baseline_file=baseline_file, dynamic_axis=dynamic_axis
        )
    return read_ngb(input_file)


def write_output_files(
    data: pa.Table,
    output_path: Path,
    base_name: str,
    output_format: str,
) -> None:
    """Write parsed data to output file(s).

    Args:
        data: PyArrow Table to write
        output_path: Directory to write files to
        base_name: Base filename (without extension)
        output_format: Output format ("parquet", "csv", or "both")
    """
    if output_format in ("parquet", "both"):
        parquet_file = output_path / f"{base_name}.parquet"
        pq.write_table(data, parquet_file, compression="snappy")
        logger.debug(f"Wrote Parquet file: {parquet_file}")

    if output_format in ("csv", "both"):
        # Optimize: Only convert to Polars when needed for CSV output
        df = pl.from_arrow(data)
        # Ensure we have a DataFrame for CSV writing
        if isinstance(df, pl.DataFrame):
            csv_file = output_path / f"{base_name}.csv"
            df.write_csv(csv_file)
            logger.debug(f"Wrote CSV file: {csv_file}")


def process_file(
    input_file: str,
    output_path: Path,
    output_format: str,
    baseline_file: str | None,
    dynamic_axis: str,
) -> None:
    """Parse one NGB file and write its output file(s).

    Args:
        input_file: Path to input NGB file
        output_path: Validated output directory
        output_format: Output format ("parquet", "csv", or "both")
        baseline_file: Optional path to baseline NGB file
        dynamic_axis: Axis for dynamic segment alignment

    Raises:
        Anything read_ngb or the filesystem raises; the caller decides
        whether one failure aborts the run.
    """
    input_path = Path(input_file)
    validate_input_file(input_path)

    data = load_data(input_file, baseline_file, dynamic_axis)

    base_name = input_path.stem
    # Add suffix to indicate baseline subtraction was performed
    if baseline_file:
        base_name += "_baseline_subtracted"

    write_output_files(data, output_path, base_name, output_format)

    if baseline_file:
        logger.info(
            f"Successfully parsed {input_file} with baseline subtraction from {baseline_file}"
        )
    else:
        logger.info(f"Successfully parsed {input_file}")


def cmd_convert(args: argparse.Namespace) -> int:
    """Run the convert subcommand: parse files and write outputs."""
    # Validate shared inputs once; failures here abort the whole run.
    try:
        if args.baseline:
            validate_baseline_file(Path(args.baseline))
        output_path = Path(args.output)
        validate_output_directory(output_path)
    except (FileNotFoundError, ValueError, PermissionError, OSError) as e:
        logger.error(str(e))
        return 1

    # Per-file failures don't stop the remaining files.
    failures = 0
    for input_file in args.input:
        try:
            process_file(
                input_file, output_path, args.format, args.baseline, args.dynamic_axis
            )
        except zipfile.BadZipFile:
            logger.error(f"{input_file} is not a valid NGB file (not a ZIP archive)")
            failures += 1
        except (FileNotFoundError, ValueError, PermissionError) as e:
            logger.error(str(e))
            failures += 1
        except NGBParseError as e:
            logger.error(f"Failed to parse {input_file}: {e}")
            failures += 1
        except OSError as e:
            logger.error(f"OS error while processing file {input_file}: {e}")
            failures += 1

    if failures:
        logger.error(f"{failures} of {len(args.input)} file(s) failed")
        return 1
    return 0


def _field_value_repr(field: Any) -> str:
    """Short human-readable rendering of one field's value."""
    if field.mode is Mode.ARRAY:
        return f"array[{field.element_count}]"
    value = repr(field.value)
    if len(value) > 80:
        value = value[:77] + "..."
    return value


def _print_table_listing(doc: NGBDocument, stream: int, values: bool) -> None:
    tables = doc.tables_of(stream)
    print(f"{len(tables)} tables in stream_{stream}")
    for table in tables:
        ids = " ".join(f"{fid:04x}" for fid in table.fields)
        if len(ids) > 120:
            ids = ids[:117] + "..."
        print(
            f"  T{table.index:03d} cat={table.category:04x} "
            f"type={table.type_ref:04x} fields={len(table.fields)} [{ids}]"
        )
        if values:
            for field in table.fields.values():
                print(
                    f"    id={field.field_id:04x} {DType(field.dtype).name:<7} "
                    f"{_field_value_repr(field)}"
                )


def _print_header_view(doc: NGBDocument) -> None:
    for stream_id in sorted(doc.streams):
        stream = doc.streams[stream_id]
        print(f"stream_{stream_id}: {len(stream.raw):,} bytes")
        for entry in stream.sections:
            print(
                f"  section id={entry.section_id} offset=0x{entry.offset:06x} "
                f"size={entry.size:,}"
            )


def _print_coverage(census: dict[str, Any]) -> None:
    for stream_id, stream_census in census["streams"].items():
        spans = ", ".join(
            f"{kind}={count}" for kind, count in stream_census["spans_by_kind"].items()
        )
        print(
            f"stream_{stream_id}: {stream_census['tables']} tables, "
            f"gap_bytes={stream_census['gap_bytes']:,} ({spans or 'no spans'}), "
            f"orphans={stream_census['orphan_fields']}"
        )


def _print_unknown(census: dict[str, Any]) -> None:
    total = 0
    for stream_id, entries in census["unknown_fields"].items():
        for entry in entries:
            print(f"stream_{stream_id} {entry}")
            total += 1
    print(f"{total} unknown category/field/dtype triple(s)")


def _table_scalar_values(table: Table) -> list[Any]:
    return [
        field.value
        for field in table.fields.values()
        if field.mode is Mode.SCALAR and field.value is not None
    ]


def _crossref(
    docs: dict[str, NGBDocument], stream: int
) -> dict[str, dict[str, list[Any]]]:
    """(category/field/dtype) -> per-file scalar values, for field comparison."""
    grid: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for name, doc in docs.items():
        for table in doc.tables_of(stream):
            for field in table.fields.values():
                if field.mode is not Mode.SCALAR or field.value is None:
                    continue
                key = (
                    f"0x{table.category:04x}/0x{field.field_id:04x}/0x{field.dtype:02x}"
                )
                grid[key][name].append(field.value)
    return {key: dict(per_file) for key, per_file in sorted(grid.items())}


def _print_crossref(grid: dict[str, dict[str, list[Any]]], n_files: int) -> None:
    for key, per_file in grid.items():
        distinct = {json.dumps(vals, default=str) for vals in per_file.values()}
        varies = "VARIES" if len(distinct) > 1 or len(per_file) < n_files else "const "
        sample = next(iter(per_file.values()))[:3]
        sample_repr = repr(sample)
        if len(sample_repr) > 60:
            sample_repr = sample_repr[:57] + "..."
        print(f"{key} {varies} n_files={len(per_file)} sample={sample_repr}")
    print(f"-- {len(grid)} field keys across {n_files} files")


def cmd_inspect(args: argparse.Namespace) -> int:
    """Run the inspect subcommand: structural views of parsed documents."""
    docs: dict[str, NGBDocument] = {}
    failures = 0
    for input_file in args.input:
        try:
            docs[input_file] = load_document(input_file)
        except (FileNotFoundError, zipfile.BadZipFile, NGBParseError) as e:
            logger.error(f"Failed to load {input_file}: {e}")
            failures += 1
    if not docs:
        return 1

    if len(docs) > 1:
        # Cross-file field comparison over the requested stream.
        grid = _crossref(docs, args.stream)
        if args.json:
            print(json.dumps({"stream": args.stream, "fields": grid}, default=str))
        else:
            _print_crossref(grid, len(docs))
        return 1 if failures else 0

    name, doc = next(iter(docs.items()))
    census = document_census(doc)

    if args.json:
        payload: dict[str, Any] = {"file": name, **census}
        print(json.dumps(payload, sort_keys=True))
        return 1 if failures else 0

    print(f"== {name}")
    if args.unknown:
        _print_unknown(census)
    elif args.coverage:
        _print_coverage(census)
    else:
        _print_header_view(doc)
        if args.stream in doc.streams:
            _print_table_listing(doc, args.stream, args.values)
        else:
            print(f"stream_{args.stream} not present")
    return 1 if failures else 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Run the validate subcommand: quality checks over parsed data."""
    failures = 0
    reports: list[dict[str, Any]] = []
    for input_file in args.input:
        try:
            metadata, table = read_ngb(input_file, return_metadata=True)
        except (
            FileNotFoundError,
            ValueError,
            zipfile.BadZipFile,
            NGBParseError,
            OSError,
        ) as e:
            logger.error(f"Failed to parse {input_file}: {e}")
            failures += 1
            reports.append({"file": input_file, "parse_error": str(e)})
            continue

        result = QualityChecker(table, metadata).full_validation()
        if not result.is_valid:
            failures += 1
        reports.append({"file": input_file, **result.summary()})
        if not args.json:
            print(f"== {input_file}")
            print(result.report())

    if args.json:
        print(json.dumps(reports, default=str))
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    """Command-line interface for pyngb.

    Usage:
        pyngb convert FILE... [-o DIR] [-f parquet|csv|both] [-b BASELINE]
        pyngb inspect FILE... [--stream N] [--values] [--unknown] [--coverage] [--json]
        pyngb validate FILE... [--json]

    Examples:
        # Parse to Parquet (default)
        pyngb convert sample.ngb-ss3

        # Parse to CSV with verbose logging
        pyngb convert sample.ngb-ss3 -f csv -v

        # Baseline-subtract every input against the same baseline
        pyngb convert *.ngb-ss3 -b baseline.ngb-bs3

        # Structural inspection and cross-file field comparison
        pyngb inspect sample.ngb-ss3 --stream 1 --values
        pyngb inspect a.ngb-ss3 b.ngb-ss3 --stream 1

        # Data-quality checks
        pyngb validate sample.ngb-ss3 --json

    Args:
        argv: Command-line arguments (defaults to sys.argv)

    Returns:
        Exit code (0 when every file succeeded, 1 otherwise)
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Configure logging
    verbose = getattr(args, "verbose", False)
    logging.basicConfig(level=(logging.DEBUG if verbose else logging.INFO))

    commands = {
        "convert": cmd_convert,
        "inspect": cmd_inspect,
        "validate": cmd_validate,
    }
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
