# Scripts Directory

Development utilities for the pyngb project. All scripts are run from the
project root with `uv run` so the virtual environment is active.

## Scripts Overview

### `benchmarks.py`
Performance benchmarks for parsing, batch processing, and analysis functions.

```bash
uv run python scripts/benchmarks.py
```

### `discover_patterns.py`
Infers the (category, field) two-byte identifiers for new metadata values you
know exist inside `Streams/stream_1.table` of an NGB file. Use this when
adding a new field to `PatternConfig.metadata_patterns`.

### `inspect_stream1_metadata.py`
Quick dev utility to inspect stream_1 metadata fields with a general regex.

### `dump_masses.py`
Dumps mass-related fields from the test fixtures for cross-checking mass
disambiguation.

### `process_all_test_files.py`
Runs `BatchProcessor` over every `.ngb-ss3`/`.ngb-bs3` fixture in
`tests/test_files/`, producing CSV, Parquet, and metadata JSON per file plus a
`processing_summary.csv`.

## Testing

Testing is done exclusively with pytest:

```bash
# Run all unit tests
uv run pytest

# Run specific test modules
uv run pytest tests/test_validation.py tests/test_batch.py
```
