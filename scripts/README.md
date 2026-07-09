# Scripts Directory

Development utilities for the pyngb project. All scripts are run from the
project root with `uv run` so the virtual environment is active.

Format inspection (structural census, field values, cross-file comparison)
lives in the CLI itself: see `pyngb inspect --help`.

## Scripts Overview

### `benchmarks.py`
Performance benchmarks for parsing, batch processing, and analysis functions.

```bash
uv run python scripts/benchmarks.py
```

### `make_goldens.py`
Regenerates the committed golden snapshots in `tests/goldens/`: `parity`
(public-API parse results per fixture) and `census` (structural census per
fixture). Regenerate only with justification — the goldens are the rewrite's
parity contract.

```bash
uv run python scripts/make_goldens.py parity
uv run python scripts/make_goldens.py census
```

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
