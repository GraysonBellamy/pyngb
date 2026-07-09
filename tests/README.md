# pyngb Test Suite

Tests for the pyngb library. The suite is anchored on two things: **six real
NGB fixtures** in `test_files/` (2022- and 2025-vintage Proteus runs,
including two baselines) and a **synthetic byte builder** that constructs
grammar-valid NGB files from scratch.

## Layout

- `conftest.py` — shared fixtures; `sample_ngb_file` is a builder-generated
  minimal NGB file
- `test_files/` — the six real fixtures (their presence is itself asserted,
  so a missing fixture fails loudly instead of silently skipping)
- `goldens/` — committed snapshots, two families per fixture:
  - `*.parity.json` — full metadata, column names/dtypes, per-column
    SHA-256 hashes, row counts. Pinned from pre-rewrite (0.3.x) output;
    guarantees parse results never drift. Zero tolerances — floats compare
    bitwise.
  - `*.census.json` — the tokenizer's own view: records by dtype, span
    kinds, byte coverage, type_refs, unknown-field census. A format-drift
    tripwire: files from a new Proteus version fail loudly and the diff is
    the mapping to-do list.
- `support/ngb_builder.py` — builds grammar-valid NGB bytes (fields, tables,
  section directories, whole files) plus corruption helpers. Its own test
  asserts builder↔tokenizer duality: `tokenize(build(x))` reproduces `x`.

## Test families

| Files | What they cover |
|-------|-----------------|
| `test_container.py`, `test_tokenizer.py`, `test_document.py` | The format layer bottom-up: section directory, record grammar walk, table/document assembly |
| `test_field_map.py`, `test_extract.py`, `test_channels.py` | Declarative maps, metadata extraction, channel assembly |
| `test_parity_goldens.py` | Parity snapshots through BOTH parse paths (`read_ngb` and `read_ngb_metadata`); fails (never skips) on missing goldens |
| `test_format_structural.py` | All fixtures × streams: byte coverage with every gap byte classified, census goldens, table-open invariants |
| `test_format_properties.py`, `test_property_based.py` | Hypothesis: build/tokenize round-trips (bitwise float equality), random-bytes and mutation fuzzing (any outcome other than a result or `NGBParseError` is a bug), coverage accounting |
| `test_corruption.py` | Corruption matrix: truncations, count overruns, directory corruption, oversized declarations → structured exceptions (types + attributes asserted, never message prose); each case first proves the uncorrupted input parses |
| Extraction pins: `test_temperature_program.py`, `test_temperature_calibration.py`, `test_reference_mass.py`, `test_reference_crucible_mass.py`, `test_run_environment.py`, `test_application_license.py`, `test_channel_attribution.py`, `test_column_metadata.py` | Field-level golden values on the real fixtures |
| `test_api.py`, `test_api_analysis.py`, `test_integration.py`, `test_workflows.py` | Public API surface and end-to-end flows |
| `test_cli_*.py` | `convert` / `inspect` / `validate` subcommands, incl. baseline and metadata flows |
| `test_batch.py`, `test_baseline.py`, `test_dtg.py`, `test_dsc_calibration.py`, `test_validation*.py` | Batch, baseline subtraction, analysis, validation |
| `test_performance.py` | Parse-time ceilings (marked `slow`) |
| `test_stress_and_edge_cases.py`, `test_exceptions.py`, `test_constants.py`, `test_util.py` | Edge cases and contracts |

## Running Tests

```bash
# Full suite with coverage (CI gate: fail_under = 86)
uv run pytest --cov=pyngb

# Skip slow tests (performance, stress)
uv run pytest -m "not slow"

# One file / one test, verbose
uv run pytest tests/test_tokenizer.py -v
uv run pytest tests/test_parity_goldens.py::test_full_parse_parity -v

# Show any skips (there should be none unexpected)
uv run pytest -rs
```

The other CI gates, runnable locally:

```bash
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
uv run mypy src/pyngb --ignore-missing-imports
uv run bandit -r src/
uv sync --resolution lowest-direct && uv run pytest   # dependency floors
```

## Regenerating goldens

```bash
uv run python scripts/make_goldens.py parity   # public-API snapshots
uv run python scripts/make_goldens.py census   # tokenizer-view snapshots
```

Golden diffs must always be intentional: a parity diff means parsed output
changed (explain why in the commit); a census diff means the tokenizer's
view of the fixtures changed. Never regenerate to make a red test green
without understanding the diff.

## Writing new tests

- Files `test_<area>.py`, classes `Test<Component>`, functions
  `test_<behavior>`.
- Synthetic inputs come from `tests.support.ngb_builder` — don't hand-craft
  NGB bytes inline.
- Corruption tests assert exception **types and structured attributes**
  (`stream`, `offset`, `declared`, …), not message text.
- Values extracted from the real fixtures get pinned exactly (no
  tolerances); if a fixture value is legitimately supposed to change, the
  golden regeneration explains it.
