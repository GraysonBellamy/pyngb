# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - Unreleased

The parsing backbone is rewritten from the ground up: per-field regex
hunting over raw stream bytes is replaced by a strict record-grammar
tokenizer and a queryable document layer. Every byte of every stream section
is now either a decoded record or an explicitly classified span — nothing is
silently skipped, and unknown fields are an enumerable census instead of a
search problem. **Parsed output is byte-identical to 0.3.0 on all six test
fixtures**, pinned by parity goldens with zero tolerances through both parse
paths. The public API is redesigned around the new model; this is a breaking
release with no compatibility shims. Breaking changes are marked
**breaking**.

### Added

- `read_ngb_metadata(path, *, limits=None)`: the metadata-only fast path
  (stream 1 only; replaces `NGBParser.parse_metadata`). Used by
  `NGBDataset`/batch metadata operations.
- Public document layer: `load_document(path, *, streams=None, limits=None)`
  returns an `NGBDocument` — every table and field of every stream
  (including the otherwise-unread streams 4–6) with query helpers
  (`find`/`first`/`by_category`), byte-coverage accounting, and
  `unknown_fields()`, the census of everything pyngb does not yet map.
  `NGBDocument`, `Table`, and `Field` are exported from the package root.
- `pyngb inspect FILE... [--stream N] [--values] [--unknown] [--coverage]
  [--json]`: document structure, byte coverage, and unknown-field census;
  with multiple files it cross-references scalar fields and reports
  differences. `pyngb validate FILE... [--json]`: data-quality checks with
  a non-zero exit code on invalid files.
- `limits: ParsingConfig | None` keyword on `read_ngb`, `read_ngb_metadata`,
  and `load_document` (replaces reaching `ParsingConfig` in through
  `NGBParser`).
- Container integrity checks: the section directory of every stream is
  parsed and validated (entry framing, section contiguity, EOF closure,
  main-section presence) before any content parsing; violations raise
  `NGBCorruptedFileError`.
- Structured attributes on corruption/limit exceptions
  (`stream`, `offset`, `table_index`, `declared`, `available`, `limit`)
  instead of prose-only messages.
- All nine observed data types are decoded (u16, i32, f32, f64, u8/bytes,
  packed-8, object-ref, string, hash-16); array element counts are validated
  against declared sizes before allocation.

### Changed

- **breaking:** the CLI is subcommand-based. `pyngb convert FILE...` replaces
  the bare `pyngb FILE...` invocation (all flags unchanged under `convert`);
  `pyngb inspect` and `pyngb validate` are new. A bare `pyngb FILE...` is
  now an error.
- **breaking:** structural corruption in data streams (malformed or
  truncated records) now always raises `NGBCorruptedFileError` before
  channel assembly. Metadata-stream grammar violations warn and continue —
  every `FileMetadata` field remains optional.
- Performance on the largest fixture (398 KB, medians): full parse ~30 ms
  (was ~30 ms), metadata-only ~18 ms (was ~12 ms) — within the review budget;
  parity, not speed, was the goal of this release.

### Removed

- **breaking:** `NGBParser` and `PatternConfig`. Call sites become function
  calls: `NGBParser().parse(path)` → `read_ngb(path, return_metadata=True)`,
  `NGBParser().parse_metadata(path)` → `read_ngb_metadata(path)`.
- **breaking:** the `pyngb.binary`, `pyngb.extractors`, and `pyngb.core`
  subpackages and their contents (`BinaryParser`, `DataTypeRegistry` and all
  data-type handlers, `MetadataExtractor`, `DataStreamProcessor`,
  `StreamTables`), plus the marker/pattern constants (`BinaryMarkers`,
  `StreamMarkers`, `DataType`, `DataTypeSizes`, `BinaryProcessing`,
  `PatternOffsets`). The record grammar in `pyngb.format` is the single
  extraction substrate.
- **breaking:** the reverse-engineering helper scripts
  (`ngb_deep_inspect.py`, `discover_patterns.py`,
  `inspect_stream1_metadata.py`, `dump_masses.py`) — superseded by
  `pyngb inspect` and the document layer.

## [0.3.0] - 2026-07-06

A metadata-completeness release driven by a deep investigation of the NGB
binary format across all test fixtures. One extraction bug is fixed (dropped
temperature-calibration fixpoints) and several new provenance fields are
extracted. Breaking changes are marked **breaking**.

### Fixed

- Temperature-calibration fixpoints beyond the fifth are no longer silently
  dropped. The fixpoint scan now covers categories `30 75` .. `3f 75` instead
  of stopping at `34 75`; real files carry 6-9 standards, so every fixture
  previously lost its high-temperature fixpoints (Ag2SO4, CsCl, K2CrO4,
  BaCO3). All recovered fixpoints were verified against the calibration
  polynomial round-trip (residual < 3e-5) and ascend in actual temperature.

### Added

- Calibration provenance on both calibration blocks: `temperature_calibration`
  now also carries `date_measured` (ISO 8601 UTC), `gas`, `crucible_type`,
  `heating_rate` (K/min), and `comment` from the calibration run that produced
  it, read from the `f5 01` source table of the external record.
- New `sensitivity_calibration` metadata block with the same provenance fields
  plus `record_path` (the external `.ngb-es3` record), backed by the
  `SensitivityCalibration` TypedDict.
- Run-environment metadata: `timezone` (Windows timezone name active on the
  instrument PC) and `utc_offset_minutes` (DST-aware), read from the `59 18`
  snapshot table. `date_performed` is UTC; these recover the local wall-clock
  time of the run.
- `correction_file_path`: the correction file selected in the measurement
  definition (`70 17` table, field `43 08`). For sample runs this identifies
  the matching `.ngb-bs3` baseline file.
- MFC flow setpoints: `purge_1_mfc_flow`, `purge_2_mfc_flow`, and
  `protective_mfc_flow` (ml/min), read from the `*_LastUsedFlow`
  device-parameter tables. For MFC channels without a data column in the file
  these are the only record of the flow. Extracted by the new
  `RunEnvironmentExtractor` and an extended `MFCExtractor`.
- `scripts/ngb_deep_inspect.py`: reverse-engineering toolkit with `header`
  (container header + section directory), `census` (every table and field in
  a stream), and `crossref` (field comparison across files) subcommands.

### Changed

- **breaking** `sensitivity_record_path` (top-level string) is replaced by
  `sensitivity_calibration["record_path"]`.

## [0.2.0] - 2026-07-03

A correctness- and hardening-focused release driven by a full source audit.
Several fixes change output for files that were previously parsed silently
wrong (see the channel-attribution and baseline-subtraction entries). The
public API surface was trimmed; breaking changes are marked **breaking**.

### Added

- Temperature-calibration metadata is now extracted from `stream_1` into the new
  `temperature_calibration` field (`coefficients`, `fixpoints`, `record_path`) plus
  a top-level `sensitivity_record_path`. Each fixpoint carries the Proteus
  calibration-table columns `name`, `actual_c`, `measured_c`, `weight`, and
  `corrected_c`, where `corrected_c = measured_c + (1e-3*B0 + 1e-5*B1*T + 1e-8*B2*T²)`
  using the extracted coefficients (verified by round-trip on all sample files).
  Backed by the new `TemperatureCalibration` and `TemperatureFixpoint` TypedDicts
  and `TemperatureCalibrationExtractor`. These values are captured for
  traceability/QA only — the `sample_temperature` channel is already
  temperature-corrected by Proteus, so the coefficients are never re-applied to
  the data.
- Metadata-only parse mode: `NGBParser.parse_metadata(path)` reads and extracts
  `stream_1` only. `NGBDataset.summary()`, `export_metadata()`, and
  `filter_by_metadata()` use it (~30% faster per file), with a parity test
  guaranteeing it never drifts from the full parse.
- Resource limits on untrusted input are now enforced instead of decorative:
  each ZIP member's declared decompressed size is checked before decompression
  (closing the decompression-bomb hole), `max_array_size_mb` is enforced in the
  new public `BinaryParser.parse_data()`, and `NGBParser` accepts a
  `ParsingConfig` so the limits are reachable from the main entry point.
- Structural corruption in data streams now raises `NGBCorruptedFileError`
  (truncated tables, payload/element-count mismatches, unknown data types,
  channel-length mismatches) instead of degrading to debug logs and silently
  missing or short columns.
- The CLI accepts multiple input files (`pyngb *.ngb-ss3`) with per-file error
  isolation, and a non-ZIP input now reports "not a valid NGB file (not a ZIP
  archive)" instead of a traceback.
- `apply_dsc_calibration` is exported from the package root (it was documented
  as public but reachable only via a deep module path).
- `NGBDataset` and `ValidationResult` gained `__repr__`.

### Fixed

- **Channel attribution (data correctness):** `stream_2` channels are now named
  by the header that precedes them instead of a shifted `column_map` that only
  matched files with the canonical channel sequence. In files without the
  purge-2 MFC channel, the purge-1 flow was published as `purge_flow_2` (whose
  MFC metadata says oxygen), misrepresenting pyrolysis runs as oxidizing
  atmospheres. Flow labels are now pinned against MFC gas metadata on all
  fixtures.
- **Baseline subtraction (data correctness):** dynamic segments now interpolate
  only against the baseline rows of the same temperature-program stage (the old
  full-run `np.interp` on a non-monotonic temperature axis returned nonsense
  without error), with a time-axis fallback when the requested axis is not
  essentially monotonic. Output now always has one row per input row — rows at
  a program's exact end and post-program cooling tails were silently dropped.
- `application_version` and `licensed_to` extracted `None` on every file ever
  parsed: both string filters were written with doubled backslashes inside raw
  strings and rejected every candidate.
- String decoding: strict UTF-8 with a strict UTF-16LE fallback replaces
  UTF-8-with-`errors="ignore"`, which silently stripped UTF-16LE payloads to
  ASCII (`'Müller'` → `'Mller'`); undecodable payloads return `None` instead of
  mangled text. Path walk-back accepts printable Latin-1, so paths containing
  characters like umlauts are no longer truncated.
- DSC peak detection rewritten on `scipy.signal.find_peaks` with a robust
  median/MAD dual criterion: a flat trace with tiny noise previously reported
  thousands of "peaks" in O(n²) time.
- `dtg`/`dtg_custom` validate input up front (finite, strictly increasing time)
  instead of smearing NaN/inf across the smoothing window on duplicate
  timestamps or null mass.
- DSC calibration masks samples where the sensitivity curve is not meaningfully
  positive (out-of-range extrapolation could turn 1 µV into −24,596 mW) and
  raises `ValueError` on a zero calibration constant instead of dividing by zero.
- Validation reports degenerate data instead of crashing on it: empty frames,
  all-null columns, and single-row data are findings, not `TypeError`s; a
  single null no longer poisons time-direction, outlier, or
  temperature-constancy checks; a validator crash becomes a finding rather
  than aborting the run. The always-true column-length check that inflated
  `checks_passed` is gone, and reports are plain text instead of emoji.
- `BatchProcessor` honors `skip_errors=False` in the parallel path (strict mode
  previously only worked with `max_workers=1`) and parallel results are
  returned in input order, matching sequential mode.
- `read_ngb()` initializes column metadata (units, processing history, source)
  on every path — `return_metadata=True` without a baseline used to skip it —
  and validates `dynamic_axis` up front on every call.
- Baseline stage comparison type-guards non-numeric stage values and uses a
  relative tolerance suited to float32-derived durations.

### Changed

- **breaking:** dependency floors are now verified minimums — `polars>=1.6`,
  `pyarrow>=14`, `numpy>=1.24`, `scipy>=1.13.1` — and a CI job runs the suite
  on exactly those versions to keep them honest (the old `polars>=0.20` floor
  could never even import). The `test`/`dev`/`performance`/`visualization`
  extras were deleted; dev tooling lives in `[dependency-groups]`. The docs
  extra now installs `zensical` (the docs build migrated off mkdocs-material).
  PEP 639 license metadata, `Typing :: Typed`, and Python 3.14 classifiers.
- **breaking:** `ParsingConfig.max_file_size_mb` is renamed
  `max_stream_size_mb` (it limits a decompressed stream, not the on-disk
  archive); the never-consumed `encoding_fallback` field is deleted.
- **breaking:** the CLI format choice `all` is renamed `both`, unifying on
  `BatchProcessor`'s vocabulary.
- Performance: data arrays decode as NumPy end-to-end (~80× faster on a path
  that was roughly a third of parse time), MFC extraction anchors on its full
  record layout instead of three byte-by-byte Python scans (~20 ms/file),
  column-metadata initialization casts once instead of once per column, file
  hashing streams in 1 MiB chunks, `stream_1` is joined once and shared across
  extractors, and the baseline path no longer parses and hashes the sample
  file twice. Fixture output is byte-identical throughout.
- Parsing a full fixture now takes tens of milliseconds; docs and docstrings
  updated to match.

### Removed

- **breaking:** the dead configuration system — `ValidationConfig`,
  `BatchConfig`, `PyNGBConfig`, `DEFAULT_CONFIG`, `ValidationThresholds`, and
  the `config_validation` module. Only `ParsingConfig` was ever consumed; its
  one useful check moved into `PatternConfig.__post_init__`.
- **breaking:** the standalone `subtract_baseline()` function.
  `read_ngb(path, baseline_file=...)` is the one public path (and unlike the
  standalone function it also tags column metadata). `baseline.py` now holds
  only `Segment` and `BaselineSubtractor`, operating purely on parsed data.
- **breaking:** dead exceptions with no raiser: `NGBBaselineError`,
  `NGBValidationError`, `NGBConfigurationError`, `NGBUnsupportedVersionError`,
  `NGBMetadataExtractionError`. Every remaining `NGBParseError` subclass is
  genuinely a parse error.
- **breaking:** `BinaryMarkers` and `DataType` are no longer package-root
  exports (still importable from `pyngb.constants`); the field-less
  `FileMetadataRequired` TypedDict and the deprecated `api.loaders.main`
  wrapper are deleted.

## [0.1.2] - 2026-04-14

### Added

- `BatchResult` TypedDict describing the per-file outcome of batch processing, re-exported from the package root.
- `NGBResourceLimitError` is now raised from `BinaryParser.split_tables()` when a stream exceeds `ParsingConfig.max_tables_per_stream`. The default limit was tuned to 10,000 after measuring real files (max observed: 545), so no legitimate input is affected.
- `BinaryParser` now accepts an optional `parsing_config: ParsingConfig` argument.

### Changed

- `BinaryParser.parse_value()` return type narrowed from `Any` to `int | float | str | bytes | None`; struct results are explicitly cast.
- Library-level loggers in `baseline`, `batch`, `config_validation`, `util/hashing`, `util/columns`, and `validation/base` now attach a `NullHandler`, matching the rest of the codebase and PEP 282 guidance.
- `BatchProcessor._setup_logging()` no longer skips its StreamHandler when a `NullHandler` is present.
- `parse_value`'s unexpected-exception fallback now logs at `warning` instead of `debug` so silent binary corruption stays visible.
- CLI error handling replaced `except Exception` + `match`/`case` with explicit clauses for `FileNotFoundError`/`ValueError`/`PermissionError`, `NGBParseError`, `ImportError`, and `OSError`. Unknown exceptions now propagate.
- Batch worker-result collector uses `logger.exception()` so subprocess tracebacks survive.

### Fixed

- Overload signatures of `read_ngb()` now document the correct `dynamic_axis` default (`"sample_temperature"`), matching the runtime default and the docstring.

## [0.1.1] - 2025-01-06

### Changed

- All filesystem-related functions now accept both `str` and `pathlib.Path` objects
- Migrated entire codebase to use `pathlib.Path` instead of strings for filesystem operations
- Updated `read_ngb()` to accept `Path` objects for both `path` and `baseline_file` parameters
- Updated `get_hash()` to accept `Path` objects and use `path.open()` instead of `open()`
- Updated `NGBParser.parse()` to accept `Path` objects
- Updated `subtract_baseline()` to accept `Path` objects for both file parameters
- Removed unnecessary `str()` conversions throughout the codebase

### Fixed

- Fixed DSC calibration tests to use Polars instead of pandas (removed implicit pandas dependency)
- Fixed test mocking to use `pathlib.Path.open` instead of `builtins.open`
- Fixed `os.unlink()` usage in tests to use `Path.unlink()`
- Updated test edge cases to handle `Path("")` behavior correctly

### Improved

- Better type safety with `str | Path` type hints throughout the API
- More consistent filesystem operations using Path methods
- Cleaner code with fewer string conversions
- All examples and scripts updated to demonstrate pathlib best practices

### Backwards Compatibility

All changes maintain full backwards compatibility - existing code using string paths will continue to work without modification.

## [0.1.0] - Initial Release

Initial release with core NGB parsing functionality.
