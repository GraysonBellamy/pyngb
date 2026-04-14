# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
