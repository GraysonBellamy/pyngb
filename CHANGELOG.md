# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
