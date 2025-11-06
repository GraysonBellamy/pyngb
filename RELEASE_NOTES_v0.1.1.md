# Release Notes: pyngb v0.1.1

## 🎉 What's New

This release modernizes the codebase by migrating from string-based filesystem operations to `pathlib.Path` throughout the entire package, providing better type safety and more consistent code.

## ✨ Major Changes

### Pathlib Migration

All filesystem-related functions now accept both `str` and `pathlib.Path` objects:

- **`read_ngb(path, baseline_file)`** - Both parameters accept Path objects
- **`get_hash(path)`** - Accepts Path objects and uses `path.open()` internally
- **`NGBParser.parse(path)`** - Accepts Path objects for file parsing
- **`subtract_baseline(sample_file, baseline_file)`** - Both parameters accept Path objects

### Example Usage

```python
from pathlib import Path
from pyngb import read_ngb

# Now you can use Path objects directly!
data_file = Path("experiments/sample.ngb-ss3")
baseline_file = Path("experiments/baseline.ngb-bs3")

# Works with Path objects
data = read_ngb(data_file, baseline_file=baseline_file)

# Still works with strings (backwards compatible)
data = read_ngb("experiments/sample.ngb-ss3")
```

## 🐛 Bug Fixes

- Fixed DSC calibration tests to use Polars instead of pandas (removed implicit pandas dependency)
- Updated test mocking from `builtins.open` to `pathlib.Path.open`
- Fixed `os.unlink()` usage in tests to use `Path.unlink()`
- Updated edge case handling for `Path("")` behavior

## 🔧 Implementation Improvements

- Replaced `open(str(path))` with `path.open()` throughout codebase
- Removed unnecessary `str()` conversions in batch processing
- Updated all examples and scripts to demonstrate pathlib best practices
- Improved type safety with `str | Path` type hints throughout the API

## ✅ Testing

- **384 tests passing** (up from 382)
- Fixed 2 previously failing tests
- All test suite now uses Polars consistently (no pandas dependency)

## 🔄 Backwards Compatibility

**All changes maintain full backwards compatibility** - existing code using string paths will continue to work without modification. This is a non-breaking enhancement.

## 📦 Files Changed

- `examples/batch_processing.py` - Updated to use Path consistently
- `scripts/process_all_test_files.py` - Simplified Path usage
- `src/pyngb/api/loaders.py` - Updated read_ngb() signature
- `src/pyngb/baseline.py` - Updated subtract_baseline() signature
- `src/pyngb/batch.py` - Removed unnecessary str() conversions
- `src/pyngb/core/parser.py` - Updated parse() signature
- `src/pyngb/util.py` - Updated get_hash() to use Path
- `tests/test_dsc_calibration.py` - Fixed to use Polars
- `tests/test_util.py` - Updated test mocking
- `tests/test_workflows.py` - Added Path import and fixed usage
- `pyproject.toml` - Version bump to 0.1.1
- `CHANGELOG.md` - New file with release history

## 🚀 Installation

```bash
pip install pyngb==0.1.1
```

## 📊 Statistics

- **12 files changed**
- **103 insertions**
- **56 deletions**
- **384 tests passing**
- **0 tests failing**

---

For full details, see the [CHANGELOG](CHANGELOG.md).
