# Scripts Directory

This directory contains utility and testing scripts for the pyngb project.

## Scripts Overview

### `run_tests.py`
**Purpose**: Comprehensive test runner covering both core functionality and new features without requiring pytest.
**Usage**:
```bash
# Run from project root
uv run python scripts/run_tests.py
```
**Features**:
- Core functionality testing (imports, exceptions, constants, binary parsing)
- New feature testing (validation and batch processing)
- Cross-module integration testing
- Clear categorized output with pass/fail reporting
- No external test framework dependencies

### `test_imports.py`
**Purpose**: Validates that all package imports work correctly.
**Usage**:
```bash
uv run python scripts/test_imports.py
```

## Best Practices

- **Run from project root**: All scripts are designed to be executed from the project root directory
- **Use with uv**: Scripts should be run with `uv run` to ensure proper virtual environment activation
- **Development workflow**: Use these scripts during development to quickly validate changes
- **CI/CD integration**: These scripts can be integrated into continuous integration pipelines

## Related Testing

For formal unit testing with detailed reporting, use the pytest-based tests in the `tests/` directory:
```bash
# Run all unit tests
uv run python -m pytest

# Run specific test modules
uv run python -m pytest tests/test_validation.py tests/test_batch.py
```

## Test Coverage

The integrated test runner (`run_tests.py`) provides a lightweight alternative to pytest that covers:

### Core Functionality Tests:
- ✅ Package imports and module structure
- ✅ Exception hierarchy and error handling
- ✅ Constants and configuration
- ✅ Binary data handlers and parsing
- ✅ Basic parser functionality

### New Features Tests:
- ✅ Data validation functionality
- ✅ Batch processing capabilities
- ✅ Cross-module integration

This provides confidence that the essential functionality works without requiring the full pytest test suite.
