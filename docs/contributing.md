---
description: Contribute to pyngb — development setup with uv, running the test suite with pytest, code style with ruff, and the pull request workflow.
---

# Contributing

We welcome contributions to pyngb! This guide explains how to set up your development environment and contribute effectively.

## Getting Started

### Development Setup

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/pyngb.git
cd pyngb

# Install with development dependencies
uv sync --extra dev

# Install pre-commit hooks
pre-commit install

# Verify setup
uv run pytest
uv run ruff check .
uv run mypy src/
```

### Alternative Setup (pip)

```bash
# Clone and set up virtual environment
git clone https://github.com/YOUR_USERNAME/pyngb.git
cd pyngb
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"
pre-commit install
```

## Code Quality

### Style and Linting

We use ruff for formatting and linting:

```bash
# Format code
uv run ruff format .

# Check for issues
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check . --fix
```

### Type Checking

We use mypy for static type checking:

```bash
# Type check main package
uv run mypy src/

# Type check specific module
uv run mypy src/pyngb/api/
```

### Security Scanning

```bash
# Security checks
uv run bandit -r src/

# Safety checks for dependencies
uv run safety check
```

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run only fast tests (skip integration tests)
uv run pytest -m "not slow"

# Run specific test
uv run pytest tests/test_api.py::test_read_ngb_basic
```

### Test Categories

- **Unit tests**: Test individual functions and classes
- **Integration tests**: Test with real NGB files
- **Performance tests**: Benchmark parsing speed
- **Stress tests**: Test with large files and edge cases

### Writing Tests

```python
import pytest
from pyngb import read_ngb

def test_new_feature():
    """Test description following numpy docstring style."""
    # Arrange
    file_path = "test_data.ngb-ss3"

    # Act
    result = read_ngb(file_path)

    # Assert
    assert result.num_rows > 0
    assert "time" in result.column_names

@pytest.mark.slow
def test_large_file_processing():
    """Test that requires significant time/resources."""
    # Test implementation
    pass
```

## Project Structure

```
pyngb/
├── src/pyngb/              # Main package
│   ├── api/               # High-level user interface
│   │   ├── loaders.py     # read_ngb / read_ngb_metadata
│   │   ├── analysis.py    # Table-level analysis (DTG, normalization, …)
│   │   ├── metadata.py    # Column-metadata helpers
│   │   └── cli.py         # convert / inspect / validate subcommands
│   ├── format/            # NGB format layer (strictly layered, top→bottom)
│   │   ├── extract.py     # build_metadata: document → FileMetadata
│   │   ├── channels.py    # build_dataframe: document → data columns
│   │   ├── census.py      # coverage / unknown-field accounting
│   │   ├── document.py    # NGBDocument / Table / Field assembly
│   │   ├── maps.py        # ALL declarative format knowledge
│   │   ├── grammar.py     # record grammar constants + tokenizer
│   │   └── container.py   # ZIP + section-directory parsing
│   ├── analysis/          # Analysis algorithms (dtg.py)
│   ├── validation/        # Data-quality validators (QualityChecker, …)
│   ├── util/              # Hashing, column helpers
│   ├── baseline.py        # Baseline subtraction
│   ├── batch.py           # Batch processing
│   ├── config.py          # ParsingConfig (resource limits)
│   ├── constants.py       # FileMetadata + column-metadata TypedDicts
│   └── exceptions.py      # Custom exceptions
├── tests/                 # Test suite (see tests/README.md)
├── docs/                  # Documentation
├── scripts/               # Goldens generator, benchmarks
└── examples/              # Usage examples
```

## Contribution Workflow

### 1. Create Issue (Optional)

For major changes, create an issue first to discuss the approach.

### 2. Create Branch

```bash
# Create feature branch
git checkout -b feature/new-analysis-method

# Or bug fix branch
git checkout -b fix/parsing-error
```

### 3. Make Changes

- Write clear, well-documented code
- Follow existing patterns and conventions
- Add tests for new functionality
- Update documentation as needed

### 4. Test Your Changes

```bash
# Run full test suite
uv run pytest

# Check code quality
uv run ruff check .
uv run mypy src/
```

### 5. Commit Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "feat: add new DTG smoothing algorithm

- Implement adaptive smoothing based on data characteristics
- Add tests for new algorithm
- Update documentation with usage examples"
```

### 6. Push and Create PR

```bash
# Push to your fork
git push origin feature/new-analysis-method

# Create pull request on GitHub
# Include description of changes and any breaking changes
```

## Development Guidelines

### Code Style

- Follow PEP 8 with ruff configuration
- Use type hints for all public functions
- Write docstrings in numpy style
- Keep functions focused and testable
- Use descriptive variable names

### Performance Considerations

- Use NumPy and PyArrow for data processing
- Avoid copying large arrays when possible
- Consider memory usage for large files
- Profile performance-critical code

### Error Handling

- Use specific exception types from `exceptions.py`
- Provide helpful error messages
- Handle edge cases gracefully
- Validate input parameters

### Testing Guidelines

- Write tests for all new functionality
- Use descriptive test names
- Test both success and failure cases
- Include edge cases and boundary conditions
- Use real NGB files for integration tests

### Documentation

- Update user guide for new features
- Add examples for complex functionality
- Update API reference for new functions
- Keep README concise and focused

## Common Development Tasks

### Adding a New Metadata Field

All format knowledge is declarative (`src/pyngb/format/maps.py`). For a
simple scalar field, add one `MetaField` entry — locate the
`(category, field_id)` pair with `pyngb inspect --unknown` / `--values`:

```python
# In src/pyngb/format/maps.py, inside FIELD_MAP:
MetaField("new_field", 0x1772, 0x0842, _clean_str),
```

then add the key to the `FileMetadata` TypedDict in `constants.py` and pin
the extracted value in a test. For a structured extraction (multiple fields,
cross-table logic), write one plain function in `src/pyngb/format/extract.py`
and append it to the `_EXTRACTORS` tuple:

```python
def extract_new_block(doc: NGBDocument, metadata: FileMetadata) -> None:
    table = doc.first(1, category=0x1234, with_fields=(0x0842,))
    if table is not None:
        metadata["new_field"] = table.value(0x0842)
```

Regenerate the parity goldens (`uv run python scripts/make_goldens.py parity`)
and explain the diff in your PR — golden changes must always be intentional.

### Adding New Analysis Function

```python
# In src/pyngb/api/analysis.py
def new_analysis(
    table: pa.Table,
    parameter: float = 1.0
) -> pa.Table:
    """
    Perform new analysis on thermal data.

    Parameters
    ----------
    table : pa.Table
        Input data table
    parameter : float
        Analysis parameter

    Returns
    -------
    pa.Table
        Table with analysis results added
    """
    # Implementation
    pass
```

### Adding New Validation Check

```python
# In src/pyngb/validation/ — one module per validator family
def validate_new_condition(df: pl.DataFrame) -> list[str]:
    """Validate new data condition."""
    issues = []

    # Check condition
    if condition_not_met:
        issues.append("Description of issue")

    return issues
```

Wire it into `QualityChecker` so `pyngb validate` picks it up.

## Release Process

Releases are managed by maintainers:

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release tag
4. GitHub Actions builds and publishes to PyPI

## Getting Help

- **Questions**: Use [GitHub Discussions](https://github.com/GraysonBellamy/pyngb/discussions)
- **Bugs**: Create [GitHub Issues](https://github.com/GraysonBellamy/pyngb/issues)
- **Development Chat**: Tag maintainers in issues/PRs

## Code of Conduct

Be respectful and constructive in all interactions. We're building tools for the scientific community and welcome diverse contributions.

## Recognition

Contributors are acknowledged in release notes. Significant contributions may be acknowledged in academic presentations.

Thank you for contributing to pyngb! 🚀
