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
│   │   ├── loaders.py     # File loading functions
│   │   └── analysis.py    # Analysis functions (DTG, etc.)
│   ├── core/              # Core parsing logic
│   │   └── parser.py      # Main parser coordination
│   ├── binary/            # Low-level binary parsing
│   │   ├── parser.py      # Binary structure parsing
│   │   └── handlers.py    # Data type handlers
│   ├── extractors/        # Data extraction modules
│   │   ├── metadata.py    # Metadata extraction
│   │   └── streams.py     # Data stream processing
│   ├── analysis/          # Analysis algorithms
│   │   └── dtg.py         # DTG calculation
│   ├── batch.py           # Batch processing
│   ├── validation.py      # Data validation
│   ├── constants.py       # Configuration constants
│   ├── exceptions.py      # Custom exceptions
│   └── util.py           # Utility functions
├── tests/                 # Test suite
├── docs/                  # Documentation
└── examples/             # Usage examples
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

### Adding New Data Type Handler

```python
# In src/pyngb/binary/handlers.py
class NewDataTypeHandler(DataTypeHandler):
    def can_handle(self, data_type: bytes) -> bool:
        return data_type == b'\x42'  # Your data type marker

    def parse(self, data: bytes) -> list:
        # Your parsing logic
        return parsed_data

# Register in appropriate place
registry.register(NewDataTypeHandler())
```

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
# In src/pyngb/validation.py
def validate_new_condition(df: pl.DataFrame) -> list[str]:
    """Validate new data condition."""
    issues = []

    # Check condition
    if condition_not_met:
        issues.append("Description of issue")

    return issues
```

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

Contributors are listed in `CONTRIBUTORS.md` and in release notes. Significant contributions may be acknowledged in academic presentations.

Thank you for contributing to pyngb! 🚀
