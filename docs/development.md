# Development Guide

This guide covers development setup, contribution guidelines, testing procedures, and architectural details for pyngb.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) package manager (recommended) or pip
- Git

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb

# Install with development dependencies
uv sync --extra dev

# Install pre-commit hooks (optional but recommended)
pre-commit install

# Verify installation
uv run pytest --version
uv run ruff --version
uv run mypy --version
```

### Alternative Setup with pip

```bash
# Clone and navigate
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

## Project Structure

```
pyngb/
├── src/pyngb/              # Main package code
│   ├── api/               # High-level user interface
│   ├── binary/            # Binary parsing components
│   ├── core/              # Core parser logic
│   ├── extractors/        # Data and metadata extractors
│   ├── batch.py           # Batch processing tools
│   ├── validation.py      # Data validation
│   ├── constants.py       # Configuration and constants
│   ├── exceptions.py      # Custom exceptions
│   └── util.py           # Utility functions
├── tests/                 # Comprehensive test suite
│   ├── test_files/        # Real NGB files for testing
│   ├── conftest.py        # Test fixtures
│   └── test_*.py         # Test modules
├── docs/                  # Documentation
├── examples/              # Usage examples
└── scripts/              # Development scripts
```

## Code Style and Quality

### Code Formatting

We use several tools to maintain code quality:

```bash
# Format code with ruff
uv run ruff format .

# Lint with ruff
uv run ruff check .

# Type checking with mypy
uv run mypy src/

# Security scanning with bandit
uv run bandit -r src/
```

### Pre-commit Hooks

Install pre-commit hooks to automatically run checks:

```bash
# Install hooks
pre-commit install

# Run hooks manually
pre-commit run --all-files
```

The pre-commit configuration includes:
- Code formatting (ruff)
- Linting (ruff)
- Type checking (mypy)
- Security checks (bandit)
- Documentation checks

### Configuration Files

- **pyproject.toml**: Main configuration for tools and build
- **.pre-commit-config.yaml**: Pre-commit hook configuration
- **.gitignore**: Git ignore patterns
- **mkdocs.yml**: Documentation configuration

## Testing

pyngb has a comprehensive test suite with over 300 tests covering unit, integration, and end-to-end scenarios.

### Test Categories

#### Unit Tests
- **Location**: `tests/test_*.py`
- **Purpose**: Test individual components in isolation
- **Coverage**: All major functions and classes
- **Execution**: Fast (<1 second per test)

#### Integration Tests
- **Location**: `tests/test_integration_comprehensive.py`
- **Purpose**: Test component interactions and real-world scenarios
- **Coverage**: Cross-module functionality, error handling
- **Execution**: Medium speed (1-10 seconds per test)

#### End-to-End Tests
- **Location**: `tests/test_end_to_end_workflows.py`
- **Purpose**: Test complete user workflows
- **Coverage**: Full data processing pipelines
- **Execution**: Slower (10+ seconds per test)

#### Stress Tests
- **Location**: `tests/test_stress_and_edge_cases.py`
- **Purpose**: Test robustness under extreme conditions
- **Coverage**: Memory management, concurrent access, edge cases
- **Execution**: Marked as slow tests

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=src --cov-report=html

# Run only fast tests (skip slow tests)
uv run pytest -m "not slow"

# Run specific test file
uv run pytest tests/test_api.py

# Run specific test function
uv run pytest tests/test_api.py::TestLoadNGBData::test_basic

# Run tests with verbose output
uv run pytest -v

# Run tests and stop on first failure
uv run pytest -x
```

### Test Data

The test suite uses:
- **Real NGB files**: Located in `tests/test_files/`
- **Mock data**: Generated programmatically for specific scenarios
- **Fixtures**: Reusable test components in `conftest.py`

```python
# Example test using real files
def test_with_real_file():
    test_file = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"
    if not test_file.exists():
        pytest.skip("Test file not found")

    table = read_ngb(str(test_file))
    assert table.num_rows > 0
```

### Writing Tests

#### Test Structure

```python
class TestMyComponent:
    """Test MyComponent functionality."""

    def test_basic_functionality(self):
        """Test basic usage scenario."""
        # Arrange
        component = MyComponent()

        # Act
        result = component.do_something()

        # Assert
        assert result is not None

    def test_error_handling(self):
        """Test error conditions."""
        component = MyComponent()

        with pytest.raises(SpecificError):
            component.do_invalid_thing()

    @pytest.mark.slow
    def test_performance_scenario(self):
        """Test performance-critical functionality."""
        # Mark slow tests appropriately
        pass
```

#### Using Fixtures

```python
def test_with_sample_data(sample_ngb_file):
    """Test using shared fixture."""
    table = read_ngb(sample_ngb_file)
    assert table.num_rows > 0
```

#### Parameterized Tests

```python
@pytest.mark.parametrize("file_path,expected_columns", [
    ("file1.ngb-ss3", ["time", "sample_temperature"]),
    ("file2.ngb-ss3", ["time", "sample_temperature", "mass"]),
])
def test_multiple_files(file_path, expected_columns):
    """Test with multiple file scenarios."""
    # Test implementation
    pass
```

### Coverage Goals

| Component | Target Coverage | Current Status |
|-----------|----------------|----------------|
| API Functions | 95%+ | ✅ Achieved |
| Core Parser | 90%+ | ✅ Achieved |
| Binary Handlers | 95%+ | ✅ Achieved |
| Validation | 90%+ | ✅ Achieved |
| Batch Processing | 90%+ | ✅ Achieved |
| Utilities | 95%+ | ✅ Achieved |
| Overall | 85%+ | ✅ Achieved (90%+) |

## Performance Testing

### Benchmarking

```bash
# Run performance tests
uv run pytest tests/test_stress_and_edge_cases.py -m slow

# Profile specific functions
uv run python -m cProfile -o profile.stats scripts/profile_parsing.py

# Memory profiling
uv run python -m memory_profiler scripts/memory_test.py
```

### Performance Targets

| Operation | Target | Measurement |
|-----------|--------|-------------|
| Parse 10MB file | <2 seconds | End-to-end parsing |
| Extract metadata | <0.5 seconds | Metadata only |
| Batch 100 files | <60 seconds | 4-core parallel |
| Memory usage | <3x file size | Peak memory |

## Contributing Guidelines

### Contribution Process

1. **Fork the Repository**
   ```bash
   # Fork on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/pyngb.git
   cd pyngb
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Changes**
   - Write code following our style guidelines
   - Add or update tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```

5. **Submit Pull Request**
   - Push to your fork
   - Create pull request with clear description
   - Address review feedback

### Commit Message Guidelines

We follow conventional commits:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `test:` - Test additions or modifications
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `style:` - Code style changes
- `chore:` - Maintenance tasks

Examples:
```
feat: add batch processing validation
fix: handle corrupted file headers correctly
docs: update API reference for new functions
test: add integration tests for real files
```

### Code Review Process

All contributions go through code review:

1. **Automated Checks**: CI runs tests, linting, and type checking
2. **Manual Review**: Maintainers review code quality and design
3. **Testing**: Verify functionality with real data
4. **Documentation**: Ensure adequate documentation
5. **Merge**: Approved changes are merged to main

## Architecture Details

### Design Principles

1. **Modularity**: Clear separation of concerns
2. **Performance**: Optimize for speed and memory efficiency
3. **Extensibility**: Easy to add new formats and features
4. **Reliability**: Comprehensive error handling and validation
5. **Usability**: Multiple APIs for different use cases

### Core Components

#### API Layer (`api/`)
- **Purpose**: High-level user interface
- **Key Files**: `loaders.py`
- **Responsibilities**: Simple functions for common use cases

#### Core Parser (`core/`)
- **Purpose**: Orchestrate parsing operations
- **Key Files**: `parser.py`
- **Responsibilities**: Coordinate binary parsing, metadata extraction, and data processing

#### Binary Parser (`binary/`)
- **Purpose**: Low-level binary format handling
- **Key Files**: `parser.py`, `handlers.py`
- **Responsibilities**: Binary structure parsing, data type conversion

#### Extractors (`extractors/`)
- **Purpose**: Specialized data extraction
- **Key Files**: `metadata.py`, `streams.py`
- **Responsibilities**: Extract metadata and measurement data

#### Batch Processing (`batch.py`)
- **Purpose**: Handle multiple files efficiently
- **Responsibilities**: Parallel processing, progress tracking, error handling

#### Validation (`validation.py`)
- **Purpose**: Data quality checking
- **Responsibilities**: Validate data integrity, check for anomalies

### Extension Points

#### Custom Data Type Handlers

```python
from pyngb.binary.handlers import DataTypeHandler

class CustomHandler(DataTypeHandler):
    def can_handle(self, data_type: bytes) -> bool:
        return data_type == b'\x99'

    def parse(self, data: bytes) -> list:
        # Custom parsing logic
        return parsed_data
```

#### Custom Validation Rules

```python
from pyngb.validation import QualityChecker

class CustomQualityChecker(QualityChecker):
    def custom_validation(self):
        # Add domain-specific validation
        pass
```

#### Custom Configuration

```python
from pyngb.constants import PatternConfig

config = PatternConfig()
config.metadata_patterns["new_field"] = (b"\x99\x99", b"\x88\x88")
config.column_map["99"] = "new_column"
```

## Documentation

### Building Documentation

```bash
# Install documentation dependencies
uv sync --extra docs

# Build documentation
cd docs
mkdocs build

# Serve documentation locally
mkdocs serve
```

### Documentation Structure

- **index.md**: Main documentation landing page
- **installation.md**: Installation instructions
- **quickstart.md**: Getting started guide
- **api.md**: Complete API reference
- **development.md**: This development guide
- **troubleshooting.md**: Common issues and solutions

### Writing Documentation

#### API Documentation

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int = 10) -> bool:
    """Brief description of function.

    Longer description if needed.

    Args:
        param1: Description of first parameter.
        param2: Description of second parameter with default.

    Returns:
        Description of return value.

    Raises:
        ValueError: When parameter is invalid.

    Examples:
        >>> result = my_function("test", 5)
        >>> print(result)
        True
    """
    return True
```

#### User Guide Documentation

- Use clear, practical examples
- Include complete code snippets
- Show expected output when helpful
- Use admonitions for tips and warnings

```markdown
!!! tip "Performance Tip"
    Use PyArrow tables for better memory efficiency.

!!! warning "Important"
    Always validate data when processing unknown files.
```

## Release Process

### Version Management

We use semantic versioning (SemVer):
- **Major** (1.0.0): Breaking changes
- **Minor** (0.1.0): New features, backwards compatible
- **Patch** (0.0.1): Bug fixes, backwards compatible

### Release Checklist

1. **Update Version**
   ```bash
   # Update version in pyproject.toml
   # Update CHANGELOG.md
   ```

2. **Run Full Test Suite**
   ```bash
   uv run pytest --cov=src
   # Ensure >90% coverage
   # All tests pass
   ```

3. **Update Documentation**
   ```bash
   # Update API docs
   # Update examples
   # Build documentation
   ```

4. **Build and Test Package**
   ```bash
   uv build
   # Test installation in clean environment
   ```

5. **Create Release**
   ```bash
   git tag v0.0.1
   git push origin v0.0.1
   # Create GitHub release
   # Upload to PyPI
   ```

## Debugging and Troubleshooting

### Development Debugging

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Use debugger
import pdb; pdb.set_trace()

# Profile performance
import cProfile
cProfile.run('my_function()')
```

### Common Issues

#### Import Errors
```bash
# Ensure package is installed in development mode
pip install -e .
# or with uv
uv sync
```

#### Test Failures
```bash
# Run specific failing test with verbose output
uv run pytest tests/test_specific.py::test_function -v -s

# Check test data files exist
ls tests/test_files/
```

#### Memory Issues
```bash
# Monitor memory usage
uv run python -m memory_profiler script.py

# Use smaller test data
# Process in chunks
```

## Getting Help

### Internal Resources
- Check existing tests for usage examples
- Review docstrings in source code
- Use IDE debugging tools

### External Resources
- [GitHub Issues](https://github.com/GraysonBellamy/pyngb/issues)
- [GitHub Discussions](https://github.com/GraysonBellamy/pyngb/discussions)
- [Documentation](https://graysonbellamy.github.io/pyngb/)

### Contributing Back

Found a bug? Have an improvement idea?

1. Search existing issues first
2. Create detailed issue with reproduction steps
3. Consider submitting a pull request
4. Help improve documentation

---

Thank you for contributing to pyngb! Your efforts help make thermal analysis data more accessible to the scientific community.
