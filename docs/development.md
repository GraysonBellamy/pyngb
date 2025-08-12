# Development

This page contains information for developers contributing to pyngb.

## Setting Up Development Environment

### Prerequisites

- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Installation

1. Clone the repository:
   ```bash
    git clone https://github.com/GraysonBellamy/pyngb.git
    cd pyngb
   ```

2. Install the package in development mode:
   ```bash
   uv sync --all-extras
   ```

3. Install pre-commit hooks:
   ```bash
   uv run pre-commit install
   ```

## Code Quality Tools

pyngb uses several automated tools to maintain code quality:

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit and include:

- **Ruff**: Fast Python linter and formatter (replaces Black and isort)
- **mypy**: Type checking
- **Bandit**: Security scanning
- Various file checks (trailing whitespace, file endings, etc.)

### Running Tools Manually

You can run any of these tools manually:

```bash
# Lint and format code with Ruff
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/

# Type check
uv run mypy src/

# Security scan
uv run bandit -r src/
uv run safety scan

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Configuration

Tool configurations are in `pyproject.toml`:

- `[tool.ruff]`: Linting and formatting rules
- `[tool.mypy]`: Type checking settings
- `[tool.bandit]`: Security scanning configuration

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=pyngb --cov-report=html

# Run specific test file
uv run pytest tests/test_api.py

# Run with verbose output
uv run pytest -v

# Run specific test types
pytest tests/ -k "not integration"  # Skip integration tests
pytest tests/test_integration.py    # Only integration tests
```

### Test Structure

Tests are organized in the `tests/` directory:

- `test_api.py`: High-level API functionality tests
- `test_binary_handlers.py`: Binary handler tests
- `test_binary_parser.py`: Binary parser tests
- `test_constants.py`: Constants tests
- `test_exceptions.py`: Exception tests
- `test_integration.py`: End-to-end integration tests
- `test_files/`: Sample NGB files for testing
- `conftest.py`: Pytest configuration and fixtures

### Adding Tests

When adding new functionality:

1. Write tests first (TDD approach)
2. Ensure good test coverage (aim for >80%)
3. Include both positive and negative test cases
4. Test edge cases and error conditions
5. Use descriptive test names that explain what is being tested
6. Follow the Arrange-Act-Assert pattern

## Performance

### Benchmarking

Use the built-in benchmarking tools:

```bash
# Run performance benchmarks
python benchmarks.py

# Run with multiple iterations for better statistics
python benchmarks.py --runs 5

# Profile memory usage
python -m memory_profiler benchmarks.py
```

### Performance Guidelines

- Use numpy arrays for numerical data when possible
- Consider using PyArrow Tables for memory efficiency with large datasets
- Minimize memory allocations in hot paths
- Profile before optimizing
- Consider using Parquet format for intermediate storage (faster than CSV)
- Process files in chunks for very large datasets

## Documentation

### Docstrings

Follow the Google/NumPy docstring convention:

```python
def parse_data(self, data: bytes) -> Dict[str, Any]:
    """Parse binary data into a structured format.

    Args:
        data: Raw binary data to parse

    Returns:
        Parsed data structure

    Raises:
        ParseError: If data format is invalid
    """
```

### Building Documentation

The project uses MkDocs with the Material theme:

```bash
# Install documentation dependencies
uv sync --extra docs

# Serve documentation locally with auto-reload
mkdocs serve

# Build static documentation
mkdocs build
```

The documentation will be available at `http://127.0.0.1:8000/` for local development, or in `site/` for static builds.

## Release Process

Releases are managed through GitHub Actions:

1. Update version in `src/pyngb/__about__.py`
2. Update `CHANGELOG.md` with new features and fixes
3. Run full test suite: `uv run pytest`
4. Run benchmarks to check for regressions: `python benchmarks.py`
5. Create release commit and tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
6. GitHub Actions will automatically build and publish to PyPI

## Contributing

### Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and add tests
3. Run pre-commit hooks: `uv run pre-commit run --all-files`
4. Ensure all tests pass: `uv run pytest`
5. Commit changes using conventional commits
6. Push and create pull request

### Commit Message Format

Use conventional commits:

- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` adding tests
- `refactor:` code refactoring
- `perf:` performance improvements
- `ci:` CI/CD changes

### Code Style Guidelines

- Follow PEP 8 (enforced by Ruff)
- Use type hints where possible
- Write clear, descriptive variable names
- Keep functions small and focused
- Add docstrings to all public functions/classes
- Ensure good test coverage for new code

## Project Structure

```text
pyngb/
├── src/pyngb/              # Main package
│   ├── api/               # High-level API
│   ├── binary/            # Binary parsing
│   ├── core/              # Core parsing logic
│   ├── extractors/        # Data extraction
│   └── ...
├── tests/                 # Test suite
├── docs/                  # MkDocs documentation
├── .github/workflows/     # CI/CD pipelines
├── benchmarks.py          # Performance benchmarks
├── mkdocs.yml            # Documentation configuration
└── pyproject.toml         # Project configuration
```

## IDE Setup

### VS Code

Recommended extensions:

- Python
- Pylance
- Ruff
- Pre-commit

Settings (`.vscode/settings.json`):

```json
{
    "python.defaultInterpreter": "./.venv/bin/python",
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.fixAll.ruff": true,
            "source.organizeImports.ruff": true
        },
        "editor.defaultFormatter": "charliermarsh.ruff"
    }
}
```

## Debugging Tips

### Common Development Issues

**Import errors during development:**
```bash
# Make sure you installed in development mode
uv sync
```

**Tests failing after changes:**
```bash
# Clear pytest cache
pytest --cache-clear
```

**Type checking errors:**
```bash
# Run mypy on specific files
mypy src/pyngb/specific_file.py
```

### Debugging Test Failures

Add debug prints or use pytest's built-in debugging:

```bash
# Run with verbose output
pytest -v -s

# Drop into debugger on failures
pytest --pdb

# Run only failed tests from last run
pytest --lf
```

## Troubleshooting

### Common Issues

1. **Pre-commit hooks failing**:
   ```bash
   # See specific issues and fix them
   uv run pre-commit run --all-files
   ```

2. **Import errors**:
   ```bash
   # Ensure package is installed in development mode
   uv sync --all-extras
   ```

3. **Type checking errors**: Update type hints or add `# type: ignore` comments

4. **Test failures**: Check if test files are available and environment is set up correctly

5. **Documentation build issues**:
   ```bash
   # Install docs dependencies
   pip install mkdocs mkdocs-material mkdocstrings[python]

   # Test local build
   mkdocs serve
   ```

### Getting Help

- Check existing issues on GitHub
- Review this documentation
- Run tests to ensure environment is working: `uv run pytest`
- Use debugging tools like `pdb` or IDE debuggers
- Check CI/CD pipeline logs for detailed error information
