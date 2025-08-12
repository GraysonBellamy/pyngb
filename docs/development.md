# Development Guide

This guide covers the development workflow and tools for PyNetzsch.

## Development Environment Setup

### Prerequisites

- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/pynetzsch.git
   cd pynetzsch
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

PyNetzsch uses several automated tools to maintain code quality:

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit and include:

- **Black**: Code formatting
- **isort**: Import sorting
- **Ruff**: Fast Python linter with auto-fixes
- **mypy**: Type checking
- **Bandit**: Security scanning
- **pydocstyle**: Docstring style checking
- Various file checks (trailing whitespace, file endings, etc.)

### Running Tools Manually

You can run any of these tools manually:

```bash
# Format code
uv run black src/ tests/
uv run isort src/ tests/

# Lint code
uv run ruff check src/ tests/ --fix

# Type check
uv run mypy src/

# Security scan
uv run bandit -r src/

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Configuration

Tool configurations are in `pyproject.toml`:

- `[tool.black]`: Code formatting settings
- `[tool.isort]`: Import sorting configuration
- `[tool.ruff]`: Linting rules and exclusions
- `[tool.mypy]`: Type checking settings
- `[tool.bandit]`: Security scanning configuration
- `[tool.pydocstyle]`: Docstring style rules

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=pynetzsch --cov-report=html

# Run specific test file
uv run pytest tests/test_api.py

# Run with verbose output
uv run pytest -v
```

### Test Structure

Tests are organized in the `tests/` directory:

- `test_api.py`: API functionality tests
- `test_binary_*.py`: Binary parsing tests
- `test_integration.py`: End-to-end integration tests
- `test_files/`: Sample NGB files for testing

### Adding Tests

When adding new functionality:

1. Write tests first (TDD approach)
2. Ensure good test coverage (aim for >80%)
3. Include both positive and negative test cases
4. Test edge cases and error conditions

## Performance

### Benchmarking

Use the built-in benchmarking tools:

```bash
# Run performance benchmarks
python benchmarks.py

# Profile memory usage
python -m memory_profiler benchmarks.py
```

### Performance Guidelines

- Use numpy arrays for numerical data when possible
- Minimize memory allocations in hot paths
- Profile before optimizing
- Consider using polars for large datasets

## Documentation

### Docstrings

Follow the NumPy docstring convention:

```python
def parse_data(self, data: bytes) -> Dict[str, Any]:
    """Parse binary data into a structured format.

    Parameters
    ----------
    data : bytes
        Raw binary data to parse

    Returns
    -------
    Dict[str, Any]
        Parsed data structure

    Raises
    ------
    ParseError
        If data format is invalid
    """
```

### Building Documentation

```bash
# Install documentation dependencies
uv sync --extra docs

# Build documentation (if sphinx setup exists)
cd docs/
make html
```

## Release Process

1. Update version in `src/pynetzsch/__about__.py`
2. Update `CHANGELOG.md` with new features and fixes
3. Run full test suite: `uv run pytest`
4. Run benchmarks to check for regressions
5. Create release commit and tag
6. Build and upload to PyPI:
   ```bash
   uv build
   uv publish
   ```

## Contributing

### Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and add tests
3. Run pre-commit hooks: `uv run pre-commit run --all-files`
4. Commit changes: `git commit -m "feat: description"`
5. Push and create pull request

### Commit Message Format

Use conventional commits:

- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` adding tests
- `refactor:` code refactoring
- `perf:` performance improvements

### Code Style Guidelines

- Follow PEP 8 (enforced by Black and Ruff)
- Use type hints where possible
- Write clear, descriptive variable names
- Keep functions small and focused
- Add docstrings to all public functions/classes

## IDE Setup

### VS Code

Recommended extensions:

- Python
- Pylance
- Black Formatter
- Ruff
- Pre-commit

Settings (`.vscode/settings.json`):

```json
{
    "python.defaultInterpreter": "./.venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "python.sortImports.path": "isort",
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.organizeImports": true
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **Pre-commit hooks failing**: Run `uv run pre-commit run --all-files` to see specific issues
2. **Import errors**: Ensure you've installed the package: `uv sync`
3. **Type checking errors**: Update type hints or add `# type: ignore` comments
4. **Test failures**: Check if test files are available and environment is set up correctly

### Getting Help

- Check existing issues on GitHub
- Review this documentation
- Run tests to ensure environment is working
- Use debugging tools like `pdb` or IDE debuggers
