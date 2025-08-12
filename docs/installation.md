# Installation

## From PyPI

Install the latest stable version:

```bash
pip install pynetzsch
```

## From Source

Clone the repository and install in development mode:

```bash
git clone https://github.com/GraysonBellamy/pynetzsch.git
cd pynetzsch
pip install -e .
```

## Development Installation

Install with all development dependencies:

```bash
pip install -e .[dev]
```

This includes:
- Testing tools (pytest, pytest-cov, pytest-mock, pytest-benchmark)
- Code quality tools (ruff, mypy, pre-commit)
- Documentation tools (mkdocs, mkdocs-material)
- Security tools (bandit, safety)

## Requirements

### Core Requirements

- **Python 3.9+**
- **polars >= 1.0.0** - Fast dataframes library
- **pyarrow >= 10.0.0** - Columnar data format

### Optional Dependencies

Development dependencies are automatically installed when using `pip install -e .[dev]`.

## Verification

Test your installation:

```python
import pynetzsch
print(pynetzsch.__version__)
```

Or run the test suite:

```bash
pytest
```

## Troubleshooting

### Common Issues

**ImportError: No module named 'pynetzsch'**
- Make sure you've installed the package: `pip install pynetzsch`
- If installing from source, use: `pip install -e .`

**ModuleNotFoundError: No module named 'polars'**
- Install dependencies: `pip install polars pyarrow`
- Or reinstall pynetzsch: `pip install --upgrade pynetzsch`

**Permission errors on Windows**
- Try installing with `--user` flag: `pip install --user pynetzsch`
- Or use a virtual environment

### Getting Help

If you encounter issues:

1. Check the [troubleshooting guide](troubleshooting.md)
2. Search existing [GitHub issues](https://github.com/GraysonBellamy/pynetzsch/issues)
3. Create a new issue with details about your setup and the error
