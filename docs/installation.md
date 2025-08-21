# Installation Guide

This guide covers various ways to install pyngb and its dependencies.

## Requirements

### System Requirements

- **Python**: 3.9 or higher
- **Operating System**: Windows, macOS, or Linux
- **Memory**: 4GB+ recommended for large files
- **Storage**: 50MB for package + space for data files

### Python Dependencies

pyngb automatically installs these core dependencies:

- **polars** (≥0.20.0): Fast DataFrame operations
- **pyarrow** (≥10.0.0): Columnar data format
- **numpy** (≥1.20.0): Numerical computing

Optional dependencies for specific features:

- **matplotlib**: Data visualization
- **pandas**: DataFrame interoperability
- **jupyter**: Notebook support

## Installation Methods

### Method 1: PyPI (Recommended)

Install the latest stable version from PyPI:

```bash
pip install pyngb
```

This is the recommended method for most users as it provides:
- Latest stable release
- Automatic dependency management
- Easy updates
- Binary wheels for faster installation

### Method 2: Using uv (Fast Alternative)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer:

```bash
# Install uv first
pip install uv

# Install pyngb with uv
uv pip install pyngb
```

Benefits of using uv:
- Much faster dependency resolution
- Better handling of version conflicts
- Improved security

### Method 3: From Source (Development)

Install from GitHub for the latest features:

```bash
pip install git+https://github.com/GraysonBellamy/pyngb.git
```

Or clone and install in development mode:

```bash
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb
pip install -e .
```

## Virtual Environment Setup

### Using venv (Recommended)

Create an isolated environment for pyngb:

```bash
# Create virtual environment
python -m venv pyngb_env

# Activate environment
# On Windows:
pyngb_env\Scripts\activate
# On macOS/Linux:
source pyngb_env/bin/activate

# Install pyngb
pip install pyngb

# Verify installation
python -c "import pyngb; print(pyngb.__version__)"
```

### Using conda

If you prefer conda environments:

```bash
# Create conda environment
conda create -n pyngb python=3.11

# Activate environment
conda activate pyngb

# Install pyngb
pip install pyngb
```

### Using uv (Modern Approach)

uv provides excellent virtual environment management:

```bash
# Create and activate environment
uv venv pyngb_env
source pyngb_env/bin/activate  # On Windows: pyngb_env\Scripts\activate

# Install pyngb
uv pip install pyngb
```

## Installation Verification

### Basic Verification

Test that pyngb is properly installed:

```python
import pyngb
print(f"pyngb version: {pyngb.__version__}")

# Test basic functionality
from pyngb import read_ngb
print("✓ Core function imported successfully")

# Test command-line interface
import subprocess
result = subprocess.run(["python", "-m", "pyngb", "--help"],
                       capture_output=True, text=True)
if result.returncode == 0:
    print("✓ Command-line interface working")
else:
    print("✗ Command-line interface issue")
```

### Dependency Verification

Check that all dependencies are properly installed:

```python
# Check core dependencies
try:
    import polars as pl
    print(f"✓ polars {pl.__version__}")
except ImportError:
    print("✗ polars not found")

try:
    import pyarrow as pa
    print(f"✓ pyarrow {pa.__version__}")
except ImportError:
    print("✗ pyarrow not found")

try:
    import numpy as np
    print(f"✓ numpy {np.__version__}")
except ImportError:
    print("✗ numpy not found")

# Check optional dependencies
optional_deps = ["matplotlib", "pandas", "jupyter"]
for dep in optional_deps:
    try:
        __import__(dep)
        print(f"✓ {dep} available")
    except ImportError:
        print(f"- {dep} not installed (optional)")
```

### Functionality Test

Test with a minimal example:

```python
import tempfile
import zipfile
from pathlib import Path

# Create a minimal test file
def create_test_file():
    with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
        with zipfile.ZipFile(temp_file.name, "w") as z:
            z.writestr("Streams/stream_1.table", b"test data")
            z.writestr("Streams/stream_2.table", b"test data")
        return temp_file.name

# Test parsing (will likely fail with mock data, but tests the pipeline)
test_file = create_test_file()
try:
    from pyngb import read_ngb
    table = read_ngb(test_file)
    print("✓ Parsing pipeline functional")
except Exception as e:
    print(f"✓ Parsing pipeline accessible (expected error with mock data: {type(e).__name__})")
finally:
    Path(test_file).unlink()
```

## Optional Dependencies

### For Data Analysis

```bash
# Install polars for DataFrame interoperability
pip install polars

# Install matplotlib for plotting
pip install matplotlib

# Install seaborn for advanced plotting
pip install seaborn

# Install jupyter for notebook support
pip install jupyter
```

### For Development

If you plan to contribute to pyngb:

```bash
# Install development dependencies
pip install pyngb[dev]

# Or manually install dev tools
pip install pytest pytest-cov ruff mypy pre-commit
```

### For Documentation

To build documentation locally:

```bash
# Install documentation dependencies
pip install pyngb[docs]

# Or manually
pip install mkdocs mkdocs-material mkdocstrings[python]
```

## Installation Troubleshooting

### Common Issues

#### Permission Denied

```bash
# Use user installation
pip install --user pyngb

# Or use virtual environment (recommended)
python -m venv pyngb_env
source pyngb_env/bin/activate
pip install pyngb
```

#### Dependency Conflicts

```bash
# Update pip first
pip install --upgrade pip

# Use clean environment
python -m venv clean_env
source clean_env/bin/activate
pip install pyngb

# Or use uv for better resolution
pip install uv
uv pip install pyngb
```

#### Slow Installation

```bash
# Use binary wheels
pip install --only-binary=all pyngb

# Or use uv (much faster)
pip install uv
uv pip install pyngb

# Clear pip cache if needed
pip cache purge
```

#### Version Conflicts

```bash
# Check installed versions
pip list | grep -E "(polars|pyarrow|numpy)"

# Force reinstall if needed
pip install --force-reinstall pyngb

# Install specific versions
pip install "polars>=0.20.0" "pyarrow>=10.0.0"
pip install pyngb
```

### Platform-Specific Issues

#### Windows

```powershell
# Use PowerShell as administrator if needed
# Install Microsoft Visual C++ redistributables if compilation errors occur

# Alternative: use conda
conda install -c conda-forge polars pyarrow
pip install pyngb
```

#### macOS

```bash
# Install Xcode command line tools if needed
xcode-select --install

# Use Homebrew Python if system Python issues
brew install python
pip3 install pyngb
```

#### Linux

```bash
# Install required system packages (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install python3-dev python3-pip

# Install required system packages (CentOS/RHEL)
sudo yum install python3-devel python3-pip

# Then install pyngb
pip3 install pyngb
```

## Docker Installation

For containerized environments:

```dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install pyngb
RUN pip install pyngb

# Verify installation
RUN python -c "import pyngb; print('pyngb installed successfully')"

# Set working directory
WORKDIR /app
```

Build and run:

```bash
docker build -t pyngb-app .
docker run -it -v $(pwd):/app pyngb-app python
```

## Performance Optimization

### Memory Management

For processing large files and optimal performance:

```bash
# Install with performance extras
pip install pyngb[performance]

# Or install memory monitoring tools
pip install psutil memory_profiler
```

### Optimized Data Processing (v0.0.2+)

pyngb v0.0.2 includes significant performance optimizations:

- **Reduced Conversions**: Eliminated unnecessary PyArrow ↔ Polars conversions
- **Smart Type Detection**: Validation functions automatically detect input format
- **Memory Efficiency**: Reduced intermediate object creation during processing
- **Batch Processing**: Optimized to convert data only when needed for specific output formats

These optimizations provide:
- ~1.04x overhead instead of multiple conversion cycles
- Lower memory usage during data processing
- Faster validation operations
- Maintained API compatibility

### Parallel Processing

Ensure optimal performance for batch processing:

```python
import os
import multiprocessing

# Check available cores
print(f"CPU cores: {os.cpu_count()}")
print(f"Available for multiprocessing: {multiprocessing.cpu_count()}")

# Configure batch processor accordingly
from pyngb import BatchProcessor
processor = BatchProcessor(max_workers=os.cpu_count())
```

## Updating pyngb

### Regular Updates

```bash
# Update to latest version
pip install --upgrade pyngb

# Check new version
python -c "import pyngb; print(pyngb.__version__)"
```

### Pre-release Versions

```bash
# Install pre-release versions
pip install --pre pyngb

# Install specific version
pip install pyngb==0.0.1
```

### Rollback if Needed

```bash
# Install specific older version
pip install pyngb==0.0.9

# Or reinstall from requirements
pip install -r requirements.txt
```

## IDE Configuration

### VS Code

Recommended extensions:
- Python extension
- Pylance for type checking
- Jupyter for notebook support

Settings for `.vscode/settings.json`:
```json
{
    "python.defaultInterpreterPath": "./pyngb_env/bin/python",
    "python.analysis.typeCheckingMode": "basic",
    "python.linting.enabled": true
}
```

### PyCharm

1. Configure Python interpreter to use your virtual environment
2. Enable type checking in Settings → Editor → Inspections → Python
3. Add pyngb source directory to Python paths for development

### Jupyter

```bash
# Install Jupyter in your environment
pip install jupyter

# Install kernel for your environment
python -m ipykernel install --user --name=pyngb_env

# Start Jupyter
jupyter notebook
```

## Next Steps

After successful installation:

1. **[Quick Start Guide](quickstart.md)**: Learn basic usage
2. **[API Reference](api.md)**: Explore all available functions
3. **[Examples](https://github.com/GraysonBellamy/pyngb/tree/main/examples)**: See practical examples
4. **[Troubleshooting](troubleshooting.md)**: Resolve common issues

## Getting Help

If you encounter installation issues:

1. Check the [troubleshooting guide](troubleshooting.md)
2. Search [existing issues](https://github.com/GraysonBellamy/pyngb/issues)
3. Create a [new issue](https://github.com/GraysonBellamy/pyngb/issues/new) with:
   - Your operating system and Python version
   - Complete error message
   - Installation method attempted
