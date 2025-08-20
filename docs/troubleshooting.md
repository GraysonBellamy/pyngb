# Troubleshooting Guide

This guide helps you resolve common issues when using pyngb. If you don't find your issue here, please check our [GitHub Issues](https://github.com/GraysonBellamy/pyngb/issues) or create a new issue.

## Installation Issues

### Package Not Found

**Problem**: `pip install pyngb` fails with "package not found"

**Solutions**:
```bash
# Update pip first
pip install --upgrade pip

# Try with explicit PyPI index
pip install --index-url https://pypi.org/simple/ pyngb

# Install from source if needed
pip install git+https://github.com/GraysonBellamy/pyngb.git
```

### Dependency Conflicts

**Problem**: Installation fails due to dependency conflicts

**Solutions**:
```bash
# Create clean virtual environment
python -m venv pyngb_env
source pyngb_env/bin/activate  # On Windows: pyngb_env\Scripts\activate
pip install pyngb

# Or use uv for better dependency resolution
pip install uv
uv pip install pyngb
```

### ImportError After Installation

**Problem**: `ImportError: No module named 'pyngb'`

**Solutions**:
```bash
# Verify installation
pip list | grep pyngb

# Check Python path
python -c "import sys; print(sys.path)"

# Reinstall if needed
pip uninstall pyngb
pip install pyngb
```

## File Parsing Issues

### FileNotFoundError

**Problem**: File exists but pyngb can't find it

**Solutions**:
```python
from pathlib import Path

# Use absolute paths
file_path = Path("sample.ngb-ss3").absolute()
table = read_ngb(str(file_path))

# Check file exists
if not Path("sample.ngb-ss3").exists():
    print("File not found!")

# Check current directory
print("Current directory:", Path.cwd())
print("Files in directory:", list(Path.cwd().glob("*.ngb-ss3")))
```

### Corrupted File Error

**Problem**: `NGBCorruptedFileError` when parsing

**Solutions**:
```python
from pyngb import NGBCorruptedFileError

try:
    table = read_ngb("sample.ngb-ss3")
except NGBCorruptedFileError as e:
    print(f"File appears corrupted: {e}")

    # Try basic file checks
    with open("sample.ngb-ss3", "rb") as f:
        header = f.read(10)
        print(f"File header: {header}")

    # Check if it's a valid ZIP
    import zipfile
    try:
        with zipfile.ZipFile("sample.ngb-ss3", "r") as z:
            print("File contents:", z.namelist())
    except zipfile.BadZipFile:
        print("Not a valid ZIP file")
```

### Unsupported Version Error

**Problem**: `NGBUnsupportedVersionError` for newer files

**Solutions**:
```python
# Check file version information
import zipfile

with zipfile.ZipFile("sample.ngb-ss3", "r") as z:
    print("ZIP contents:", z.namelist())

    # Look for version information
    for name in z.namelist():
        if "version" in name.lower():
            content = z.read(name)
            print(f"{name}: {content[:100]}")

# Report unsupported files as issues
print("Please report this file format to: https://github.com/GraysonBellamy/pyngb/issues")
```

### Empty or Missing Data

**Problem**: File parses but returns no data

**Solutions**:
```python
table = read_ngb("sample.ngb-ss3")

# Check table structure
print(f"Rows: {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Column names: {table.column_names}")

# Check metadata
import json
if b'file_metadata' in table.schema.metadata:
    metadata = json.loads(table.schema.metadata[b'file_metadata'])
    print(f"Metadata keys: {list(metadata.keys())}")
else:
    print("No metadata found")

# Check raw file structure
import zipfile
with zipfile.ZipFile("sample.ngb-ss3", "r") as z:
    for name in z.namelist():
        size = z.getinfo(name).file_size
        print(f"{name}: {size} bytes")
```

## Memory and Performance Issues

### Out of Memory Errors

**Problem**: `MemoryError` when processing large files

**Solutions**:
```python
# Check file size first
from pathlib import Path
file_size = Path("large_file.ngb-ss3").stat().st_size
print(f"File size: {file_size / 1024 / 1024:.1f} MB")

# Process in chunks for very large files
def process_large_file_chunked(file_path, chunk_size=10000):
    table = read_ngb(file_path)

    for i in range(0, table.num_rows, chunk_size):
        chunk = table.slice(i, min(chunk_size, table.num_rows - i))
        # Process chunk
        yield chunk

# Use generators to reduce memory usage
for chunk in process_large_file_chunked("large_file.ngb-ss3"):
    # Process each chunk
    pass
```

### Slow Parsing Performance

**Problem**: Parsing takes too long

**Solutions**:
```python
import time
import polars as pl

# Measure parsing time
start_time = time.time()
table = read_ngb("sample.ngb-ss3")
parse_time = time.time() - start_time
print(f"Parse time: {parse_time:.2f} seconds")

# Check file complexity
print(f"File size: {Path('sample.ngb-ss3').stat().st_size / 1024 / 1024:.1f} MB")

# Optimize validation performance (v0.0.2+)
from pyngb.validation import validate_sta_data

# Convert once, reuse for multiple operations
df = pl.from_arrow(table)

# Validation with Polars DataFrame (zero conversion overhead)
issues = validate_sta_data(df)

# Multiple validation calls are now more efficient
temp_analysis = check_temperature_profile(df)  # No conversion needed
mass_analysis = check_mass_data(df)           # No conversion needed
```

### Conversion Overhead (Optimized in v0.0.2)

**Problem**: Multiple conversions between PyArrow and Polars

**Solutions**:
```python
# Before v0.0.2: Multiple conversions
table = read_ngb("sample.ngb-ss3")
df1 = pl.from_arrow(table)      # Conversion 1
df2 = pl.from_arrow(table)      # Conversion 2 (redundant)
validate_sta_data(table)        # Internal conversion 3

# v0.0.2+: Optimized approach
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)       # Single conversion

# All subsequent operations use the DataFrame directly
validate_sta_data(df)           # No conversion needed
check_temperature_profile(df)   # No conversion needed
check_mass_data(df)            # No conversion needed
```
print(f"Data points: {table.num_rows}")
print(f"Columns: {table.num_columns}")

# Use batch processing for multiple files
from pyngb import BatchProcessor
processor = BatchProcessor(max_workers=4)  # Use multiple cores
```

### Memory Leaks in Batch Processing

**Problem**: Memory usage grows when processing many files

**Solutions**:
```python
import gc

def process_files_memory_safe(file_list):
    for i, file_path in enumerate(file_list):
        table = read_ngb(file_path)

        # Process immediately
        process_table(table)

        # Clear reference
        del table

        # Force garbage collection every 10 files
        if i % 10 == 0:
            gc.collect()
```

## Data Quality Issues

### Unexpected Column Names

**Problem**: Expected columns are missing or have different names

**Solutions**:
```python
table = read_ngb("sample.ngb-ss3")

# Check available columns
print("Available columns:", table.column_names)

# Check for common variations
common_names = ["time", "sample_temperature", "mass", "dsc_signal"]
available = set(table.column_names)

for name in common_names:
    if name in available:
        print(f"✓ Found: {name}")
    else:
        # Look for similar names
        similar = [col for col in available if name.lower() in col.lower()]
        if similar:
            print(f"? Possible matches for '{name}': {similar}")
        else:
            print(f"✗ Missing: {name}")
```

### NaN or Invalid Values

**Problem**: Data contains NaN or unrealistic values

**Solutions**:
```python
import polars as pl

table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Check for NaN values
print("Null counts per column:")
print(df.null_count())

# Check for infinite values
for col in df.columns:
    if df[col].dtype in [pl.Float32, pl.Float64]:
        inf_count = df.filter(pl.col(col).is_infinite()).height
        if inf_count > 0:
            print(f"Column '{col}' has {inf_count} infinite values")

# Check data ranges
print("\nData ranges:")
for col in df.columns:
    if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
        min_val = df[col].min()
        max_val = df[col].max()
        print(f"{col}: {min_val} to {max_val}")
```

### Validation Failures

**Problem**: Data validation reports many issues

**Solutions**:
```python
from pyngb.validation import QualityChecker, validate_sta_data

# Get detailed validation report
df = pl.from_arrow(table)
checker = QualityChecker(df)
result = checker.full_validation()

print("Validation Summary:")
print(f"Valid: {result.is_valid}")
print(f"Errors: {result.summary()['error_count']}")
print(f"Warnings: {result.summary()['warning_count']}")

# Get detailed report
print("\nDetailed Report:")
print(result.report())

# Check specific issues
issues = validate_sta_data(df)
for issue in issues:
    print(f"Issue: {issue}")
```

## Batch Processing Issues

### Batch Processing Fails

**Problem**: Batch processing stops or fails

**Solutions**:
```python
from pyngb import BatchProcessor

# Use error tolerance
processor = BatchProcessor(max_workers=2, verbose=True)
results = processor.process_files(
    file_list,
    skip_errors=True,  # Continue even if some files fail
    output_dir="./output/"
)

# Check results
successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "error"]

print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

# Review failures
for failure in failed:
    print(f"Failed: {failure['file']} - {failure.get('error', 'Unknown error')}")
```

### Output Files Not Created

**Problem**: Batch processing completes but no output files

**Solutions**:
```python
from pathlib import Path

# Check output directory
output_dir = Path("./output/")
if not output_dir.exists():
    output_dir.mkdir(parents=True)

# Check permissions
try:
    test_file = output_dir / "test.txt"
    test_file.write_text("test")
    test_file.unlink()
    print("Output directory is writable")
except PermissionError:
    print("Permission denied - check output directory permissions")

# Verify file creation
results = processor.process_files(files, output_dir=output_dir)
for result in results:
    if result["status"] == "success":
        expected_file = output_dir / f"{Path(result['file']).stem}.parquet"
        if expected_file.exists():
            print(f"✓ Created: {expected_file}")
        else:
            print(f"✗ Missing: {expected_file}")
```

## Command Line Issues

### CLI Command Not Found

**Problem**: `python -m pyngb` fails

**Solutions**:
```bash
# Check if pyngb is properly installed
python -c "import pyngb; print(pyngb.__version__)"

# Try explicit python path
python -m pyngb sample.ngb-ss3

# Check if __main__.py exists
python -c "import pyngb.__main__; print('CLI module found')"

# Use direct function call if CLI fails
python -c "from pyngb import read_ngb; print(read_ngb('sample.ngb-ss3').num_rows)"
```

### CLI Arguments Not Working

**Problem**: CLI arguments are ignored or cause errors

**Solutions**:
```bash
# Check CLI help
python -m pyngb --help

# Use supported flags
python -m pyngb sample.ngb-ss3 -f parquet -o ./output/

# Quote paths with spaces
python -m pyngb "file with spaces.ngb-ss3" -f csv
```

## Integration Issues

### Polars/Pandas Conversion Problems

**Problem**: Issues converting between data formats

**Solutions**:
```python
import polars as pl
import pandas as pd

table = read_ngb("sample.ngb-ss3")

# Safe conversion to Polars
try:
    df_polars = pl.from_arrow(table)
    print("Polars conversion successful")
except Exception as e:
    print(f"Polars conversion failed: {e}")
    # Try column by column
    for col_name in table.column_names:
        try:
            col_data = table[col_name].to_pylist()
            print(f"Column '{col_name}': {len(col_data)} values")
        except Exception as col_e:
            print(f"Issue with column '{col_name}': {col_e}")

# Safe conversion to Pandas
try:
    df_pandas = df_polars.to_pandas()
    print("Pandas conversion successful")
except Exception as e:
    print(f"Pandas conversion failed: {e}")
```

### Plotting Issues

**Problem**: Cannot plot data with matplotlib/seaborn

**Solutions**:
```python
import matplotlib.pyplot as plt
import polars as pl

table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Check data types
print("Column types:")
for col, dtype in zip(df.columns, df.dtypes):
    print(f"{col}: {dtype}")

# Handle plotting issues
try:
    if "time" in df.columns and "sample_temperature" in df.columns:
        time_data = df["time"].to_numpy()
        temp_data = df["sample_temperature"].to_numpy()

        plt.figure(figsize=(10, 6))
        plt.plot(time_data, temp_data)
        plt.xlabel("Time")
        plt.ylabel("Temperature")
        plt.show()
except Exception as e:
    print(f"Plotting failed: {e}")

    # Debug data
    print(f"Time data type: {type(time_data)}")
    print(f"Time data shape: {time_data.shape if hasattr(time_data, 'shape') else 'No shape'}")
    print(f"Temperature data type: {type(temp_data)}")
```

## Getting Help

### Reporting Issues

When reporting issues, please include:

1. **System Information**:
   ```python
   import sys
   import pyngb
   print(f"Python: {sys.version}")
   print(f"pyngb: {pyngb.__version__}")
   print(f"Platform: {sys.platform}")
   ```

2. **Error Details**:
   - Full error message and traceback
   - Minimal code example that reproduces the issue
   - Sample file (if possible) or file characteristics

3. **Expected vs Actual Behavior**:
   - What you expected to happen
   - What actually happened

### Debug Information Collection

```python
def collect_debug_info(file_path):
    """Collect debug information for troubleshooting."""
    from pathlib import Path
    import zipfile
    import sys
    import pyngb

    info = {
        "system": {
            "python_version": sys.version,
            "pyngb_version": pyngb.__version__,
            "platform": sys.platform
        },
        "file": {
            "path": str(file_path),
            "exists": Path(file_path).exists(),
            "size": Path(file_path).stat().st_size if Path(file_path).exists() else None
        }
    }

    # Check ZIP structure
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            info["zip"] = {
                "valid": True,
                "files": z.namelist(),
                "file_sizes": {name: z.getinfo(name).file_size for name in z.namelist()}
            }
    except Exception as e:
        info["zip"] = {"valid": False, "error": str(e)}

    # Try parsing
    try:
        table = read_ngb(file_path)
        info["parsing"] = {
            "success": True,
            "rows": table.num_rows,
            "columns": table.num_columns,
            "column_names": table.column_names
        }
    except Exception as e:
        info["parsing"] = {"success": False, "error": str(e)}

    return info

# Use it like this:
debug_info = collect_debug_info("problematic_file.ngb-ss3")
print(json.dumps(debug_info, indent=2))
```

### Community Resources

- **GitHub Issues**: [Report bugs and request features](https://github.com/GraysonBellamy/pyngb/issues)
- **GitHub Discussions**: [Ask questions and share ideas](https://github.com/GraysonBellamy/pyngb/discussions)
- **Documentation**: [Complete user guide](https://graysonbellamy.github.io/pyngb/)

### Getting Quick Help

For quick help with common issues:

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Use verbose mode in batch processing
processor = BatchProcessor(verbose=True)

# Check validation details
result = checker.full_validation()
print(result.report())
```

Remember: Most issues can be resolved by checking file formats, verifying installations, and using the comprehensive error messages provided by pyngb.
