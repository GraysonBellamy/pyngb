# pyngb Troubleshooting Guide

This guide helps diagnose and resolve common issues when using pyngb.

## Common Issues

### 1. File Format Issues

#### "File is not a valid ZIP archive"
**Problem**: NGB files are ZIP archives containing binary data streams.

**Solutions**:
```python
import zipfile
from pathlib import Path

# Check if file is a valid ZIP
try:
    with zipfile.ZipFile("your_file.ngb-ss3", "r") as z:
        print("Valid ZIP archive")
        print("Contents:", z.namelist())
except zipfile.BadZipFile:
    print("Not a valid ZIP file - file may be corrupted")
```

#### "NGBStreamNotFoundError: stream_1.table not found"
**Problem**: Required metadata stream is missing.

**Solutions**:
- Verify file completeness (not partially downloaded)
- Check file extension is `.ngb-ss3`
- Try with different NGB file to isolate issue

### 2. Memory Issues

#### Out of Memory Errors
**Problem**: Large NGB files can consume significant memory.

**Solutions**:
```python
import psutil
import gc

# Monitor memory usage
def check_memory():
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024  # MB

print(f"Memory before: {check_memory():.1f} MB")

# Process file
table = load_ngb_data("large_file.ngb-ss3")

print(f"Memory after: {check_memory():.1f} MB")

# Free memory when done
del table
gc.collect()
```

### 3. Performance Issues

#### Slow Parsing
**Problem**: Some NGB files take a long time to parse.

**Solutions**:
```python
import time
from pyngb import load_ngb_data

# Benchmark parsing time
start_time = time.perf_counter()
table = load_ngb_data("your_file.ngb-ss3")
parse_time = time.perf_counter() - start_time

print(f"Parsed {table.num_rows:,} rows in {parse_time:.2f} seconds")
print(f"Rate: {table.num_rows/parse_time:,.0f} rows/sec")

# If too slow, try custom parser config
from pyngb import NGBParser, PatternConfig

config = PatternConfig()
# Remove unnecessary metadata patterns to speed up parsing
config.metadata_patterns = {
    "instrument": config.metadata_patterns["instrument"],
    "sample_name": config.metadata_patterns["sample_name"],
}

parser = NGBParser(config)
metadata, data = parser.parse("your_file.ngb-ss3")
```

### 4. Data Quality Issues

#### Missing Columns
**Problem**: Expected measurement columns are not present.

**Diagnosis**:
```python
from pyngb import load_ngb_data

table = load_ngb_data("your_file.ngb-ss3")
print("Available columns:", table.column_names)

# Check for common STA columns
expected_columns = ['time', 'temperature', 'dsc', 'sample_mass']
missing = [col for col in expected_columns if col not in table.column_names]
if missing:
    print("Missing expected columns:", missing)
```

#### Incorrect Data Values
**Problem**: Data values seem incorrect or out of range.

**Diagnosis**:
```python
import polars as pl

# Convert to DataFrame for analysis
df = pl.from_arrow(table)

# Check data ranges
for col in df.columns:
    if col in ['temperature', 'time', 'sample_mass', 'dsc']:
        values = df[col].drop_nulls()
        if len(values) > 0:
            print(f"{col}: {values.min()} to {values.max()}")
            print(f"  Null values: {df[col].null_count()}")
```

### 5. Import and Dependencies

#### Import Errors
**Problem**: Cannot import pyngb modules.

**Solutions**:
```bash
# Check installation
pip show pyngb

# Reinstall if needed
pip uninstall pyngb
pip install pyngb

# For development installation
pip install -e .
```

#### Version Compatibility
**Problem**: Dependency version conflicts.

**Check versions**:
```python
import polars as pl
import pyarrow as pa
import numpy as np
import pyngb

print(f"pyngb: {pyngb.__version__}")
print(f"Polars: {pl.__version__}")
print(f"PyArrow: {pa.__version__}")
print(f"NumPy: {np.__version__}")
```

## Debugging Tools

### Enable Debug Logging
```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Now run your code - you'll see detailed parsing information
from pyngb import load_ngb_data
table = load_ngb_data("your_file.ngb-ss3")
```

### Custom Validation
```python
def validate_ngb_data(table, metadata=None):
    """Validate parsed NGB data for common issues."""
    issues = []

    # Check for empty data
    if table.num_rows == 0:
        issues.append("No data rows found")

    # Check for essential columns
    essential_cols = ['time', 'temperature']
    missing_cols = [col for col in essential_cols if col not in table.column_names]
    if missing_cols:
        issues.append(f"Missing essential columns: {missing_cols}")

    # Check time sequence
    if 'time' in table.column_names:
        time_values = table.column('time').to_pylist()
        if not all(a <= b for a, b in zip(time_values, time_values[1:])):
            issues.append("Time values are not monotonic")

    # Check temperature range
    if 'temperature' in table.column_names:
        temps = table.column('temperature').to_pylist()
        temp_range = max(temps) - min(temps)
        if temp_range < 10:  # Less than 10°C range
            issues.append(f"Unusual temperature range: {temp_range:.1f}°C")

    return issues

# Usage
table = load_ngb_data("your_file.ngb-ss3")
issues = validate_ngb_data(table)
if issues:
    print("Data validation issues found:")
    for issue in issues:
        print(f"  - {issue}")
else:
    print("Data validation passed")
```

### Memory Profiling
```python
import tracemalloc
from pyngb import load_ngb_data

# Start memory tracing
tracemalloc.start()

# Your code here
table = load_ngb_data("your_file.ngb-ss3")

# Get memory statistics
current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage: {current / 1024 / 1024:.1f} MB")
print(f"Peak memory usage: {peak / 1024 / 1024:.1f} MB")

tracemalloc.stop()
```

## Getting Help

### Check File Information
```python
import zipfile
from pathlib import Path

def diagnose_ngb_file(filepath):
    """Diagnose NGB file for common issues."""
    path = Path(filepath)

    print(f"File: {path.name}")
    print(f"Size: {path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"Extension: {path.suffix}")

    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            files = z.namelist()
            print(f"ZIP contents: {len(files)} files")

            streams = [f for f in files if f.startswith('Streams/')]
            print(f"Stream files: {streams}")

            # Check stream sizes
            for stream in streams:
                info = z.getinfo(stream)
                print(f"  {stream}: {info.file_size} bytes")

    except Exception as e:
        print(f"Error reading ZIP: {e}")

# Usage
diagnose_ngb_file("your_file.ngb-ss3")
```

### Report Issues
If you continue to have problems:

1. **Check the GitHub Issues**: https://github.com/GraysonBellamy/pyngb/issues
2. **Create a minimal example** that reproduces the problem
3. **Include system information**: Python version, OS, dependency versions
4. **Share file information** (but not the actual file if it contains sensitive data)

### Example Issue Report
```
**Environment**:
- pyngb version: 0.1.0
- Python: 3.11.5
- OS: Ubuntu 22.04
- Dependencies: polars 0.20.0, pyarrow 14.0.0

**Problem**:
NGBStreamNotFoundError when parsing specific file

**Code**:
```python
from pyngb import load_ngb_data
table = load_ngb_data("problem_file.ngb-ss3")  # Error here
```

**Error message**:
NGBStreamNotFoundError: stream_1.table not found - metadata unavailable

**File info**:
- Size: 2.3 MB
- Created by: NETZSCH STA 449 F3 Jupiter
- ZIP contents: [list from diagnose_ngb_file()]
```
