# Troubleshooting

Common issues and solutions when using pyngb.

## Installation Issues

### Package Not Found

**Problem**: `pip install pyngb` fails with "package not found"

**Solutions**:
```bash
# Update pip first
pip install --upgrade pip

# Try with explicit PyPI index
pip install --index-url https://pypi.org/simple/ pyngb

# Install from source
pip install git+https://github.com/GraysonBellamy/pyngb.git
```

### Dependency Conflicts

**Problem**: Installation fails due to conflicting dependencies

**Solutions**:
```bash
# Create clean virtual environment
python -m venv pyngb_env
source pyngb_env/bin/activate  # Windows: pyngb_env\Scripts\activate
pip install pyngb

# Use uv for better dependency resolution
pip install uv
uv pip install pyngb
```

### Import Errors

**Problem**: `ImportError: No module named 'pyngb'` after installation

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

### File Not Found

**Problem**: File exists but pyngb can't find it

**Solutions**:
```python
from pathlib import Path
from pyngb import read_ngb

# Use absolute paths
file_path = Path("sample.ngb-ss3").absolute()
table = read_ngb(str(file_path))

# Check file exists and extension
if not Path("sample.ngb-ss3").exists():
    print("File not found!")

# List available NGB files
ngb_files = list(Path.cwd().glob("*.ngb-*"))
print(f"Found {len(ngb_files)} NGB files")
```

### Corrupted File Error

**Problem**: File appears corrupted or invalid

**Solutions**:
```python
import zipfile
from pathlib import Path

# Check if file is valid ZIP
try:
    with zipfile.ZipFile("sample.ngb-ss3", "r") as z:
        print("ZIP contents:", z.namelist())

        # Check for expected files
        expected = ["measurement", "metadata"]
        found = [name for name in z.namelist() if any(exp in name.lower() for exp in expected)]
        print("Expected files found:", found)

except zipfile.BadZipFile:
    print("File is not a valid ZIP - may be corrupted")

    # Check file size
    size = Path("sample.ngb-ss3").stat().st_size
    print(f"File size: {size} bytes")
    if size < 1000:
        print("File seems too small to be valid NGB")
```

### Empty or Missing Data

**Problem**: File parses but contains no useful data

**Solutions**:
```python
import json
from pyngb import read_ngb

table = read_ngb("sample.ngb-ss3")

# Check data structure
print(f"Shape: {table.num_rows} rows × {table.num_columns} columns")
print(f"Columns: {table.column_names}")

# Check metadata
if b'file_metadata' in table.schema.metadata:
    metadata = json.loads(table.schema.metadata[b'file_metadata'])
    print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
    print(f"Instrument: {metadata.get('instrument', 'Unknown')}")

    # Check temperature program
    if 'temperature_program' in metadata:
        stages = len([k for k in metadata['temperature_program'].keys() if k.startswith('stage_')])
        print(f"Temperature program has {stages} stages")
else:
    print("No metadata found - this may indicate parsing issues")
```

## Memory and Performance Issues

### Out of Memory

**Problem**: `MemoryError` when processing large files

**Solutions**:
```python
from pathlib import Path

# Check file size
file_size = Path("large_file.ngb-ss3").stat().st_size
print(f"File size: {file_size / (1024**2):.1f} MB")

# For files >100MB, consider chunking
if file_size > 100 * 1024 * 1024:
    print("Large file detected - using chunked processing")

    table = read_ngb("large_file.ngb-ss3")
    chunk_size = 10000

    for i in range(0, table.num_rows, chunk_size):
        end_idx = min(i + chunk_size, table.num_rows)
        chunk = table.slice(i, end_idx - i)
        # Process chunk
        print(f"Processing rows {i} to {end_idx}")
```

### Slow Performance

**Problem**: Parsing takes too long

**Solutions**:
```python
import time

# Measure parsing performance
start_time = time.time()
table = read_ngb("sample.ngb-ss3")
parse_time = time.time() - start_time

print(f"Parse time: {parse_time:.2f} seconds")
print(f"Rate: {table.num_rows / parse_time:.0f} rows/second")

# For multiple files, use batch processing
from pyngb import BatchProcessor

processor = BatchProcessor(max_workers=4)
results = processor.process_files(file_list, skip_errors=True)
```

## Data Quality Issues

### Missing Expected Columns

**Problem**: Required columns like 'mass' or 'temperature' are missing

**Solutions**:
```python
import polars as pl

table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Check for required columns
required = ["time", "sample_temperature", "mass"]
available = set(df.columns)

for col in required:
    if col in available:
        print(f"✓ Found: {col}")
    else:
        # Look for similar names
        similar = [c for c in available if col.lower() in c.lower() or c.lower() in col.lower()]
        if similar:
            print(f"? Possible matches for '{col}': {similar}")
        else:
            print(f"✗ Missing: {col}")

print(f"\nAll available columns: {sorted(df.columns)}")
```

### NaN or Invalid Values

**Problem**: Data contains NaN, infinite, or unrealistic values

**Solutions**:
```python
import polars as pl
import numpy as np

df = pl.from_arrow(table)

# Check for null values
print("Null counts:")
print(df.null_count())

# Check for infinite values in numeric columns
for col in df.columns:
    if df[col].dtype in [pl.Float32, pl.Float64]:
        values = df[col].to_numpy()
        inf_count = np.sum(np.isinf(values))
        if inf_count > 0:
            print(f"Column '{col}': {inf_count} infinite values")

# Check data ranges
print("\nData ranges:")
numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Float32, pl.Float64]]
print(df.select(numeric_cols).describe())

# Look for outliers
if "mass" in df.columns:
    mass_values = df["mass"].to_numpy()
    mass_change = abs(mass_values[-1] - mass_values[0])
    if mass_change < 0.01:
        print(f"Warning: Very small mass change ({mass_change:.3f} mg)")
```

## Baseline Subtraction Issues

### Temperature Program Mismatch

**Problem**: Baseline subtraction fails due to incompatible temperature programs

**Solutions**:
```python
import json
from pyngb import read_ngb

# Compare temperature programs
sample_metadata, _ = read_ngb("sample.ngb-ss3", return_metadata=True)
baseline_metadata, _ = read_ngb("baseline.ngb-bs3", return_metadata=True)

sample_program = sample_metadata.get('temperature_program', {})
baseline_program = baseline_metadata.get('temperature_program', {})

print("Sample temperature program:")
for stage, params in sample_program.items():
    if stage.startswith('stage_'):
        print(f"  {stage}: {params}")

print("\nBaseline temperature program:")
for stage, params in baseline_program.items():
    if stage.startswith('stage_'):
        print(f"  {stage}: {params}")

# Check for differences
sample_stages = set(sample_program.keys())
baseline_stages = set(baseline_program.keys())

if sample_stages != baseline_stages:
    print(f"\nStage mismatch:")
    print(f"  Sample has: {sample_stages}")
    print(f"  Baseline has: {baseline_stages}")
```

### Dynamic Axis Issues

**Problem**: Baseline subtraction with wrong alignment axis

**Solutions**:
```python
from pyngb import read_ngb

# Try different dynamic axes
axes = ["time", "sample_temperature", "furnace_temperature"]

for axis in axes:
    try:
        corrected = read_ngb(
            "sample.ngb-ss3",
            baseline_file="baseline.ngb-bs3",
            dynamic_axis=axis
        )
        print(f"✓ Success with axis: {axis}")
        print(f"  Result shape: {corrected.num_rows} × {corrected.num_columns}")
        break
    except Exception as e:
        print(f"✗ Failed with axis {axis}: {e}")
```

## Batch Processing Issues

### Processing Failures

**Problem**: Batch processing stops or fails

**Solutions**:
```python
from pyngb import BatchProcessor
from pathlib import Path

# Use error tolerance
processor = BatchProcessor(max_workers=2, verbose=True)

# Get list of files
files = list(Path(".").glob("*.ngb-ss3"))
print(f"Found {len(files)} files to process")

# Process with error handling
results = processor.process_files(
    [str(f) for f in files],
    skip_errors=True,  # Continue even if some fail
    output_dir="./output/"
)

# Analyze results
successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "error"]

print(f"\nResults: {len(successful)} success, {len(failed)} failed")

# Review failures
for failure in failed[:5]:  # Show first 5 failures
    print(f"Failed: {Path(failure['file']).name}")
    print(f"  Error: {failure.get('error', 'Unknown')}")
```

## Command Line Issues

### CLI Not Working

**Problem**: `python -m pyngb` command fails

**Solutions**:
```bash
# Verify pyngb installation
python -c "import pyngb; print(f'pyngb {pyngb.__version__} installed')"

# Check CLI module
python -c "import pyngb.__main__; print('CLI module available')"

# Use full module path if needed
python -m pyngb.api.loaders sample.ngb-ss3

# Alternative: use function directly
python -c "
from pyngb import read_ngb
import pyarrow.parquet as pq
table = read_ngb('sample.ngb-ss3')
pq.write_table(table, 'output.parquet')
print('File converted successfully')
"
```

### Permission Errors

**Problem**: Cannot write output files

**Solutions**:
```bash
# Check output directory permissions
ls -la ./output/

# Create output directory if needed
mkdir -p ./output/
chmod 755 ./output/

# Use different output location
python -m pyngb sample.ngb-ss3 -o ~/Documents/ngb_output/

# Check disk space
df -h .
```

## Getting Help

### Debug Information

When reporting issues, collect this information:

```python
import sys
import platform
from pathlib import Path

def debug_info():
    try:
        import pyngb
        version = pyngb.__version__
    except:
        version = "not installed"

    print(f"Python: {sys.version}")
    print(f"Platform: {platform.platform()}")
    print(f"pyngb: {version}")

    # Check dependencies
    deps = ['polars', 'pyarrow', 'numpy']
    for dep in deps:
        try:
            mod = __import__(dep)
            print(f"{dep}: {getattr(mod, '__version__', 'unknown')}")
        except ImportError:
            print(f"{dep}: not installed")

debug_info()
```

### File-Specific Debug

For file-specific issues:

```python
def debug_file(filepath):
    from pathlib import Path
    import zipfile

    path = Path(filepath)

    print(f"File: {path.name}")
    print(f"Exists: {path.exists()}")

    if path.exists():
        print(f"Size: {path.stat().st_size:,} bytes")

        try:
            with zipfile.ZipFile(path, 'r') as z:
                print(f"ZIP contents: {len(z.namelist())} files")
                for name in z.namelist()[:10]:  # First 10 files
                    size = z.getinfo(name).file_size
                    print(f"  {name}: {size:,} bytes")
        except:
            print("Not a valid ZIP file")

debug_file("problematic_file.ngb-ss3")
```

### Community Support

- **GitHub Issues**: [Report bugs](https://github.com/GraysonBellamy/pyngb/issues)
- **GitHub Discussions**: [Ask questions](https://github.com/GraysonBellamy/pyngb/discussions)

When reporting issues, include:
1. Complete error message and traceback
2. Debug information from above
3. Sample file (if possible) or file characteristics
4. Expected vs actual behavior

Most issues can be resolved by verifying file formats, checking installations, and using the error messages provided by pyngb.
