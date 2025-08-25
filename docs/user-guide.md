# User Guide

This guide covers the essential functionality of pyngb for parsing and analyzing NETZSCH NGB files.

## Core Functions

### Loading NGB Files

The primary function for loading NGB files is `read_ngb()`:

```python
from pyngb import read_ngb
import polars as pl
import json

# Basic loading
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Load with metadata separated
metadata, table = read_ngb("sample.ngb-ss3", return_metadata=True)

# Access embedded metadata
metadata_json = json.loads(table.schema.metadata[b'file_metadata'])
print(f"Sample: {metadata_json.get('sample_name', 'Unknown')}")
```

### Baseline Subtraction

Automatically subtract baseline measurements from sample data:

```python
# Method 1: Integrated with loading
corrected_table = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)

# Method 2: Standalone function
from pyngb import subtract_baseline
corrected_df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")

# Custom dynamic axis for alignment
corrected_table = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3",
    dynamic_axis="time"  # Options: "time", "sample_temperature", "furnace_temperature"
)
```

**How it works:**
- Automatically detects isothermal vs dynamic segments
- Aligns data using specified axis (default: sample_temperature)
- Subtracts only `mass` and `dsc_signal` columns
- Preserves time, temperature, and flow data from sample

### DTG Analysis

Calculate derivative thermogravimetry with smart defaults:

```python
from pyngb.api.analysis import add_dtg

# Add DTG column to your table (recommended)
table_with_dtg = add_dtg(table, method="savgol", smooth="medium")
df = pl.from_arrow(table_with_dtg)

# Alternative: Standalone DTG calculation
from pyngb import dtg
time = df.get_column('time').to_numpy()
mass = df.get_column('mass').to_numpy()
dtg_values = dtg(time, mass)
```

**Smoothing options:**
- `"strict"` - Preserve all features (quantitative analysis)
- `"medium"` - Balanced smoothing (default, recommended)
- `"loose"` - Heavy smoothing (noisy data)

**Methods:**
- `"savgol"` - Savitzky-Golay (recommended, smooth and accurate)
- `"gradient"` - NumPy gradient (fast, simple)

## Batch Processing

Process multiple files efficiently:

```python
from pyngb import BatchProcessor

# Initialize processor
processor = BatchProcessor(max_workers=4, verbose=True)

# Process multiple files
results = processor.process_files(
    ["file1.ngb-ss3", "file2.ngb-ss3", "file3.ngb-ss3"],
    output_format="parquet",
    output_dir="./processed_data/"
)

# Check results
successful = [r for r in results if r["status"] == "success"]
print(f"Successfully processed {len(successful)} files")
```

## Data Export Formats

### Parquet (Recommended)

Preserves all data types and metadata:

```python
import pyarrow.parquet as pq

# Export table directly
pq.write_table(table, "output.parquet")

# Load back with metadata intact
loaded_table = pq.read_table("output.parquet")
metadata = json.loads(loaded_table.schema.metadata[b'file_metadata'])
```

### CSV Export

```python
# Convert to DataFrame first
df = pl.from_arrow(table)
df.write_csv("output.csv")

# Note: Metadata is lost in CSV format
```

### JSON Metadata

```python
# Extract and save metadata
metadata = json.loads(table.schema.metadata[b'file_metadata'])
with open("metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)
```

## Data Integration

### Working with Pandas

```python
import pandas as pd

# Convert PyArrow table to Pandas
df_pandas = table.to_pandas()

# Or via Polars
df_polars = pl.from_arrow(table)
df_pandas = df_polars.to_pandas()
```

### Working with NumPy

```python
import numpy as np

# Extract columns as NumPy arrays
temperature = table['sample_temperature'].to_numpy()
mass = table['mass'].to_numpy()
time = table['time'].to_numpy()

# Perform NumPy operations
mass_loss = (mass[0] - mass) / mass[0] * 100  # Mass loss percentage
```

## Common Analysis Patterns

### Temperature Range Analysis

```python
df = pl.from_arrow(table)

if "sample_temperature" in df.columns:
    temp_stats = df.select([
        pl.col("sample_temperature").min().alias("temp_min"),
        pl.col("sample_temperature").max().alias("temp_max"),
        pl.col("sample_temperature").mean().alias("temp_mean")
    ])
    print(temp_stats)
```

### Mass Loss Calculation

```python
if "mass" in df.columns:
    initial_mass = df["mass"].max()
    final_mass = df["mass"].min()
    total_loss = initial_mass - final_mass
    percent_loss = (total_loss / initial_mass) * 100

    print(f"Initial mass: {initial_mass:.3f} mg")
    print(f"Final mass: {final_mass:.3f} mg")
    print(f"Mass loss: {total_loss:.3f} mg ({percent_loss:.2f}%)")
```

### Peak Detection in DTG

```python
from scipy.signal import find_peaks
import numpy as np

# Calculate DTG
table_with_dtg = add_dtg(table, smooth="medium")
df = pl.from_arrow(table_with_dtg)

# Extract arrays
temperature = df["sample_temperature"].to_numpy()
dtg_values = df["dtg"].to_numpy()

# Find peaks (negative for mass loss)
peaks, properties = find_peaks(-dtg_values, height=0.01, distance=50)

# Peak temperatures
peak_temps = temperature[peaks]
print(f"DTG peaks at: {peak_temps} °C")
```

### Data Quality Checks

```python
# Check for missing values
missing_data = df.null_count()
print("Missing values per column:", missing_data)

# Check data ranges
if "mass" in df.columns:
    mass_range = df["mass"].max() - df["mass"].min()
    if mass_range < 0.1:  # Very small mass change
        print("Warning: Very small mass change detected")

# Check temperature program
if "sample_temperature" in df.columns:
    temp_range = df["sample_temperature"].max() - df["sample_temperature"].min()
    if temp_range < 50:  # Less than 50°C range
        print("Warning: Small temperature range")
```

## Command Line Usage

### Basic Conversion

```bash
# Convert single file to Parquet
python -m pyngb sample.ngb-ss3

# Convert to CSV
python -m pyngb sample.ngb-ss3 --format csv

# Convert to both formats
python -m pyngb sample.ngb-ss3 --format both
```

### Batch Processing with CLI

```bash
# Process all files in directory
python -m pyngb *.ngb-ss3 --output ./processed/

# With baseline correction
python -m pyngb *.ngb-ss3 --baseline baseline.ngb-bs3 --output ./corrected/

# Verbose output
python -m pyngb *.ngb-ss3 --verbose --format parquet
```

### Advanced CLI Options

```bash
# Custom dynamic axis for baseline subtraction
python -m pyngb sample.ngb-ss3 --baseline baseline.ngb-bs3 --dynamic-axis time

# Specific output directory with all formats
python -m pyngb experiments/*.ngb-ss3 --format all --output ./results/
```

## Performance Tips

### Memory Management

```python
# For large files, process in chunks
chunk_size = 10000
for i in range(0, table.num_rows, chunk_size):
    chunk = table.slice(i, chunk_size)
    # Process chunk...
```

### Parallel Processing

```python
# Use BatchProcessor for multiple files
processor = BatchProcessor(max_workers=4)  # Adjust based on CPU cores
results = processor.process_files(file_list)
```

### Efficient Data Access

```python
# Access specific columns efficiently
temperature_col = table.column('sample_temperature')
mass_col = table.column('mass')

# Convert to NumPy only when needed
temp_array = temperature_col.to_numpy()
```

## Error Handling

```python
from pyngb.exceptions import NGBParsingError, ValidationError

try:
    table = read_ngb("sample.ngb-ss3")
except NGBParsingError as e:
    print(f"Parsing failed: {e}")
except FileNotFoundError:
    print("File not found")
except ValidationError as e:
    print(f"Data validation failed: {e}")
```

## Next Steps

- **[API Reference](api-reference.md)** - Complete function documentation
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
- **[Contributing](contributing.md)** - Development guidelines

This guide covers the essential workflows for most thermal analysis applications. For advanced customization and additional features, refer to the complete API documentation.
