# Quick Start Guide

This guide will help you get started with pyngb quickly.

## Basic Usage

### Loading Data

pyngb provides a simple, intuitive function for loading data:

```python
from pyngb import read_ngb

# Default mode: Load as PyArrow Table with embedded metadata
table = read_ngb("sample.ngb-ss3")
print(f"Loaded {table.num_rows} rows with {table.num_columns} columns")

# Advanced mode: Get structured data with separate metadata
metadata, data = read_ngb("sample.ngb-ss3", return_metadata=True)
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
```

### Working with Data

Convert to different formats and analyze:

```python
import polars as pl

# Convert to Polars DataFrame
df = pl.from_arrow(table)

# Basic analysis
print(df.describe())
print(f"Columns: {df.columns}")

# Filter data
temperature_data = df.filter(pl.col("sample_temperature") > 100)

# Save to files
df.write_parquet("output.parquet")
df.write_csv("output.csv")
```

### Baseline Subtraction

pyngb supports automatic baseline subtraction with temperature program validation:

```python
from pyngb import read_ngb, subtract_baseline

# Method 1: Integrated approach (recommended)
corrected_data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)

# Method 2: Standalone function
corrected_df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")

# Advanced options
corrected_data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3",
    dynamic_axis="time"  # Default: "sample_temperature"
)
```

**Key Features:**
- Automatic temperature program validation ensures scientific accuracy
- Dynamic segments aligned by sample temperature (default) or time/furnace temperature
- Isothermal segments always aligned by time
- Only measurement columns (mass, DSC signal) are corrected

## Command Line Interface

pyngb includes a powerful CLI for batch processing:

### Basic Commands (single file)

```bash
python -m pyngb sample.ngb-ss3 -f parquet

# Convert to CSV
python -m pyngb sample.ngb-ss3 -f csv

# Convert to both formats (Parquet and CSV)
python -m pyngb sample.ngb-ss3 -f all
```

### Baseline Subtraction

```bash
# Basic baseline subtraction (default: sample_temperature axis)
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3

# Use time axis for dynamic segments
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time

# Use furnace temperature axis for dynamic segments
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis furnace_temperature

# Output includes "_baseline_subtracted" suffix
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -f all -o ./corrected/
```

### Multiple files

```bash
# The CLI processes one input file at a time.
# Use a simple shell loop for multiple files, or see the Batch Processor.
for f in *.ngb-ss3; do
    python -m pyngb "$f" -f parquet -o ./results
done
```

### Advanced Options

```bash
# Verbose output
python -m pyngb sample.ngb-ss3 -f parquet -v

# Get help
python -m pyngb --help
```

## Common Use Cases

### Data Exploration

```python
from pyngb import read_ngb
import polars as pl

# Load and explore
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Check data structure
print("Available columns:", df.columns)
print("Data types:", df.dtypes)
print("Shape:", df.shape)

# Basic statistics
print(df.select(pl.col("sample_temperature", "time", "dsc_signal")).describe())

# Check for missing values
print(df.null_count())
```

### Plotting Data

```python
import matplotlib.pyplot as plt
import polars as pl
from pyngb import read_ngb

# Load data
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Simple temperature vs time plot
if 'time' in df.columns and 'sample_temperature' in df.columns:
    plt.figure(figsize=(10, 6))
    plt.plot(df['time'], df['sample_temperature'])
    plt.xlabel('Time (s)')
    plt.ylabel('Sample Temperature (°C)')
    plt.title('Sample Temperature Program')
    plt.grid(True)
    plt.show()

# Multiple plots
if 'dsc_signal' in df.columns:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    # Temperature plot
    ax1.plot(df['time'], df['sample_temperature'])
    ax1.set_ylabel('Sample Temperature (°C)')
    ax1.grid(True)

    # DSC plot
    ax2.plot(df['time'], df['dsc_signal'])
    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('DSC Signal (µV)')
    ax2.grid(True)

    plt.tight_layout()
    plt.show()
```

### Batch Processing Multiple Files

```python
from pathlib import Path
from pyngb import read_ngb
import polars as pl

# Process all files in a directory
data_dir = Path("./sta_files")
results = []

for file in data_dir.glob("*.ngb-ss3"):
    try:
        metadata, data = read_ngb(str(file), return_metadata=True)

        # Extract key information
        result = {
            'filename': file.name,
            'sample_name': metadata.get('sample_name', 'Unknown'),
            'operator': metadata.get('operator', 'Unknown'),
            'data_points': data.num_rows,
            'columns': len(data.column_names),
            'file_size_mb': file.stat().st_size / 1024 / 1024
        }
        results.append(result)
        print(f"✓ Processed {file.name}")

    except Exception as e:
        print(f"✗ Error processing {file.name}: {e}")

# Create summary DataFrame
if results:
    summary_df = pl.DataFrame(results)
    print("\nProcessing Summary:")
    print(summary_df)

    # Save summary
    summary_df.write_csv("processing_summary.csv")
    print("Summary saved to processing_summary.csv")
```

### Data Analysis Workflow

```python
import polars as pl
from pyngb import read_ngb

# Load multiple files and combine
files = ["sample1.ngb-ss3", "sample2.ngb-ss3", "sample3.ngb-ss3"]
all_data = []

for file in files:
    table = read_ngb(file)
    df = pl.from_arrow(table)
    df = df.with_columns(pl.lit(file).alias("source_file"))
    all_data.append(df)

# Combine all data
combined_df = pl.concat(all_data)

# Analysis
print("Combined dataset shape:", combined_df.shape)

# Group by source file and get statistics
stats = combined_df.group_by("source_file").agg([
    pl.col("sample_temperature").mean().alias("avg_temp"),
    pl.col("sample_temperature").max().alias("max_temp"),
    pl.col("time").max().alias("duration")
])

print("Statistics by file:")
print(stats)
```

## Tips and Best Practices

!!! tip "Performance Tips"
    - Use `read_ngb()` for large datasets - it returns PyArrow tables which are more memory efficient
    - Convert to Polars DataFrames for analysis - they're faster than Pandas for most operations
    - Use Parquet format for storing processed data - it's much faster to read/write than CSV
    - **New in v0.0.2**: Optimized conversions automatically reduce overhead when working with both PyArrow and Polars
    - Pass Polars DataFrames directly to validation functions for zero-conversion overhead

!!! warning "Common Pitfalls"
    - Always check if expected columns exist before using them
    - Handle exceptions when processing multiple files
    - Be aware of memory usage with very large datasets

!!! info "Next Steps"
    - Check the [API Reference](api.md) for detailed function documentation
    - See [Development](development.md) for contributing guidelines
    - Browse the [troubleshooting guide](troubleshooting.md) for common issues
