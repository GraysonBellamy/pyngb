# Quick Start Guide

This guide will help you get started with pyngb quickly.

## Basic Usage

### Loading Data

pyngb provides two main functions for loading data:

```python
from pyngb import load_ngb_data, get_sta_data

# Method 1: Load as PyArrow Table (recommended for large datasets)
table = load_ngb_data("sample.ngb-ss3")
print(f"Loaded {table.num_rows} rows with {len(table.column_names)} columns")

# Method 2: Get structured data with metadata
metadata, data = get_sta_data("sample.ngb-ss3")
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
temperature_data = df.filter(pl.col("temperature") > 100)

# Save to files
df.write_parquet("output.parquet")
df.write_csv("output.csv")
```

## Command Line Interface

pyngb includes a powerful CLI for batch processing:

### Basic Commands

```bash
# Convert a single file to Parquet
python -m pyngb sample.ngb-ss3 --format parquet

# Convert to CSV
python -m pyngb sample.ngb-ss3 --format csv

# Convert to all formats (Parquet, CSV, JSON)
python -m pyngb sample.ngb-ss3 --format all
```

### Batch Processing

```bash
# Process all files in current directory
python -m pyngb *.ngb-ss3 --format parquet

# Process files with custom output directory
python -m pyngb *.ngb-ss3 --format all --output ./results/

# Extract metadata only
python -m pyngb *.ngb-ss3 --metadata-only --format json
```

### Advanced Options

```bash
# Verbose output
python -m pyngb sample.ngb-ss3 --format parquet --verbose

# Quiet mode (minimal output)
python -m pyngb *.ngb-ss3 --format csv --quiet

# Get help
python -m pyngb --help
```

## Common Use Cases

### Data Exploration

```python
from pyngb import load_ngb_data
import polars as pl

# Load and explore
table = load_ngb_data("sample.ngb-ss3")
df = pl.from_arrow(table)

# Check data structure
print("Available columns:", df.columns)
print("Data types:", df.dtypes)
print("Shape:", df.shape)

# Basic statistics
print(df.select(pl.col("temperature", "time", "dsc")).describe())

# Check for missing values
print(df.null_count())
```

### Plotting Data

```python
import matplotlib.pyplot as plt
import polars as pl
from pyngb import load_ngb_data

# Load data
table = load_ngb_data("sample.ngb-ss3")
df = pl.from_arrow(table)

# Simple temperature vs time plot
if 'time' in df.columns and 'temperature' in df.columns:
    plt.figure(figsize=(10, 6))
    plt.plot(df['time'], df['temperature'])
    plt.xlabel('Time (s)')
    plt.ylabel('Temperature (°C)')
    plt.title('Temperature Program')
    plt.grid(True)
    plt.show()

# Multiple plots
if 'dsc' in df.columns:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    # Temperature plot
    ax1.plot(df['time'], df['temperature'])
    ax1.set_ylabel('Temperature (°C)')
    ax1.grid(True)

    # DSC plot
    ax2.plot(df['time'], df['dsc'])
    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('DSC (mW/mg)')
    ax2.grid(True)

    plt.tight_layout()
    plt.show()
```

### Batch Processing Multiple Files

```python
from pathlib import Path
from pyngb import get_sta_data
import polars as pl

# Process all files in a directory
data_dir = Path("./sta_files")
results = []

for file in data_dir.glob("*.ngb-ss3"):
    try:
        metadata, data = get_sta_data(str(file))

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
from pyngb import load_ngb_data

# Load multiple files and combine
files = ["sample1.ngb-ss3", "sample2.ngb-ss3", "sample3.ngb-ss3"]
all_data = []

for file in files:
    table = load_ngb_data(file)
    df = pl.from_arrow(table)
    df = df.with_columns(pl.lit(file).alias("source_file"))
    all_data.append(df)

# Combine all data
combined_df = pl.concat(all_data)

# Analysis
print("Combined dataset shape:", combined_df.shape)

# Group by source file and get statistics
stats = combined_df.group_by("source_file").agg([
    pl.col("temperature").mean().alias("avg_temp"),
    pl.col("temperature").max().alias("max_temp"),
    pl.col("time").max().alias("duration")
])

print("Statistics by file:")
print(stats)
```

## Tips and Best Practices

!!! tip "Performance Tips"
    - Use `load_ngb_data()` for large datasets - it returns PyArrow tables which are more memory efficient
    - Convert to Polars DataFrames for analysis - they're faster than Pandas for most operations
    - Use Parquet format for storing processed data - it's much faster to read/write than CSV

!!! warning "Common Pitfalls"
    - Always check if expected columns exist before using them
    - Handle exceptions when processing multiple files
    - Be aware of memory usage with very large datasets

!!! info "Next Steps"
    - Check the [API Reference](api.md) for detailed function documentation
    - See [Development](development.md) for contributing guidelines
    - Browse the [troubleshooting guide](troubleshooting.md) for common issues
