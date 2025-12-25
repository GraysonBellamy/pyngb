# Getting Started

This guide will help you install pyngb and parse your first NGB file.

## Installation

### Requirements

- Python 3.10 or higher
- Operating System: Windows, macOS, or Linux

### Install from PyPI

```bash
pip install pyngb
```

### Verify Installation

```bash
python -c "import pyngb; print('pyngb installed successfully')"
```

### Development Installation

If you want to contribute or use the latest features:

```bash
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb
pip install -e ".[dev]"
```

## Your First NGB File

### Basic File Loading

```python
from pyngb import read_ngb
import polars as pl

# Load an NGB file
table = read_ngb("your_file.ngb-ss3")

# Convert to DataFrame for analysis
df = pl.from_arrow(table)

print(f"Loaded {df.height} data points with {df.width} columns")
print(f"Available columns: {df.columns}")
```

### Accessing Metadata

NGB files contain rich metadata embedded in the file structure:

```python
import json

# Extract metadata from the table schema
metadata = json.loads(table.schema.metadata[b'file_metadata'])

# Print key information
print(f"Sample name: {metadata.get('sample_name', 'Unknown')}")
print(f"Instrument: {metadata.get('instrument', 'Unknown')}")
print(f"Sample mass: {metadata.get('sample_mass', 'Unknown')} mg")
print(f"Operator: {metadata.get('operator', 'Unknown')}")
```

### Basic Data Exploration

```python
# Check data types and basic statistics
print(df.describe())

# Temperature range
if "sample_temperature" in df.columns:
    temp_min = df["sample_temperature"].min()
    temp_max = df["sample_temperature"].max()
    print(f"Temperature range: {temp_min:.1f} to {temp_max:.1f} °C")

# Mass loss analysis
if "mass" in df.columns:
    initial_mass = df["mass"].max()
    final_mass = df["mass"].min()
    mass_loss_percent = (initial_mass - final_mass) / initial_mass * 100
    print(f"Mass loss: {mass_loss_percent:.2f}%")
```

## Command Line Interface

pyngb includes a powerful CLI for batch processing and automation.

### Basic Usage

```bash
# Convert single file to Parquet (default)
python -m pyngb sample.ngb-ss3

# Convert to CSV
python -m pyngb sample.ngb-ss3 --format csv

# Convert to both formats
python -m pyngb sample.ngb-ss3 --format both

# Specify output directory
python -m pyngb sample.ngb-ss3 --output ./processed_data/
```

### Batch Processing

```bash
# Process all NGB files in current directory
python -m pyngb *.ngb-ss3

# Process files with verbose output
python -m pyngb *.ngb-ss3 --verbose

# Process with custom output location
python -m pyngb experiments/*.ngb-ss3 --output ./results/ --format both
```

### Get Help

```bash
python -m pyngb --help
```

## Data Export Options

### Parquet (Recommended)

Parquet preserves all data types and metadata efficiently:

```python
# Parquet files are created automatically by CLI
# or manually:
import pyarrow.parquet as pq
pq.write_table(table, "output.parquet")
```

### CSV Export

```python
# Convert to CSV (loses metadata)
df.write_csv("output.csv")
```

### Manual Export

```python
# Export metadata separately
with open("metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)
```

## Quick Visualization

```python
import matplotlib.pyplot as plt

# Simple temperature vs time plot
if "time" in df.columns and "sample_temperature" in df.columns:
    plt.figure(figsize=(10, 6))
    plt.plot(df["time"], df["sample_temperature"])
    plt.xlabel("Time (s)")
    plt.ylabel("Temperature (°C)")
    plt.title("Temperature Program")
    plt.show()

# Mass vs temperature (TGA curve)
if "sample_temperature" in df.columns and "mass" in df.columns:
    plt.figure(figsize=(10, 6))
    plt.plot(df["sample_temperature"], df["mass"])
    plt.xlabel("Temperature (°C)")
    plt.ylabel("Mass (mg)")
    plt.title("Thermogravimetric Analysis")
    plt.show()
```

## Next Steps

Now that you can load and examine NGB files, explore these advanced features:

- **[User Guide](user-guide.md)** - Learn about baseline correction, DTG analysis, and batch processing
- **[API Reference](api-reference.md)** - Complete function documentation
- **[Troubleshooting](troubleshooting.md)** - Solutions to common issues

## Common File Types

pyngb supports these NETZSCH file extensions:

- `.ngb-ss3` - Sample files (main data)
- `.ngb-bs3` - Baseline files (for correction)

Both contain the same data structure and can be parsed identically.
