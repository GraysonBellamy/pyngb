# PyNetzsch

PyNetzsch is a Python library for parsing and analyzing NETZSCH STA (Simultaneous Thermal Analysis) data files.

## Features

- Parse NETZSCH `.ngb-ss3` files
- Extract metadata and measurement data
- Export to multiple formats (Parquet, CSV, JSON)
- Command-line interface for batch processing
- Type-safe with modern Python features
- High performance with PyArrow and Polars

## Quick Example

```python
from pynetzsch import load_ngb_data, get_sta_data

# Load data as PyArrow Table
table = load_ngb_data("your_file.ngb-ss3")
print(f"Loaded {table.num_rows} rows with {len(table.column_names)} columns")

# Get structured data with metadata
metadata, data = get_sta_data("your_file.ngb-ss3")
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
```

## Command Line Usage

```bash
# Convert a single file
python -m pynetzsch input.ngb-ss3 --format parquet

# Process multiple files
python -m pynetzsch *.ngb-ss3 --format all --output ./results/

# Get help
python -m pynetzsch --help
```

## Requirements

- Python 3.9+
- polars >= 1.0.0
- pyarrow >= 10.0.0

## Installation

Install from PyPI:

```bash
pip install pynetzsch
```

Or from source:

```bash
git clone https://github.com/GraysonBellamy/pynetzsch.git
cd pynetzsch
pip install -e .
```

## License

This project is licensed under the MIT License.
