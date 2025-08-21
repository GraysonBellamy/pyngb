# pyNGB - NETZSCH STA File Parser

[![PyPI version](https://badge.fury.io/py/pyngb.svg)](https://badge.fury.io/py/pyngb)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/GraysonBellamy/pyngb/workflows/Tests/badge.svg)](https://github.com/GraysonBellamy/pyngb/actions)

A comprehensive Python library for parsing and analyzing NETZSCH STA (Simultaneous Thermal Analysis) NGB files with high performance, extensive metadata extraction, and robust batch processing capabilities.

## 🚨 Disclaimer

**This package and its author are not affiliated with, endorsed by, or approved by NETZSCH-Gerätebau GmbH.** This is an independent, open-source project created to provide Python support for parsing NGB (NETZSCH binary) file formats. NETZSCH is a trademark of NETZSCH-Gerätebau GmbH.

## ✨ Features

### Core Capabilities
- 🚀 **High-Performance Parsing**: Optimized binary parsing with NumPy and PyArrow
- 📊 **Rich Metadata Extraction**: Complete instrument settings, sample information, and measurement parameters
- 🧮 **Baseline Subtraction**: Automatic baseline correction with temperature program validation
- 🔧 **Flexible Data Access**: Multiple APIs for different use cases
- 📦 **Modern Data Formats**: PyArrow tables with embedded metadata
- 🔍 **Data Validation**: Built-in quality checking and validation tools
- ⚡ **Batch Processing**: Parallel processing of multiple files
- 🛠️ **Command Line Interface**: Production-ready CLI for automation

### Advanced Features
- 🏗️ **Modular Architecture**: Extensible and maintainable design
- 🔒 **Type Safety**: Full type hints and static analysis support
- 🧪 **Comprehensive Testing**: 300+ tests including integration and stress tests
- 🔄 **Format Conversion**: Export to Parquet, CSV, and JSON
- 📈 **Dataset Management**: Tools for managing collections of NGB files
- 🔀 **Concurrent Processing**: Thread-safe operations and parallel execution
- 📝 **Rich Documentation**: Complete API documentation with examples

## 🚀 Quick Start

### Installation

```bash
pip install pyngb
```

### Basic Usage

```python
from pyngb import read_ngb

# Quick data loading (recommended for most users)
data = read_ngb("sample.ngb-ss3")
print(f"Loaded {data.num_rows} rows with {data.num_columns} columns")
print(f"Columns: {data.column_names}")

# Access embedded metadata
import json
metadata = json.loads(data.schema.metadata[b'file_metadata'])
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
print(f"Instrument: {metadata.get('instrument', 'Unknown')}")

# Separate metadata and data (for advanced analysis)
metadata, data = read_ngb("sample.ngb-ss3", return_metadata=True)
```

### Data Analysis

```python
import polars as pl

# Convert to DataFrame for analysis
df = pl.from_arrow(table)

# Basic exploration
print(df.describe())
print(f"Temperature range: {df['sample_temperature'].min():.1f} to {df['sample_temperature'].max():.1f} °C")

# Simple plotting
import matplotlib.pyplot as plt

plt.figure(figsize=(12, 8))
plt.subplot(2, 1, 1)
plt.plot(df['time'], df['sample_temperature'])
plt.ylabel('Temperature (°C)')
plt.title('Temperature Program')

plt.subplot(2, 1, 2)
plt.plot(df['time'], df['mass'])
plt.xlabel('Time (s)')
plt.ylabel('Mass (mg)')
plt.title('Mass Loss')
plt.show()
```

### Baseline Subtraction

```python
from pyngb import read_ngb, subtract_baseline

# Method 1: Integrated baseline subtraction (recommended)
corrected_data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)

# Method 2: Standalone baseline subtraction
corrected_df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")

# Advanced: Custom dynamic axis (default is sample_temperature)
corrected_data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3",
    dynamic_axis="time"  # or "furnace_temperature"
)
```

## 📋 Complete Usage Guide

### 1. Single File Processing

```python
from pyngb import read_ngb

# Method 1: Unified data and metadata (recommended)
table = read_ngb("experiment.ngb-ss3")
# Access data
df = pl.from_arrow(table)
# Access metadata
metadata = json.loads(table.schema.metadata[b'file_metadata'])

# Method 2: Separate metadata and data
metadata, data = read_ngb("experiment.ngb-ss3", return_metadata=True)
```

### 2. Batch Processing

```python
from pyngb import BatchProcessor

# Initialize batch processor
processor = BatchProcessor(max_workers=4, verbose=True)

# Process multiple files
results = processor.process_files(
    ["file1.ngb-ss3", "file2.ngb-ss3", "file3.ngb-ss3"],
    output_format="both",  # Parquet and CSV
    output_dir="./processed_data/"
)

# Check results
successful = [r for r in results if r["status"] == "success"]
print(f"Successfully processed {len(successful)} files")
```

### 3. Dataset Management

```python
from pyngb import NGBDataset

# Create dataset from directory
dataset = NGBDataset.from_directory("./sta_experiments/")

# Get overview
summary = dataset.summary()
print(f"Dataset contains {summary['file_count']} files")
print(f"Unique instruments: {summary['unique_instruments']}")

# Export metadata for analysis
dataset.export_metadata("dataset_metadata.csv", format="csv")

# Filter dataset
high_temp_files = dataset.filter_by_metadata(
    lambda meta: meta.get('sample_mass', 0) > 10.0
)
```

### 4. Data Validation

```python
from pyngb.validation import QualityChecker, validate_sta_data

# Quick validation
issues = validate_sta_data(df)
print(f"Found {len(issues)} data quality issues")

# Comprehensive validation
checker = QualityChecker(df)
result = checker.full_validation()

print(f"Validation passed: {result.is_valid}")
print(f"Errors: {result.summary()['error_count']}")
print(f"Warnings: {result.summary()['warning_count']}")
```

### 5. Advanced Parser Configuration

```python
from pyngb import NGBParser, PatternConfig

# Custom configuration
config = PatternConfig()
config.column_map["custom_id"] = "custom_column"
config.metadata_patterns["custom_field"] = (b"\x99\x99", b"\x88\x88")

# Use custom parser
parser = NGBParser(config)
metadata, data = parser.parse("sample.ngb-ss3")
```

## 🖥️ Command Line Interface

### Basic Commands

```bash
# Convert single file to Parquet
python -m pyngb sample.ngb-ss3

# Convert to CSV with verbose output
python -m pyngb sample.ngb-ss3 -f csv -v

# Convert to all formats (Parquet, CSV)
python -m pyngb sample.ngb-ss3 -f all -o ./output/
```

### Baseline Subtraction via CLI

```bash
# Basic baseline subtraction
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3

# Baseline subtraction with custom dynamic axis
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time

# Baseline subtraction with all output formats
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -f all -o ./corrected/
```

### Batch Processing

```bash
# Process all files in directory
python -m pyngb *.ngb-ss3 -f parquet -o ./processed/

# Process with specific output formats
python -m pyngb experiments/*.ngb-ss3 -f both -o ./results/

# Get help
python -m pyngb --help
```

### Advanced CLI Usage

```bash
# Process directory with pattern matching
find ./data -name "*.ngb-ss3" | xargs python -m pyngb -f parquet -o ./output/

# Automated processing pipeline
python -m pyngb $(find ./incoming -name "*.ngb-ss3" -mtime -1) -f all -o ./daily_processing/
```

## 🏗️ Architecture

pyngb uses a modular, extensible architecture designed for performance and maintainability:

```
pyngb/
├── api/                    # High-level user interface
│   └── loaders.py         # Main loading functions
├── binary/                # Low-level binary parsing
│   ├── parser.py          # Binary structure parsing
│   └── handlers.py        # Data type handlers
├── core/                  # Core orchestration
│   └── parser.py          # Main parser coordination
├── extractors/            # Data extraction modules
│   ├── metadata.py        # Metadata extraction
│   └── streams.py         # Data stream processing
├── batch.py               # Batch processing tools
├── validation.py          # Data quality validation
├── constants.py           # Configuration and constants
├── exceptions.py          # Custom exception hierarchy
└── util.py               # Utility functions
```

### Design Principles

- **Performance First**: Optimized for speed and memory efficiency
- **Extensibility**: Easy to add new data types and extraction patterns
- **Reliability**: Comprehensive error handling and validation
- **Usability**: Multiple APIs for different user needs
- **Maintainability**: Clean separation of concerns and thorough testing

## 📊 Data Output

### Supported Columns

Common data columns extracted from NGB files:

| Column | Description | Units |
|--------|-------------|--------|
| `time` | Measurement time | seconds |
| `sample_temperature` | Sample temperature | °C |
| `furnace_temperature` | Furnace temperature | °C |
| `mass` | Sample mass | mg |
| `dsc_signal` | DSC heat flow | µV/mg |
| `purge_flow_1` | Primary purge gas flow | mL/min |
| `purge_flow_2` | Secondary purge gas flow | mL/min |
| `protective_flow` | Protective gas flow | mL/min |

### Metadata Fields

Comprehensive metadata extraction including:

- **Instrument Information**: Model, version, calibration data
- **Sample Details**: Name, mass, material, crucible type
- **Experimental Conditions**: Operator, date, lab, project
- **Temperature Program**: Complete heating/cooling profiles
- **Gas Flows**: MFC settings and gas types
- **System Parameters**: PID settings, acquisition rates

## 🔧 Advanced Features

### Performance Optimization

```python
# Memory-efficient processing of large files
table = read_ngb("large_file.ngb-ss3")
# Process in chunks to manage memory
chunk_size = 10000
for i in range(0, table.num_rows, chunk_size):
    chunk = table.slice(i, chunk_size)
    # Process chunk...
```

### Custom Data Types

```python
from pyngb.binary.handlers import DataTypeHandler, DataTypeRegistry

class CustomHandler(DataTypeHandler):
    def can_handle(self, data_type: bytes) -> bool:
        return data_type == b'\x99'

    def parse(self, data: bytes) -> list:
        # Custom parsing logic
        return [struct.unpack('<f', data[i:i+4])[0] for i in range(0, len(data), 4)]

# Register custom handler
registry = DataTypeRegistry()
registry.register(CustomHandler())
```

### Validation Customization

```python
from pyngb.validation import QualityChecker

class CustomQualityChecker(QualityChecker):
    def custom_check(self):
        """Add custom validation logic."""
        if "custom_column" in self.data.columns:
            values = self.data["custom_column"]
            if values.min() < 0:
                self.result.add_error("Custom column has negative values")
```

## 🧪 Testing and Quality

pyngb includes a comprehensive test suite ensuring reliability:

- **300+ Tests**: Unit, integration, and end-to-end tests
- **Real Data Testing**: Tests using actual NGB files
- **Stress Testing**: Memory management and concurrent processing
- **Edge Case Coverage**: Corrupted files, extreme data values
- **Performance Testing**: Large file processing benchmarks

Run tests locally:

```bash
# Install development dependencies
uv sync --extra dev

# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run only fast tests
pytest -m "not slow"
```

## 🤝 Contributing

We welcome contributions! Here's how to get started:

### Development Setup

```bash
# Clone repository
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb

# Install with development dependencies
uv sync --extra dev

# Install pre-commit hooks
pre-commit install

# Run tests
pytest
```

### Contributing Guidelines

1. **Fork the repository** and create a feature branch
2. **Write tests** for new functionality
3. **Follow code style** (ruff + mypy)
4. **Update documentation** for new features
5. **Submit a pull request** with clear description

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## 📚 Documentation

- **[Complete Documentation](https://graysonbellamy.github.io/pyngb/)**: Full user guide and API reference
- **[Quick Start Guide](docs/quickstart.md)**: Get up and running quickly
- **[API Reference](docs/api.md)**: Detailed function documentation
- **[Development Guide](docs/development.md)**: Contributing and development setup
- **[Troubleshooting](docs/troubleshooting.md)**: Common issues and solutions

## 🚀 Performance

pyngb is optimized for performance:

- **Fast Parsing**: Typical files parse in 0.1-2 seconds
- **Memory Efficient**: Uses PyArrow for optimal memory usage
- **Parallel Processing**: Multi-core batch processing
- **Scalable**: Handles files from KB to GB sizes

### Benchmarks

| Operation | Performance |
|-----------|-------------|
| Parse 10MB file | ~0.5 seconds |
| Extract metadata | ~0.1 seconds |
| Batch process 100 files | ~30 seconds (4 cores) |
| Memory usage | ~2x file size |

## 🔗 Integration

pyngb integrates well with the scientific Python ecosystem:

```python
# With Pandas
import pandas as pd
df_pandas = pl.from_arrow(table).to_pandas()

# With NumPy
import numpy as np
temperature_array = table['sample_temperature'].to_numpy()

# With Matplotlib/Seaborn
import matplotlib.pyplot as plt
import seaborn as sns

# With Jupyter notebooks
from IPython.display import display
display(df.head())
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## 🙏 Acknowledgments

- NETZSCH-Gerätebau GmbH for creating the STA instruments (no affiliation)
- The PyArrow and Polars teams for excellent data processing libraries
- The scientific Python community for the foundational tools

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/GraysonBellamy/pyngb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/GraysonBellamy/pyngb/discussions)
- **Documentation**: [Full Documentation](https://graysonbellamy.github.io/pyngb/)

---

Made with ❤️ for the scientific community
