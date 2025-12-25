# pyNGB - NETZSCH STA File Parser

[![PyPI version](https://badge.fury.io/py/pyngb.svg)](https://badge.fury.io/py/pyngb)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/GraysonBellamy/pyngb/workflows/Tests/badge.svg)](https://github.com/GraysonBellamy/pyngb/actions)

A high-performance Python library for parsing NETZSCH STA (Simultaneous Thermal Analysis) NGB files with comprehensive metadata extraction and analysis tools.

**⚠️ Disclaimer**: This package is not affiliated with NETZSCH-Gerätebau GmbH. NETZSCH is a trademark of NETZSCH-Gerätebau GmbH.

## Features

- **Fast Binary Parsing**: Optimized parsing with NumPy and PyArrow (0.1-1 sec/file)
- **Complete Metadata**: Extract instrument settings, sample info, and experimental conditions
- **Baseline Correction**: Automatic baseline subtraction with validation
- **DTG Analysis**: Derivative thermogravimetry calculation with smoothing options
- **Batch Processing**: Parallel processing of multiple files
- **Data Export**: Convert to Parquet, CSV, and JSON formats
- **CLI Support**: Command-line interface for automation

## Installation

```bash
pip install pyngb
```

## Quick Example

```python
from pyngb import read_ngb
import polars as pl
import json

# Load NGB file with embedded metadata
table = read_ngb("experiment.ngb-ss3")
df = pl.from_arrow(table)

# Access data
print(f"Loaded {df.height} data points")
print(f"Temperature range: {df['sample_temperature'].min():.1f} to {df['sample_temperature'].max():.1f} °C")

# Access metadata
metadata = json.loads(table.schema.metadata[b'file_metadata'])
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")

# Baseline correction
corrected = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")

# DTG analysis
from pyngb.api.analysis import add_dtg
table_with_dtg = add_dtg(table, method="savgol", smooth="medium")
```

## Command Line Usage

```bash
# Convert single file
python -m pyngb sample.ngb-ss3

# Batch processing with baseline correction
python -m pyngb *.ngb-ss3 -b baseline.ngb-bs3 -f parquet -o ./processed/

# Get help
python -m pyngb --help
```

## Documentation

- **[Getting Started](docs/getting-started.md)** - Installation and first steps
- **[User Guide](docs/user-guide.md)** - Core functionality and examples
- **[API Reference](docs/api-reference.md)** - Complete function documentation
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions
- **[Contributing](docs/contributing.md)** - Development guidelines

## License

MIT License. See [LICENSE.txt](LICENSE.txt) for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/GraysonBellamy/pyngb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/GraysonBellamy/pyngb/discussions)

---

Made with ❤️ for the scientific community
