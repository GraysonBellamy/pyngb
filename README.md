# pyNGB (Unofficial NETZSCH NGB Parser)

An unofficial Python library for parsing NETZSCH STA (Simultaneous Thermal Analysis) NGB (NETZSCH binary) files produced by NETZSCH thermal analysis instruments.

## Disclaimer

**This package and its author are not affiliated with, endorsed by, or approved by NETZSCH-Gerätebau GmbH.** This is an independent, open-source project created to provide Python support for parsing NGB (NETZSCH binary) file formats. NETZSCH is a trademark of NETZSCH-Gerätebau GmbH.

## Installation

```bash
pip install pyngb
```

## Quick Start

```python
from pyngb import load_ngb_data

# Load NGB file
table = load_ngb_data("sample.ngb-ss3")
print(f"Columns: {table.column_names}")
print(f"Rows: {table.num_rows}")

# Access embedded metadata
import json
metadata = json.loads(table.schema.metadata[b'file_metadata'])
print(f"Instrument: {metadata.get('instrument', 'Unknown')}")
```

## Features

- Fast binary parsing with NumPy optimization
- Comprehensive metadata extraction
- PyArrow table output with embedded metadata
- Modular, extensible architecture
- Command-line interface
- Type-safe API with full documentation

## Architecture

This library uses a modular architecture for maintainability and extensibility:

- **binary/**: Low-level binary parsing and data type handlers
- **extractors/**: Metadata and stream data extraction
- **core/**: Main parser orchestration
- **api/**: High-level public interface
- **constants.py**: Configuration and data types
- **exceptions.py**: Custom exception hierarchy

## Advanced Usage

```python
from pyngb import get_sta_data, NGBParser, PatternConfig

# Get separate metadata and data objects
metadata, data = get_sta_data("sample.ngb-ss3")

# Custom configuration
config = PatternConfig()
config.column_map["custom_id"] = "custom_column"
parser = NGBParser(config)
```

## Command Line Interface

```bash
# Convert to Parquet (default)
python -m pyngb sample.ngb-ss3

# Convert to CSV with verbose logging
python -m pyngb sample.ngb-ss3 -f csv -v

# Convert to both formats in custom directory
python -m pyngb sample.ngb-ss3 -f all -o /output/dir
```

## Development

```bash
# Clone repository
git clone https://github.com/GraysonBellamy/pyngb.git
cd pyngb

# Install development dependencies
uv sync

# Run tests
pytest

# Build package
uv build
```

## License

MIT - see [LICENSE.txt](LICENSE.txt) for details.
