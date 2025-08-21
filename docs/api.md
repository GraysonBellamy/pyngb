# API Reference

This section provides comprehensive documentation of pyngb's API, including all functions, classes, and modules.

## Core Functions

### Data Loading

::: pyngb.read_ngb
    options:
      heading_level: 3
      show_source: true

### Usage Examples

```python
# Basic data loading
from pyngb import read_ngb

# Method 1: Load as PyArrow table with embedded metadata (recommended)
table = read_ngb("sample.ngb-ss3")
print(f"Shape: {table.num_rows} x {table.num_columns}")

# Method 2: Get separate metadata and data
metadata, data = read_ngb("sample.ngb-ss3", return_metadata=True)
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
```

### Baseline Subtraction

::: pyngb.subtract_baseline
    options:
      heading_level: 3
      show_source: true

::: pyngb.BaselineSubtractor
    options:
      heading_level: 3
      show_source: true

#### Usage Examples

```python
# Standalone baseline subtraction
from pyngb import subtract_baseline

# Default behavior (sample_temperature axis for dynamic segments)
corrected_df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")

# Custom axis selection
corrected_df = subtract_baseline(
    "sample.ngb-ss3",
    "baseline.ngb-bs3",
    dynamic_axis="time"
)

# Integrated approach
from pyngb import read_ngb

corrected_data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)
```

## Batch Processing

### BatchProcessor Class

::: pyngb.BatchProcessor
    options:
      heading_level: 3
      show_source: true
      members:
        - __init__
        - process_directory
        - process_files

### NGBDataset Class

::: pyngb.NGBDataset
    options:
      heading_level: 3
      show_source: true
      members:
        - __init__
        - from_directory
        - summary
        - export_metadata
        - filter_by_metadata

### Convenience Functions

::: pyngb.process_directory
    options:
      heading_level: 3

::: pyngb.process_files
    options:
      heading_level: 3

### Batch Processing Examples

```python
from pyngb import BatchProcessor, NGBDataset, process_directory

# Method 1: Using BatchProcessor class
processor = BatchProcessor(max_workers=4, verbose=True)
results = processor.process_files(
    ["file1.ngb-ss3", "file2.ngb-ss3"],
    output_format="both",
    output_dir="./output/"
)

# Method 2: Using convenience functions
results = process_directory(
    "./data/",
    pattern="*.ngb-ss3",
    output_format="parquet",
    max_workers=2
)

# Method 3: Dataset management
dataset = NGBDataset.from_directory("./experiments/")
summary = dataset.summary()
dataset.export_metadata("metadata.csv")
```

## Data Validation

### Validation Functions

::: pyngb.validate_sta_data
    options:
      heading_level: 3

### QualityChecker Class

::: pyngb.QualityChecker
    options:
      heading_level: 3
      show_source: true
      members:
        - __init__
        - quick_check
        - full_validation

### ValidationResult Class

::: pyngb.ValidationResult
    options:
      heading_level: 3
      members:
        - is_valid
        - has_warnings
        - summary
        - report

### Validation Examples

```python
from pyngb.validation import QualityChecker, validate_sta_data
import polars as pl

# Load data
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Method 1: Quick validation
issues = validate_sta_data(df)
print(f"Found {len(issues)} issues")

# Method 2: Comprehensive validation
checker = QualityChecker(df)
result = checker.full_validation()

print(f"Valid: {result.is_valid}")
print(f"Errors: {result.summary()['error_count']}")
print(f"Warnings: {result.summary()['warning_count']}")

# Get detailed report
print(result.report())
```

## Core Parser Classes

### NGBParser

::: pyngb.NGBParser
    options:
      heading_level: 3
      show_source: true
      members:
        - __init__
        - parse

### Advanced Parser Usage

```python
from pyngb import NGBParser, PatternConfig

# Custom configuration
config = PatternConfig()
config.column_map["custom_id"] = "custom_column"
config.metadata_patterns["custom_field"] = (b"\x99\x99", b"\x88\x88")

# Create parser with custom config
parser = NGBParser(config)
metadata, data = parser.parse("sample.ngb-ss3")
```

## Configuration Classes

### PatternConfig

::: pyngb.PatternConfig
    options:
      heading_level: 3
      show_source: true

### BinaryMarkers

::: pyngb.BinaryMarkers
    options:
      heading_level: 3
      show_source: true

### Configuration Examples

```python
from pyngb.constants import PatternConfig, BinaryMarkers

# Examine default configuration
config = PatternConfig()
print("Column mappings:", config.column_map)
print("Metadata patterns:", list(config.metadata_patterns.keys()))

# Binary markers for advanced use
markers = BinaryMarkers()
print("Start data marker:", markers.START_DATA)
print("End data marker:", markers.END_DATA)
```

## Data Types and Enums

### DataType Enum

::: pyngb.DataType
    options:
      heading_level: 3
      show_source: true

### FileMetadata Type

::: pyngb.FileMetadata
    options:
      heading_level: 3

### Data Type Examples

```python
from pyngb.constants import DataType, FileMetadata

# Data type identifiers
print("Float64 identifier:", DataType.FLOAT64.value)
print("String identifier:", DataType.STRING.value)

# Metadata structure (TypedDict)
metadata_example: FileMetadata = {
    "instrument": "NETZSCH STA 449 F3",
    "sample_name": "Test Sample",
    "sample_mass": 15.5,
    "operator": "Lab Technician"
}
```

## Exception Hierarchy

### Base Exception

::: pyngb.NGBParseError
    options:
      heading_level: 3

### Specific Exceptions

::: pyngb.NGBCorruptedFileError
    options:
      heading_level: 3

::: pyngb.NGBUnsupportedVersionError
    options:
      heading_level: 3

::: pyngb.NGBDataTypeError
    options:
      heading_level: 3

::: pyngb.NGBStreamNotFoundError
    options:
      heading_level: 3

### Exception Handling Examples

```python
from pyngb import read_ngb, NGBParseError, NGBCorruptedFileError

try:
    table = read_ngb("sample.ngb-ss3")
except NGBCorruptedFileError:
    print("File appears to be corrupted")
except NGBParseError as e:
    print(f"Parsing error: {e}")
except FileNotFoundError:
    print("File not found")
```

## Internal Modules

### Binary Parser Module

::: pyngb.binary.parser.BinaryParser
    options:
      heading_level: 3
      show_source: false

### Binary Handlers Module

::: pyngb.binary.handlers.DataTypeRegistry
    options:
      heading_level: 3
      show_source: false

::: pyngb.binary.handlers.Float64Handler
        options:
            heading_level: 3
            show_source: false

::: pyngb.binary.handlers.Float32Handler
        options:
            heading_level: 3
            show_source: false

::: pyngb.binary.handlers.Int32Handler
        options:
            heading_level: 3
            show_source: false

### Metadata Extraction Module

::: pyngb.extractors.metadata.MetadataExtractor
    options:
      heading_level: 3
      show_source: false

### Stream Processing Module

::: pyngb.extractors.streams.DataStreamProcessor
    options:
      heading_level: 3
      show_source: false

## Utility Functions

### File Utilities

::: pyngb.util.get_hash
    options:
      heading_level: 3

::: pyngb.util.set_metadata
    options:
      heading_level: 3

### Utility Examples

```python
from pyngb.util import get_hash, set_metadata
import pyarrow as pa

# Generate file hash
file_hash = get_hash("sample.ngb-ss3")
print(f"File hash: {file_hash}")

# Add metadata to PyArrow table
table = pa.table({"data": [1, 2, 3]})
table_with_meta = set_metadata(
    table,
    tbl_meta={"source": "experiment_1", "version": "1.0"}
)
```

## Advanced Usage Patterns

### Custom Data Type Handlers

```python
from pyngb.binary.handlers import DataTypeHandler, DataTypeRegistry
import struct

class CustomFloatHandler(DataTypeHandler):
    def can_handle(self, data_type: bytes) -> bool:
        return data_type == b'\x99'  # Custom type identifier

    def parse(self, data: bytes) -> list[float]:
        # Parse as 32-bit floats
        return [struct.unpack('<f', data[i:i+4])[0]
                for i in range(0, len(data), 4)]

# Register custom handler
registry = DataTypeRegistry()
registry.register(CustomFloatHandler())
```

### Custom Validation Rules

```python
from pyngb.validation import QualityChecker, ValidationResult

class CustomQualityChecker(QualityChecker):
    def domain_specific_validation(self):
        """Add domain-specific validation rules."""
        result = ValidationResult()

        # Custom rule: Check for reasonable mass loss
        if "mass" in self.data.columns:
            mass_col = self.data["mass"]
            initial_mass = mass_col.max()
            final_mass = mass_col.min()
            mass_loss_percent = (initial_mass - final_mass) / initial_mass * 100

            if mass_loss_percent > 50:
                result.add_warning(f"High mass loss: {mass_loss_percent:.1f}%")
            elif mass_loss_percent < 0:
                result.add_error("Negative mass loss detected")
            else:
                result.add_pass(f"Normal mass loss: {mass_loss_percent:.1f}%")

        return result
```

### Memory-Efficient Processing

```python
from pyngb import read_ngb
import polars as pl

def process_large_file_efficiently(file_path: str, chunk_size: int = 10000):
    """Process large files in chunks to manage memory."""
    table = read_ngb(file_path)

    results = []
    for i in range(0, table.num_rows, chunk_size):
        # Process chunk
        chunk = table.slice(i, min(chunk_size, table.num_rows - i))
        df_chunk = pl.from_arrow(chunk)

        # Perform analysis on chunk
        chunk_result = df_chunk.select([
            pl.col("time").mean().alias("avg_time"),
            pl.col("sample_temperature").mean().alias("avg_temp")
        ])

        results.append(chunk_result)

    # Combine results
    final_result = pl.concat(results)
    return final_result
```

## Performance Considerations

### Best Practices

1. **Use PyArrow Tables**: More memory-efficient than Pandas DataFrames
2. **Batch Processing**: Process multiple files in parallel when possible
3. **Chunk Large Files**: Use slicing for very large datasets
4. **Cache Metadata**: Extract metadata once and reuse
5. **Choose Appropriate Formats**: Parquet for storage, CSV for sharing
6. **Optimize Conversions (v0.0.2+)**: Pass Polars DataFrames directly to validation functions

### Optimized Data Processing (v0.0.2+)

```python
import polars as pl
from pyngb import read_ngb
from pyngb.validation import validate_sta_data, check_temperature_profile

# Efficient workflow with minimal conversions
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)  # Single conversion

# All operations use the DataFrame directly (no additional conversions)
issues = validate_sta_data(df)           # Zero conversion overhead
temp_analysis = check_temperature_profile(df)  # Zero conversion overhead

# Previous approach (pre-v0.0.2) required multiple conversions:
# validate_sta_data(table)  # Internal PyArrow → Polars conversion
# check_temperature_profile(table)  # Another PyArrow → Polars conversion
```

### Memory Management

```python
import gc
from pyngb import read_ngb

def memory_conscious_processing(files: list[str]):
    """Process files with explicit memory management."""
    for file_path in files:
        # Load and process
        table = read_ngb(file_path)

        # Process immediately
        process_table(table)

        # Explicitly delete reference
        del table

        # Force garbage collection periodically
        gc.collect()
```

### Parallel Processing

```python
from concurrent.futures import ProcessPoolExecutor
from pyngb import read_ngb

def parallel_file_processing(files: list[str], max_workers: int = 4):
    """Process files in parallel across multiple processes."""
    def process_single_file(file_path: str):
        table = read_ngb(file_path)
        # Perform processing
        return {"file": file_path, "rows": table.num_rows}

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_file, files))

    return results
```

## Error Handling Patterns

### Robust File Processing

```python
from pyngb import read_ngb, NGBParseError
import logging

def robust_file_processing(files: list[str]):
    """Process files with comprehensive error handling."""
    results = []

    for file_path in files:
        try:
            table = read_ngb(file_path)
            results.append({
                "file": file_path,
                "status": "success",
                "rows": table.num_rows,
                "columns": table.num_columns
            })

        except NGBParseError as e:
            logging.error(f"Parse error in {file_path}: {e}")
            results.append({
                "file": file_path,
                "status": "parse_error",
                "error": str(e)
            })

        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            results.append({
                "file": file_path,
                "status": "not_found"
            })

        except Exception as e:
            logging.error(f"Unexpected error in {file_path}: {e}")
            results.append({
                "file": file_path,
                "status": "error",
                "error": str(e)
            })

    return results
```

## Command Line Interface

pyngb provides a comprehensive CLI for data processing and baseline subtraction:

### Basic Usage

```bash
python -m pyngb input.ngb-ss3 [options]
```

### Arguments

- **`input`**: Path to the input NGB file (required)
- **`-o, --output`**: Output directory (default: current directory)
- **`-f, --format`**: Output format: `parquet`, `csv`, or `all` (default: `parquet`)
- **`-v, --verbose`**: Enable verbose logging
- **`-b, --baseline`**: Path to baseline file for baseline subtraction
- **`--dynamic-axis`**: Axis for dynamic segment alignment: `time`, `sample_temperature`, or `furnace_temperature` (default: `sample_temperature`)

### Examples

```bash
# Basic conversion
python -m pyngb sample.ngb-ss3

# CSV output with verbose logging
python -m pyngb sample.ngb-ss3 -f csv -v

# Baseline subtraction with default settings
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3

# Baseline subtraction with time axis alignment
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time

# All formats with custom output directory
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -f all -o ./results/
```

### Output Files

- Without baseline: `{input_name}.{format}`
- With baseline: `{input_name}_baseline_subtracted.{format}`

---

For more examples and detailed usage patterns, see the [Quick Start Guide](quickstart.md) and [Development Guide](development.md).
