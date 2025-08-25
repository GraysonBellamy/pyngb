# API Reference

Complete reference for all pyngb functions and classes.

## Core Functions

### read_ngb()

Main function for loading NGB files with optional baseline subtraction.

```python
def read_ngb(
    path: str,
    *,
    return_metadata: bool = False,
    baseline_file: str | None = None,
    dynamic_axis: str = "sample_temperature",
) -> Union[pa.Table, tuple[FileMetadata, pa.Table]]
```

**Parameters:**
- `path` (str): Path to NGB file (.ngb-ss3 or similar)
- `return_metadata` (bool, default False): If True, return (metadata, table) tuple
- `baseline_file` (str, optional): Path to baseline file for subtraction
- `dynamic_axis` (str, default "sample_temperature"): Axis for dynamic segment alignment

**Returns:**
- `pa.Table`: PyArrow table with embedded metadata (default)
- `tuple[dict, pa.Table]`: Metadata and table tuple (if return_metadata=True)

**Examples:**
```python
# Basic loading
table = read_ngb("sample.ngb-ss3")

# With metadata separated
metadata, table = read_ngb("sample.ngb-ss3", return_metadata=True)

# With baseline subtraction
corrected = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")
```

### subtract_baseline()

Standalone baseline subtraction function.

```python
def subtract_baseline(
    sample_file: str,
    baseline_file: str,
    dynamic_axis: str = "sample_temperature"
) -> pl.DataFrame
```

**Parameters:**
- `sample_file` (str): Path to sample NGB file
- `baseline_file` (str): Path to baseline NGB file
- `dynamic_axis` (str): Alignment axis for dynamic segments

**Returns:**
- `pl.DataFrame`: Baseline-corrected data

## Analysis Functions

### add_dtg()

Add DTG column to PyArrow table.

```python
def add_dtg(
    table: pa.Table,
    method: str = "savgol",
    smooth: str = "medium",
    column_name: str = "dtg",
) -> pa.Table
```

**Parameters:**
- `table` (pa.Table): Table with 'time' and 'mass' columns
- `method` (str): "savgol" or "gradient"
- `smooth` (str): "strict", "medium", or "loose"
- `column_name` (str): Name for DTG column

**Returns:**
- `pa.Table`: Table with added DTG column

### dtg()

Calculate derivative thermogravimetry values.

```python
def dtg(
    time: np.ndarray,
    mass: np.ndarray,
    method: str = "savgol",
    smooth: str = "medium",
) -> np.ndarray
```

**Parameters:**
- `time` (np.ndarray): Time values
- `mass` (np.ndarray): Mass values
- `method` (str): Calculation method
- `smooth` (str): Smoothing level

**Returns:**
- `np.ndarray`: DTG values in mg/min

**Smoothing Options:**
- `"strict"`: Minimal smoothing, preserves all features
- `"medium"`: Balanced smoothing (recommended)
- `"loose"`: Heavy smoothing for noisy data

### normalize_to_initial_mass()

Normalize data columns to initial sample mass.

```python
def normalize_to_initial_mass(
    table: pa.Table,
    columns: list[str] = ["mass", "dsc_signal"],
) -> pa.Table
```

**Parameters:**
- `table` (pa.Table): Table with embedded metadata
- `columns` (list[str]): Columns to normalize

**Returns:**
- `pa.Table`: Table with normalized columns added

## Batch Processing

### BatchProcessor

High-performance batch processing for multiple files.

```python
class BatchProcessor:
    def __init__(self, max_workers: int | None = None, verbose: bool = True)
```

**Parameters:**
- `max_workers` (int, optional): Number of parallel processes (default: CPU count)
- `verbose` (bool): Show progress information

#### process_files()

Process multiple NGB files in parallel.

```python
def process_files(
    self,
    file_paths: list[str],
    output_format: str = "parquet",
    output_dir: str | None = None,
    skip_errors: bool = True,
) -> list[dict]
```

**Parameters:**
- `file_paths` (list[str]): List of file paths to process
- `output_format` (str): "parquet", "csv", or "both"
- `output_dir` (str, optional): Output directory
- `skip_errors` (bool): Continue processing if errors occur

**Returns:**
- `list[dict]`: Processing results with status and metadata

#### process_directory()

Process all NGB files in a directory.

```python
def process_directory(
    self,
    directory: str,
    pattern: str = "*.ngb-ss3",
    output_format: str = "parquet",
    output_dir: str | None = None,
    skip_errors: bool = True,
) -> list[dict]
```

**Parameters:**
- `directory` (str): Directory containing NGB files
- `pattern` (str): File pattern to match
- `output_format` (str): Output format
- `output_dir` (str, optional): Output directory
- `skip_errors` (bool): Error handling mode

## Data Structures

### FileMetadata

Dictionary containing extracted metadata from NGB files.

**Common Fields:**
- `sample_name` (str): Sample identifier
- `sample_mass` (float): Initial sample mass in mg
- `instrument` (str): Instrument model/name
- `operator` (str): Operator name
- `measurement_date` (str): Date of measurement
- `temperature_program` (dict): Complete temperature program
- `crucible_type` (str): Crucible type used
- `atmosphere` (str): Measurement atmosphere
- `heating_rate` (float): Primary heating rate
- `max_temperature` (float): Maximum temperature reached

**Temperature Program Structure:**
```python
{
    "stage_0": {"temperature": 25.0, "heating_rate": 0.0, "time": 5.0},
    "stage_1": {"temperature": 700.0, "heating_rate": 10.0, "time": 67.5},
    "stage_2": {"temperature": 700.0, "heating_rate": 0.0, "time": 15.0}
}
```

### Column Names

Standard column names in processed data:

**Time and Temperature:**
- `time`: Measurement time (seconds)
- `sample_temperature`: Sample temperature (°C)
- `furnace_temperature`: Furnace temperature (°C)

**Mass and Signals:**
- `mass`: Sample mass (mg)
- `dsc_signal`: DSC heat flow (µV or mW)
- `dtg`: Derivative thermogravimetry (mg/min) - when calculated

**Gas Flows:**
- `purge_flow_1`: Primary purge gas flow (mL/min)
- `purge_flow_2`: Secondary purge gas flow (mL/min)
- `protective_flow`: Protective gas flow (mL/min)

**Normalized Columns:** (when using normalize_to_initial_mass)
- `mass_normalized`: Mass as fraction of initial mass
- `dsc_signal_normalized`: DSC signal per unit initial mass

## Exception Classes

### NGBParsingError

Raised when NGB file parsing fails.

```python
class NGBParsingError(Exception):
    """Raised when parsing NGB file structure fails."""
```

### ValidationError

Raised when data validation fails.

```python
class ValidationError(Exception):
    """Raised when data validation fails."""
```

### BaselineError

Raised when baseline subtraction fails.

```python
class BaselineError(Exception):
    """Raised when baseline subtraction cannot be performed."""
```

## Constants

### File Extensions

- `.ngb-ss3`: Sample files (main data)
- `.ngb-bs3`: Baseline files (for correction)

### Default Parameters

**DTG Calculation:**
- Default method: `"savgol"`
- Default smoothing: `"medium"`
- Savitzky-Golay parameters by smoothing level:
  - strict: window=7, polyorder=1
  - medium: window=25, polyorder=2
  - loose: window=51, polyorder=3

**Baseline Subtraction:**
- Default dynamic axis: `"sample_temperature"`
- Supported axes: `"time"`, `"sample_temperature"`, `"furnace_temperature"`

**Batch Processing:**
- Default output format: `"parquet"`
- Default workers: CPU count
- Default compression: `"snappy"`

## Usage Examples

### Complete Analysis Workflow

```python
from pyngb import read_ngb, BatchProcessor
from pyngb.api.analysis import add_dtg, normalize_to_initial_mass
import polars as pl
import json

# Single file analysis
table = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")
table = normalize_to_initial_mass(table)
table = add_dtg(table, smooth="medium")

# Convert to DataFrame
df = pl.from_arrow(table)

# Access metadata
metadata = json.loads(table.schema.metadata[b'file_metadata'])
print(f"Sample: {metadata['sample_name']}")
print(f"Mass loss: {(1 - df['mass_normalized'].min()) * 100:.1f}%")

# Batch processing
processor = BatchProcessor(max_workers=4)
results = processor.process_directory("./data/", output_format="parquet")
```

### Error Handling

```python
from pyngb.exceptions import NGBParsingError, ValidationError

try:
    table = read_ngb("sample.ngb-ss3")
except NGBParsingError as e:
    print(f"File parsing failed: {e}")
except FileNotFoundError:
    print("File not found")
except ValidationError as e:
    print(f"Data validation failed: {e}")
```

### Custom Analysis

```python
import numpy as np
from scipy.signal import find_peaks

# Extract data
df = pl.from_arrow(table)
temperature = df['sample_temperature'].to_numpy()
dtg_values = df['dtg'].to_numpy()

# Find decomposition peaks
peaks, _ = find_peaks(-dtg_values, height=0.01)
peak_temps = temperature[peaks]

print(f"Decomposition peaks at: {peak_temps} °C")
```

This reference covers all public functions and classes in pyngb. For implementation details and advanced customization, see the source code documentation.
