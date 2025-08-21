# Baseline Subtraction Example

This example demonstrates the new baseline subtraction feature in pyngb.

## Overview

The baseline subtraction feature allows you to automatically subtract baseline measurements from sample data. This is particularly useful for:

- Correcting for instrumental drift
- Removing systematic measurement artifacts
- Improving signal-to-noise ratio
- Standardizing measurements across different runs

## Key Features

- **Automatic segment detection**: Identifies isothermal vs dynamic segments based on temperature program
- **Flexible axis selection**: Choose alignment axis for dynamic segments (time, sample temperature, or furnace temperature)
- **Selective subtraction**: Only subtracts `mass` and `dsc_signal` columns, preserves time and flow data from sample
- **Integrated API**: Works seamlessly with existing `read_ngb()` function

## Basic Usage

### Method 1: Using the standalone function

```python
from pyngb import subtract_baseline
import polars as pl

# Perform baseline subtraction
df = subtract_baseline(
    sample_file="sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)

print(f"Result shape: {df.shape}")
```

### Method 2: Using the integrated API

```python
from pyngb import read_ngb
import polars as pl

# Load data with automatic baseline subtraction
data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
)

df = pl.from_arrow(data)
print(f"Result shape: {df.shape}")
```

## Advanced Usage

### Choosing alignment axis for dynamic segments

```python
from pyngb import read_ngb
import polars as pl

# Default: Use sample temperature for dynamic segment alignment
data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3"
    # dynamic_axis="sample_temperature" is the default
)

# Alternative: Use time axis
data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3",
    dynamic_axis="time"  # Options: "time", "sample_temperature", "furnace_temperature"
)

df = pl.from_arrow(data)
```

### Getting metadata along with subtracted data

```python
from pyngb import read_ngb
import polars as pl

# Get both metadata and subtracted data
metadata, data = read_ngb(
    "sample.ngb-ss3",
    baseline_file="baseline.ngb-bs3",
    return_metadata=True
)

df = pl.from_arrow(data)
print(f"Sample name: {metadata.get('sample_name', 'Unknown')}")
print(f"Temperature program: {metadata.get('temperature_program', {})}")
```

### Method 3: Using the Command Line Interface

```bash
# Basic baseline subtraction (uses sample_temperature axis by default)
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3

# Use time axis for dynamic segments
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time

# Use furnace temperature axis for dynamic segments
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis furnace_temperature

# Output all formats with verbose logging
python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -f all -v -o ./results/

# Multiple files with baseline subtraction
for file in *.ngb-ss3; do
    baseline="${file%.ngb-ss3}_baseline.ngb-bs3"
    if [ -f "$baseline" ]; then
        python -m pyngb "$file" -b "$baseline" -o ./corrected/
    fi
done
```

The CLI automatically adds "_baseline_subtracted" to output filenames to indicate processing was performed.

## How it Works

1. **Temperature Program Analysis**: The function reads the temperature program from the sample file metadata to identify heating segments.

2. **Segment Classification**:
   - **Isothermal segments**: Heating rate ≈ 0°C/min
   - **Dynamic segments**: Heating rate > 0°C/min

3. **Alignment Strategy**:
   - **Isothermal**: Always aligned on time axis
   - **Dynamic**: User-selectable axis (time, sample temperature, or furnace temperature)

4. **Interpolation**: Baseline data is interpolated to match sample data points using linear interpolation.

5. **Selective Subtraction**: Only `mass` and `dsc_signal` columns are subtracted. All other columns (time, temperatures, flows) are preserved from the sample file.

6. **Recombination**: Processed segments are recombined into a single output DataFrame.

## Column Behavior

| Column Type | Behavior |
|-------------|----------|
| `mass` | Subtracted (sample - baseline) |
| `dsc_signal` | Subtracted (sample - baseline) |
| `time` | Preserved from sample |
| `sample_temperature` | Preserved from sample |
| `furnace_temperature` | Preserved from sample |
| Flow columns | Preserved from sample |
| Other columns | Preserved from sample |

## File Requirements

- **Sample file**: Must have `.ngb-ss3` extension
- **Baseline file**: Must have `.ngb-bs3` extension
- **Temperature programs**: Must be identical between sample and baseline files
- Both files should have compatible measurement channels
- Temperature program metadata should be available in both files

## Temperature Program Validation

Before performing baseline subtraction, the function automatically validates that the temperature programs are identical between the sample and baseline files. This ensures that:

- Both files follow the same heating profile
- Segments can be properly aligned
- The subtraction is scientifically meaningful

### Validation Criteria

The validation checks that each stage in the temperature program has identical:
- `temperature` (target temperature)
- `heating_rate` (°C/min)
- `time` (duration in minutes)

### Example of Compatible Programs

```json
// Sample file temperature program
{
  "stage_0": {"temperature": 26.0, "heating_rate": 0.0, "time": 0.0},
  "stage_1": {"temperature": 26.0, "heating_rate": 0.0, "time": 30.0},
  "stage_2": {"temperature": 825.0, "heating_rate": 10.0, "time": 79.9},
  "stage_3": {"temperature": 825.0, "heating_rate": 0.0, "time": 30.0}
}

// Baseline file temperature program (identical)
{
  "stage_0": {"temperature": 26.0, "heating_rate": 0.0, "time": 0.0},
  "stage_1": {"temperature": 26.0, "heating_rate": 0.0, "time": 30.0},
  "stage_2": {"temperature": 825.0, "heating_rate": 10.0, "time": 79.9},
  "stage_3": {"temperature": 825.0, "heating_rate": 0.0, "time": 30.0}
}
```

## Example Temperature Program

The function automatically detects segments like this:

```json
{
  "stage_0": {"temperature": 25.0, "heating_rate": 0.0, "time": 5.0},    // Isothermal
  "stage_1": {"temperature": 700.0, "heating_rate": 10.0, "time": 67.5}, // Dynamic
  "stage_2": {"temperature": 700.0, "heating_rate": 0.0, "time": 15.0},  // Isothermal
  "stage_3": {"temperature": 25.0, "heating_rate": -10.0, "time": 67.5}  // Dynamic
}
```

## Error Handling

The function provides informative error messages for common issues:

```python
try:
    df = subtract_baseline("sample.ngb-ss3", "baseline.ngb-bs3")
except FileNotFoundError:
    print("One of the files was not found")
except ValueError as e:
    if "Temperature program mismatch" in str(e):
        print(f"Incompatible temperature programs: {e}")
    else:
        print(f"Invalid parameter: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Common Validation Errors

- **Temperature program mismatch**: The sample and baseline files have different heating profiles
- **Missing temperature program**: One or both files lack temperature program metadata
- **Stage count mismatch**: Different number of heating stages between files
- **Parameter differences**: Individual stage parameters don't match within tolerance

## Performance Notes

- Processing time depends on file size and number of segments
- Memory usage scales with data size
- Interpolation is optimized using NumPy
- Large files may benefit from chunked processing in future versions
