# Column Metadata System

pyngb provides a comprehensive column-level metadata system that tracks units, processing history, baseline correction status, and data source for all columns in your thermal analysis data. This enables complete provenance tracking through complex analysis workflows.

## Overview

The column metadata system automatically:

- **Assigns default metadata** to known column types during data loading
- **Preserves metadata** through analysis operations (DTG, normalization)
- **Updates processing history** as operations are applied
- **Tracks baseline correction** status for applicable columns
- **Stores natively** in PyArrow/Parquet format for interoperability

## Metadata Fields

Each column can have four metadata fields:

| Field | Type | Description | Applies To |
|-------|------|-------------|------------|
| `units` | `str` | Physical units (e.g., "mg", "µV", "°C") | All columns |
| `processing_history` | `list[str]` | Steps applied (e.g., ["raw", "normalized"]) | All columns |
| `source` | `str` | Data origin ("measurement", "calculated", "derived") | All columns |
| `baseline_subtracted` | `bool` | Baseline correction status | Only `mass` and `dsc_signal` |

## Default Column Metadata

pyngb automatically assigns sensible defaults based on column names:

```python
from pyngb import read_ngb
from pyngb.api.metadata import inspect_column_metadata

# Load data - automatic metadata initialization
table = read_ngb("sample.ngb-ss3")

# Common column defaults
print(inspect_column_metadata(table, 'time'))
# {'units': 'min', 'processing_history': ['raw'], 'source': 'measurement'}

print(inspect_column_metadata(table, 'mass'))
# {'units': 'mg', 'processing_history': ['raw'], 'source': 'measurement', 'baseline_subtracted': False}

print(inspect_column_metadata(table, 'dsc_signal'))
# {'units': 'µV', 'processing_history': ['raw'], 'source': 'measurement', 'baseline_subtracted': False}
```

## Basic Metadata Operations

### Getting Metadata

```python
from pyngb.api.metadata import (
    get_column_units,
    get_processing_history,
    get_column_baseline_status,
    inspect_column_metadata
)

# Get specific metadata fields
units = get_column_units(table, 'mass')          # "mg"
history = get_processing_history(table, 'mass')  # ["raw"]
baseline = get_column_baseline_status(table, 'mass')  # False

# Get all metadata for a column
all_metadata = inspect_column_metadata(table, 'mass')
print(all_metadata)
```

### Setting Metadata

```python
from pyngb.api.metadata import (
    set_column_units,
    add_column_processing_step,
    mark_baseline_corrected
)

# Change units
table = set_column_units(table, 'mass', 'g')

# Add processing step
table = add_column_processing_step(table, 'mass', 'filtered')

# Mark as baseline corrected (only for mass/DSC columns)
table = mark_baseline_corrected(table, ['mass', 'dsc_signal'])
```

## Automatic Metadata Updates

### DTG Analysis

When you add DTG analysis, metadata is automatically handled:

```python
from pyngb.api.analysis import add_dtg

# Original mass column metadata preserved
# New DTG column gets appropriate metadata
table_with_dtg = add_dtg(table)

dtg_metadata = inspect_column_metadata(table_with_dtg, 'dtg')
print(dtg_metadata)
# {'units': 'mg/min', 'processing_history': ['calculated'], 'source': 'derived'}

# Original columns retain their metadata
mass_metadata = inspect_column_metadata(table_with_dtg, 'mass')
print(mass_metadata)  # Unchanged from original
```

### Normalization (In-Place)

Normalization updates columns in place with automatic unit conversion:

```python
from pyngb.api.analysis import normalize_to_initial_mass

# Before normalization
print(f"Mass units before: {get_column_units(table, 'mass')}")      # "mg"
print(f"DSC units before: {get_column_units(table, 'dsc_signal')}")  # "µV"

# Normalize in place
normalized_table = normalize_to_initial_mass(table)

# After normalization - units automatically updated
print(f"Mass units after: {get_column_units(normalized_table, 'mass')}")      # "mg/mg"
print(f"DSC units after: {get_column_units(normalized_table, 'dsc_signal')}")  # "µV/mg"

# Processing history automatically updated
mass_history = get_processing_history(normalized_table, 'mass')
print(mass_history)  # ["raw", "normalized"]
```

### Baseline Correction

Baseline correction status is tracked for applicable columns:

```python
# Load with baseline correction
baseline_table = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")

# Mark as baseline corrected
corrected_table = mark_baseline_corrected(baseline_table, ['mass', 'dsc_signal'])

# Check status
mass_status = get_column_baseline_status(corrected_table, 'mass')
dsc_status = get_column_baseline_status(corrected_table, 'dsc_signal')
time_status = get_column_baseline_status(corrected_table, 'time')  # None (not applicable)

print(f"Mass baseline corrected: {mass_status}")   # True
print(f"DSC baseline corrected: {dsc_status}")    # True
print(f"Time baseline status: {time_status}")     # None
```

## Complete Analysis Workflow

Here's a complete workflow showing metadata evolution:

```python
from pyngb import read_ngb
from pyngb.api.analysis import add_dtg, normalize_to_initial_mass
from pyngb.api.metadata import mark_baseline_corrected, inspect_column_metadata

# 1. Load raw data
print("=== STEP 1: Raw Data ===")
table = read_ngb("sample.ngb-ss3")
print(f"Mass metadata: {inspect_column_metadata(table, 'mass')}")

# 2. Apply baseline correction
print("\n=== STEP 2: Baseline Correction ===")
baseline_table = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")
baseline_corrected = mark_baseline_corrected(baseline_table, ['mass', 'dsc_signal'])
print(f"Mass metadata: {inspect_column_metadata(baseline_corrected, 'mass')}")

# 3. Add DTG analysis
print("\n=== STEP 3: DTG Analysis ===")
with_dtg = add_dtg(baseline_corrected)
print(f"Mass metadata: {inspect_column_metadata(with_dtg, 'mass')}")
print(f"DTG metadata: {inspect_column_metadata(with_dtg, 'dtg')}")

# 4. Normalize to initial mass
print("\n=== STEP 4: Normalization ===")
final_table = normalize_to_initial_mass(with_dtg)
print(f"Mass metadata: {inspect_column_metadata(final_table, 'mass')}")
print(f"DSC metadata: {inspect_column_metadata(final_table, 'dsc_signal')}")

# Final provenance summary
print(f"\n=== FINAL PROVENANCE ===")
for col in ['time', 'mass', 'dsc_signal', 'dtg']:
    if col in final_table.column_names:
        metadata = inspect_column_metadata(final_table, col)
        units = metadata['units']
        history = ' → '.join(metadata['processing_history'])
        source = metadata['source']
        baseline = metadata.get('baseline_subtracted', 'N/A')
        print(f"{col:12} | {units:8} | {source:11} | {history} | BL: {baseline}")
```

Expected output:
```
time         | min      | measurement | raw | BL: N/A
mass         | mg/mg    | measurement | raw → baseline_corrected → normalized | BL: True
dsc_signal   | µV/mg    | measurement | raw → baseline_corrected → normalized | BL: True
dtg          | mg/min   | derived     | calculated | BL: N/A
```

## Persistence and Interoperability

Column metadata is stored natively in PyArrow format and persists when saved to Parquet:

```python
import pyarrow.parquet as pq

# Save to Parquet - metadata included
pq.write_table(final_table, "analysis_results.parquet")

# Load from Parquet - metadata preserved
reloaded_table = pq.read_table("analysis_results.parquet")

# Metadata still available
mass_metadata = inspect_column_metadata(reloaded_table, 'mass')
print(f"Metadata preserved: {mass_metadata}")
```

## Advanced Usage

### Custom Processing Steps

```python
# Add custom processing steps
table = add_column_processing_step(table, 'mass', 'outlier_removal')
table = add_column_processing_step(table, 'mass', 'temperature_corrected')

history = get_processing_history(table, 'mass')
print(history)  # ["raw", "outlier_removal", "temperature_corrected"]
```

### Bulk Operations

```python
# Mark multiple columns as baseline corrected
table = mark_baseline_corrected(table, ['mass', 'dsc_signal'])

# Check multiple column units
columns = ['time', 'mass', 'dsc_signal', 'dtg']
for col in columns:
    if col in table.column_names:
        units = get_column_units(table, col)
        print(f"{col}: {units}")
```

### Metadata Validation

```python
def validate_metadata_completeness(table, required_columns):
    """Validate that required columns have complete metadata."""
    missing_metadata = []

    for col in required_columns:
        if col not in table.column_names:
            missing_metadata.append(f"Column '{col}' not found")
            continue

        metadata = inspect_column_metadata(table, col)
        if not metadata:
            missing_metadata.append(f"Column '{col}' has no metadata")
        elif 'units' not in metadata:
            missing_metadata.append(f"Column '{col}' missing units")
        elif not metadata.get('processing_history'):
            missing_metadata.append(f"Column '{col}' missing processing history")

    return missing_metadata

# Use validation
issues = validate_metadata_completeness(table, ['time', 'mass', 'dsc_signal'])
if issues:
    print("Metadata issues found:")
    for issue in issues:
        print(f"  - {issue}")
else:
    print("All required columns have complete metadata ✓")
```

## Best Practices

1. **Always use the high-level API functions** (`add_dtg`, `normalize_to_initial_mass`) rather than manual operations to ensure metadata is properly maintained.

2. **Check baseline applicability** using `get_column_baseline_status()` - it returns `None` for non-applicable columns.

3. **Inspect metadata after major operations** to understand the complete provenance of your data.

4. **Save to Parquet format** to preserve metadata across sessions.

5. **Use meaningful processing step names** when adding custom steps with `add_column_processing_step()`.

## Column Type Reference

| Column Name | Default Units | Baseline Applicable | Typical Source |
|-------------|---------------|-------------------|----------------|
| `time` | min | No | measurement |
| `mass` | mg | Yes | measurement |
| `dsc_signal` | µV | Yes | measurement |
| `sample_temperature` | °C | No | measurement |
| `furnace_temperature` | °C | No | measurement |
| `dtg` | mg/min | No | derived |
| `purge_flow_1` | ml/min | No | measurement |
| `purge_flow_2` | ml/min | No | measurement |
| `protective_flow` | ml/min | No | measurement |
| `furnace_power` | W | No | measurement |

The column metadata system provides complete traceability for your thermal analysis workflows, ensuring you always know the units, processing history, and provenance of your data.
