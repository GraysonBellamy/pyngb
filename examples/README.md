# pyNGB Examples

This directory contains comprehensive examples showing how to use pyNGB effectively.

## Available Examples

- **batch_processing_example.py** - Demonstrates batch processing and data validation features
- **01_basic_loading.py** - Basic data loading and exploration
- **02_data_analysis.py** - Statistical analysis and visualization
- **03_advanced_features.py** - Advanced parsing and customization

## New Features (v0.1.0+)

### Batch Processing

Process multiple NGB files efficiently with parallel processing:

```python
from pyngb import BatchProcessor

processor = BatchProcessor(max_workers=4)
results = processor.process_directory('./data', output_dir='./processed')
```

### Data Validation

Validate STA data quality and detect common issues:

```python
from pyngb import validate_sta_data, QualityChecker

# Quick validation
issues = validate_sta_data(table)

# Comprehensive quality checking
checker = QualityChecker(table)
result = checker.full_validation()
print(result.report())
```

### Dataset Management

Manage collections of STA files:

```python
from pyngb import NGBDataset

dataset = NGBDataset()
dataset.add_directory('./data')
summary = dataset.get_summary()
dataset.to_parquet('consolidated_data.parquet')
```

## Basic Usage Examples

### 1. Loading and Exploring Data

```python
# examples/01_basic_loading.py
from pyngb import load_ngb_data
import json

# Load NGB file
table = load_ngb_data("sample.ngb-ss3")

print(f"Data shape: {table.num_rows} rows × {len(table.column_names)} columns")
print(f"Columns: {table.column_names}")

# Access metadata
metadata_bytes = table.schema.metadata[b'file_metadata']
metadata = json.loads(metadata_bytes)

print(f"Instrument: {metadata.get('instrument', 'Unknown')}")
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
print(f"Mass: {metadata.get('sample_mass', 'Unknown')} mg")
```

### 2. Data Analysis with Polars

```python
# examples/02_polars_analysis.py
import polars as pl
from pyngb import load_ngb_data

# Load data
table = load_ngb_data("sample.ngb-ss3")
df = pl.from_arrow(table)

# Basic analysis
print("Temperature range:", df['temperature'].min(), "to", df['temperature'].max(), "°C")
print("Total time:", df['time'].max(), "minutes")

# Calculate mass loss percentage
initial_mass = df['sample_mass'].first()
final_mass = df['sample_mass'].last()
mass_loss_pct = (initial_mass - final_mass) / initial_mass * 100
print(f"Mass loss: {mass_loss_pct:.2f}%")

# Find peak DSC signal
peak_dsc = df.select([
    pl.col('temperature').filter(pl.col('dsc') == pl.col('dsc').max()).first(),
    pl.col('dsc').max()
])
print(f"Peak DSC: {peak_dsc['dsc'][0]:.3f} at {peak_dsc['temperature'][0]:.1f}°C")
```

### 3. Visualization with Matplotlib

```python
# examples/03_visualization.py
import matplotlib.pyplot as plt
import polars as pl
from pyngb import load_ngb_data

# Load and convert data
table = load_ngb_data("sample.ngb-ss3")
df = pl.from_arrow(table).to_pandas()

# Create comprehensive plot
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))

# Temperature vs Time
ax1.plot(df['time'], df['temperature'], 'r-', linewidth=1)
ax1.set_xlabel('Time (min)')
ax1.set_ylabel('Temperature (°C)')
ax1.set_title('Temperature Program')
ax1.grid(True, alpha=0.3)

# DSC vs Temperature
ax2.plot(df['temperature'], df['dsc'], 'b-', linewidth=1)
ax2.set_xlabel('Temperature (°C)')
ax2.set_ylabel('DSC (μV)')
ax2.set_title('DSC Signal')
ax2.grid(True, alpha=0.3)

# Mass Loss vs Temperature
ax3.plot(df['temperature'], df['sample_mass'], 'g-', linewidth=1)
ax3.set_xlabel('Temperature (°C)')
ax3.set_ylabel('Mass (mg)')
ax3.set_title('Thermogravimetric Analysis')
ax3.grid(True, alpha=0.3)

# Combined view
ax4_twin = ax4.twinx()
ax4.plot(df['temperature'], df['dsc'], 'b-', alpha=0.7, label='DSC')
ax4_twin.plot(df['temperature'], df['sample_mass'], 'g-', alpha=0.7, label='Mass')
ax4.set_xlabel('Temperature (°C)')
ax4.set_ylabel('DSC (μV)', color='b')
ax4_twin.set_ylabel('Mass (mg)', color='g')
ax4.set_title('Combined STA Analysis')
ax4.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()
```

### 4. Advanced Metadata Access

```python
# examples/04_metadata_extraction.py
from pyngb import get_sta_data
import json
from datetime import datetime

# Get separate metadata and data
metadata, data = get_sta_data("sample.ngb-ss3")

# Print comprehensive metadata
print("=== INSTRUMENT INFORMATION ===")
print(f"Instrument: {metadata.get('instrument', 'Unknown')}")
print(f"Lab: {metadata.get('lab', 'Unknown')}")
print(f"Operator: {metadata.get('operator', 'Unknown')}")

print("\n=== SAMPLE INFORMATION ===")
print(f"Sample ID: {metadata.get('sample_id', 'Unknown')}")
print(f"Sample Name: {metadata.get('sample_name', 'Unknown')}")
print(f"Material: {metadata.get('material', 'Unknown')}")
print(f"Sample Mass: {metadata.get('sample_mass', 'Unknown')} mg")
print(f"Crucible Mass: {metadata.get('crucible_mass', 'Unknown')} mg")

print("\n=== EXPERIMENTAL CONDITIONS ===")
print(f"Crucible: {metadata.get('crucible_type', 'Unknown')}")
print(f"Furnace: {metadata.get('furnace_type', 'Unknown')}")
print(f"Carrier: {metadata.get('carrier_type', 'Unknown')}")

# Parse date
date_str = metadata.get('date_performed', '')
if date_str:
    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        print(f"Date: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
    except:
        print(f"Date: {date_str}")

print("\n=== TEMPERATURE PROGRAM ===")
temp_program = metadata.get('temperature_program', {})
for step_name, params in temp_program.items():
    print(f"{step_name}:")
    for key, value in params.items():
        if key == 'heating_rate':
            print(f"  {key}: {value} °C/min")
        elif key == 'temperature':
            print(f"  {key}: {value} °C")
        elif key == 'time':
            print(f"  {key}: {value} min")
        else:
            print(f"  {key}: {value}")

print("\n=== CALIBRATION CONSTANTS ===")
cal_constants = metadata.get('calibration_constants', {})
for param, value in cal_constants.items():
    print(f"{param}: {value}")
```

### 5. Batch Processing

```python
# examples/05_batch_processing.py
from pathlib import Path
import polars as pl
from pyngb import load_ngb_data
import json

def process_ngb_files(directory: str, output_format='parquet'):
    """Process all NGB files in a directory."""
    ngb_dir = Path(directory)
    results = []

    for ngb_file in ngb_dir.glob("*.ngb-ss3"):
        print(f"Processing {ngb_file.name}...")

        try:
            # Load data
            table = load_ngb_data(str(ngb_file))

            # Extract key information
            metadata_bytes = table.schema.metadata[b'file_metadata']
            metadata = json.loads(metadata_bytes)

            # Convert to DataFrame for analysis
            df = pl.from_arrow(table)

            # Calculate summary statistics
            summary = {
                'filename': ngb_file.name,
                'sample_name': metadata.get('sample_name', 'Unknown'),
                'instrument': metadata.get('instrument', 'Unknown'),
                'operator': metadata.get('operator', 'Unknown'),
                'data_points': len(df),
                'time_range': f"{df['time'].min():.1f} - {df['time'].max():.1f} min",
                'temp_range': f"{df['temperature'].min():.1f} - {df['temperature'].max():.1f} °C",
                'initial_mass': df['sample_mass'].first(),
                'final_mass': df['sample_mass'].last(),
            }

            # Calculate mass loss
            if summary['initial_mass'] and summary['final_mass']:
                mass_loss = (summary['initial_mass'] - summary['final_mass']) / summary['initial_mass'] * 100
                summary['mass_loss_percent'] = f"{mass_loss:.2f}%"

            results.append(summary)

            # Save individual file
            output_file = ngb_file.with_suffix(f'.{output_format}')
            if output_format == 'parquet':
                table.to_pandas().to_parquet(output_file)
            elif output_format == 'csv':
                df.write_csv(output_file)

        except Exception as e:
            print(f"Error processing {ngb_file.name}: {e}")

    # Create summary report
    summary_df = pl.DataFrame(results)
    summary_df.write_csv(ngb_dir / "batch_summary.csv")
    print(f"\nProcessed {len(results)} files. Summary saved to batch_summary.csv")

    return summary_df

# Usage
if __name__ == "__main__":
    # Process all NGB files in current directory
    summary = process_ngb_files("./data")
    print(summary)
```

### 6. Custom Parser Configuration

```python
# examples/06_custom_configuration.py
from pyngb import NGBParser, PatternConfig
from pyngb.constants import DataType

# Create custom configuration
config = PatternConfig()

# Add custom column mapping
config.column_map["99"] = "custom_sensor"
config.column_map["9a"] = "environmental_data"

# Add custom metadata patterns
config.metadata_patterns["custom_field"] = (b"\x99\x17", b"\x88\x10")

# Create parser with custom config
parser = NGBParser(config)

# Parse with custom settings
metadata, data = parser.parse("sample.ngb-ss3")

print(f"Available columns: {data.column_names}")
print(f"Custom metadata: {metadata.get('custom_field', 'Not found')}")

# Example: Extended parser with validation
from pyngb.core.parser import NGBParserExtended

extended_parser = NGBParserExtended(config, cache_patterns=True)

# Add runtime customizations
extended_parser.add_custom_column_mapping("9b", "pressure_sensor")
extended_parser.add_metadata_pattern("batch_id", b"\x9c\x17", b"\x44\x08")

# Parse with validation
metadata, data = extended_parser.parse_with_validation("sample.ngb-ss3")
```

## Running the Examples

Each example can be run independently:

```bash
# Make sure you have a sample NGB file
python examples/01_basic_loading.py

# For visualization example, install matplotlib:
pip install matplotlib
python examples/03_visualization.py

# For batch processing:
mkdir data
# Copy your NGB files to the data directory
python examples/05_batch_processing.py
```

## Sample Data

The examples work with any valid NGB file. For testing, you can use:
- The sample file in `tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3`
- Your own experimental NGB files

## Next Steps

- Check out the [API Reference](../docs/api_reference.md) for detailed function documentation
- See [Advanced Usage](../docs/advanced_usage.md) for performance optimization tips
- Visit [Troubleshooting](../docs/troubleshooting.md) for common issues and solutions
