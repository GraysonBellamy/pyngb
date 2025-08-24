# DTG (Derivative Thermogravimetry) Analysis

pyNGB provides clean, simple DTG calculation capabilities with smart defaults and flexible control. This guide covers everything you need to know about DTG analysis using the simplified `pyngb.analysis` module.

## Overview

DTG (Derivative Thermogravimetry) represents the rate of mass change with respect to time. It's essential for:

- **Peak Detection**: Identifying decomposition events
- **Rate Analysis**: Calculating reaction rates
- **Material Characterization**: Understanding thermal behavior
- **Quality Control**: Comparing sample properties

pyNGB implements a **dramatically simplified** DTG interface focused on the 90% use case while maintaining power user flexibility.

## Quick Start

### Dead Simple DTG Calculation

```python
from pyngb import read_ngb, dtg
import polars as pl

# Load your data
table = read_ngb("sample.ngb-ss3")
df = pl.from_arrow(table)

# Convert to numpy arrays
time = df.get_column('time').to_numpy()
mass = df.get_column('mass').to_numpy()

# Calculate DTG - one line, perfect results
dtg_values = dtg(time, mass)

print(f"DTG range: {dtg_values.min():.3f} to {dtg_values.max():.3f} mg/min")
```

That's it! The `dtg()` function uses smart defaults that work great for 90% of thermal analysis data.

## Method Selection

Choose between two proven calculation methods:

```python
# Savitzky-Golay (recommended) - smooth and accurate
dtg_savgol = dtg(time, mass, method="savgol")    # Default

# NumPy gradient - simple and fast
dtg_gradient = dtg(time, mass, method="gradient") # For comparison

# Compare methods
import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))
plt.plot(temperature, dtg_savgol, label='Savitzky-Golay (recommended)', linewidth=2)
plt.plot(temperature, dtg_gradient, label='Gradient method', alpha=0.7)
plt.xlabel('Temperature (°C)')
plt.ylabel('DTG (mg/min)')
plt.title('DTG Method Comparison')
plt.legend()
plt.show()
```

**Method Guidelines:**
- **`savgol`**: Best for most applications - smooth, accurate, handles noise well
- **`gradient`**: Good for comparison, validation, or when scipy isn't available

## Smoothing Control

Control smoothing with three clear levels based on your needs:

```python
# Preserve all features - use for quantitative analysis
dtg_preserve = dtg(time, mass, smooth="strict")

# Balanced smoothing - recommended for most applications
dtg_balanced = dtg(time, mass, smooth="medium")  # Default

# Heavy smoothing - use for noisy data or trend analysis
dtg_clean = dtg(time, mass, smooth="loose")

# Visualize the difference
plt.figure(figsize=(14, 8))
plt.plot(temperature, dtg_preserve, label='Strict - preserve all features', alpha=0.8)
plt.plot(temperature, dtg_balanced, label='Medium - balanced (default)', linewidth=2)
plt.plot(temperature, dtg_clean, label='Loose - remove noise', alpha=0.8)
plt.xlabel('Temperature (°C)')
plt.ylabel('DTG (mg/min)')
plt.title('DTG Smoothing Level Comparison')
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()
```

### Choosing Smoothing Levels

**Use Case Guidelines:**

| Smoothing | Window | Polyorder | Best For |
|-----------|--------|-----------|----------|
| `"strict"` | 7 | 1 | Quantitative peak analysis, preserve all features |
| `"medium"` | 25 | 2 | General analysis, good balance (default) |
| `"loose"` | 51 | 3 | Noisy data, trend identification |

## Advanced Control for Power Users

Need manual control? Use `dtg_custom()`:

```python
from pyngb import dtg_custom

# Custom Savitzky-Golay parameters
dtg_custom_sg = dtg_custom(time, mass, method="savgol",
                          window=31, polyorder=3)

# Custom gradient with post-smoothing
dtg_custom_grad = dtg_custom(time, mass, method="gradient",
                            window=15, polyorder=2)

# Compare with default
dtg_default = dtg(time, mass)

print("Custom vs Default correlation:",
      np.corrcoef(dtg_custom_sg, dtg_default)[0,1])
```

## Integration with PyArrow Tables

Work directly with your NGB data tables:

```python
from pyngb import add_dtg, calculate_table_dtg

# Method 1: Add DTG column to existing table
table_with_dtg = add_dtg(table, method="savgol", smooth="medium")
print(f"Added DTG. New shape: {table_with_dtg.num_rows} x {table_with_dtg.num_columns}")

# Method 2: Calculate DTG as array
dtg_array = calculate_table_dtg(table, smooth="strict")
print(f"DTG array shape: {dtg_array.shape}")

# Method 3: Custom column name and parameters
table_rates = add_dtg(table, method="gradient", smooth="loose",
                     column_name="mass_loss_rate")
```

## Practical Examples

### Peak Detection Integration

```python
from scipy.signal import find_peaks
import numpy as np

# Calculate DTG with appropriate smoothing for peak detection
dtg_values = dtg(time, mass, smooth="medium")

# Find peaks (DTG peaks are negative for mass loss, so invert)
peaks, properties = find_peaks(-dtg_values, height=0.01, distance=50)

# Plot with peak markers
temperature = df.get_column('sample_temperature').to_numpy()

plt.figure(figsize=(12, 6))
plt.plot(temperature, dtg_values, 'b-', linewidth=2, label='DTG')
plt.plot(temperature[peaks], dtg_values[peaks], 'ro', markersize=8, label='Detected peaks')

# Annotate peak temperatures
for i, peak in enumerate(peaks):
    plt.annotate(f'{temperature[peak]:.0f}°C',
                xy=(temperature[peak], dtg_values[peak]),
                xytext=(10, 10), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))

plt.xlabel('Temperature (°C)')
plt.ylabel('DTG (mg/min)')
plt.title('DTG Analysis with Automatic Peak Detection')
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

print(f"Found {len(peaks)} peaks at temperatures: {temperature[peaks]}")
```

### Batch Processing Multiple Files

```python
from pyngb import read_ngb, dtg
import numpy as np

# Process multiple files
files = ["sample1.ngb-ss3", "sample2.ngb-ss3", "sample3.ngb-ss3"]
results = []

for file_path in files:
    table = read_ngb(file_path)
    df = pl.from_arrow(table)

    # Extract data
    time = df.get_column('time').to_numpy()
    mass = df.get_column('mass').to_numpy()
    temperature = df.get_column('sample_temperature').to_numpy()

    # Calculate DTG with smart defaults
    dtg_values = dtg(time, mass)

    # Store results
    results.append({
        'file': file_path,
        'max_rate': np.max(np.abs(dtg_values)),
        'temp_at_max_rate': temperature[np.argmax(np.abs(dtg_values))],
        'total_mass_loss': mass[0] - mass[-1],
        'mass_loss_percent': ((mass[0] - mass[-1]) / mass[0]) * 100
    })

# Print summary
print("\nDTG Analysis Summary:")
for result in results:
    print(f"{result['file']}: Max rate = {result['max_rate']:.3f} mg/min "
          f"at {result['temp_at_max_rate']:.1f}°C, "
          f"Mass loss = {result['mass_loss_percent']:.1f}%")
```

### Data Quality Assessment

```python
def assess_dtg_quality(time, mass, dtg_values):
    """Assess the quality of DTG calculation."""

    # Check for reasonable values
    mass_range = mass.max() - mass.min()
    time_range = time.max() - time.min()
    expected_max_rate = (mass_range / (time_range / 60))  # mg/min

    # Quality metrics
    has_reasonable_range = abs(dtg_values.max()) < expected_max_rate * 2
    has_no_nan = not np.any(np.isnan(dtg_values))
    has_no_inf = not np.any(np.isinf(dtg_values))

    return {
        'dtg_range': (dtg_values.min(), dtg_values.max()),
        'expected_max_rate': expected_max_rate,
        'reasonable_range': has_reasonable_range,
        'no_nan': has_no_nan,
        'no_inf': has_no_inf,
        'quality_score': sum([has_reasonable_range, has_no_nan, has_no_inf])
    }

# Use it
dtg_values = dtg(time, mass, smooth="medium")
quality = assess_dtg_quality(time, mass, dtg_values)

print(f"DTG Quality Assessment:")
print(f"  Range: {quality['dtg_range'][0]:.3f} to {quality['dtg_range'][1]:.3f} mg/min")
print(f"  Quality Score: {quality['quality_score']}/3")
print(f"  Issues: {[] if quality['quality_score'] == 3 else 'Check data quality'}")
```

## Comparison with Complex Alternatives

### What pyNGB Used to Require (Old Way)

```python
# OLD - Complex, overwhelming
from pyngb.analysis import (
    calculate_dtg, estimate_noise_level, select_savgol_params,
    get_recommended_params_for_thermal_data, validate_savgol_params
)

# Too many steps, too many decisions
noise = estimate_noise_level(time, mass)
feature_scale = estimate_feature_scale(time, mass)
window, polyorder = select_savgol_params(time, mass, target_snr=5.0)
is_valid, msg = validate_savgol_params(window, polyorder, len(time))

if is_valid:
    dtg_result = calculate_dtg(time, mass, method="savgol", auto_params=False,
                              window_length=window, polyorder=polyorder)
```

### What pyNGB Does Now (New Way)

```python
# NEW - Simple, perfect
from pyngb import dtg

# One line, optimal results
dtg_result = dtg(time, mass)
```

## Error Handling and Troubleshooting

### Common Issues and Solutions

**Issue: DTG values seem too noisy**
```python
# Solution: Use looser smoothing
dtg_smooth = dtg(time, mass, smooth="loose")
```

**Issue: Important peaks are being smoothed out**
```python
# Solution: Use stricter smoothing
dtg_preserve = dtg(time, mass, smooth="strict")
```

**Issue: Need to compare with another method**
```python
# Solution: Compare savgol vs gradient
dtg_sg = dtg(time, mass, method="savgol")
dtg_grad = dtg(time, mass, method="gradient")
correlation = np.corrcoef(dtg_sg, dtg_grad)[0, 1]
print(f"Method correlation: {correlation:.3f}")
```

**Issue: Getting scipy import errors**
```python
# Solution: DTG gracefully degrades to gradient method
try:
    dtg_result = dtg(time, mass, method="savgol")
except ImportError:
    print("Scipy not available, using gradient method")
    dtg_result = dtg(time, mass, method="gradient")
```

### Validation

```python
def validate_dtg_input(time, mass):
    """Validate input data for DTG calculation."""
    errors = []

    if len(time) != len(mass):
        errors.append("time and mass arrays must have same length")

    if len(time) < 3:
        errors.append("Need at least 3 data points")

    if not np.all(np.isfinite(time)) or not np.all(np.isfinite(mass)):
        errors.append("Input data contains NaN or infinite values")

    if len(errors) > 0:
        raise ValueError("; ".join(errors))

    return True

# Use before DTG calculation
validate_dtg_input(time, mass)
dtg_values = dtg(time, mass)
```

## Performance Tips

### Memory Efficiency

```python
# For very large datasets, consider chunking
def chunked_dtg_processing(time, mass, chunk_size=10000):
    """Process DTG in chunks for large datasets."""
    if len(time) <= chunk_size:
        return dtg(time, mass)

    # Process in overlapping chunks to avoid edge effects
    overlap = 100
    dtg_chunks = []

    for i in range(0, len(time), chunk_size - overlap):
        end_idx = min(i + chunk_size, len(time))
        time_chunk = time[i:end_idx]
        mass_chunk = mass[i:end_idx]

        dtg_chunk = dtg(time_chunk, mass_chunk, smooth="medium")

        # Remove overlap (except for first and last chunks)
        if i > 0:
            dtg_chunk = dtg_chunk[overlap//2:]
        if end_idx < len(time):
            dtg_chunk = dtg_chunk[:-overlap//2]

        dtg_chunks.append(dtg_chunk)

    return np.concatenate(dtg_chunks)

# Use for large datasets
if len(time) > 50000:
    dtg_values = chunked_dtg_processing(time, mass)
else:
    dtg_values = dtg(time, mass)
```

### Speed Optimization

```python
# For multiple calculations on the same data, method selection matters
import time as time_module

methods = ["savgol", "gradient"]
times = {}

for method in methods:
    start = time_module.time()
    for _ in range(100):  # Multiple calculations
        result = dtg(time, mass, method=method, smooth="medium")
    end = time_module.time()
    times[method] = end - start

print("Performance comparison:")
for method, elapsed in times.items():
    print(f"  {method}: {elapsed:.3f}s for 100 calculations")
```

## Best Practices

1. **Start with defaults**: `dtg(time, mass)` works great for most data
2. **Use savgol method**: More accurate than gradient for thermal analysis
3. **Choose smoothing based on use case**:
   - `"strict"` for quantitative analysis
   - `"medium"` for general use (default)
   - `"loose"` for trend analysis or very noisy data
4. **Validate your data**: Check for NaN, infinite, or unreasonable values
5. **Compare methods**: Use gradient method to validate savgol results
6. **Consider data size**: Use appropriate smoothing for your data length
7. **Handle errors gracefully**: DTG will degrade to gradient if scipy unavailable

## Migration from Old API

If you're upgrading from the old complex DTG system:

```python
# OLD API (no longer available)
# from pyngb.analysis import calculate_dtg, calculate_smooth_dtg
# dtg_old = calculate_smooth_dtg(time, mass, smoothing="medium")

# NEW API (simple replacement)
from pyngb import dtg
dtg_new = dtg(time, mass, smooth="medium")

# OLD API (no longer available)
# from pyngb.analysis import ThermalDerivatives
# analyzer = ThermalDerivatives(time, mass)
# dtg_old = analyzer.calculate(method="savgol")

# NEW API (direct replacement)
dtg_new = dtg(time, mass, method="savgol")
```

The new API is **dramatically simpler** while providing the same (or better) results.

## Next Steps

- See the [API Reference](../api.md) for complete function documentation
- Browse [Data Analysis](data-analysis.md) for multi-sample workflows
- Check [Quickstart Guide](../quickstart.md) for more examples
- Review [Troubleshooting](../troubleshooting.md) for common issues

---

The simplified DTG interface makes derivative thermogravimetry analysis as easy as `dtg(time, mass)` while maintaining the flexibility power users need. No more complex parameter tuning - just clean, reliable results! 🚀
