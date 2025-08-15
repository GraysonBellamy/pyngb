````markdown
# Data Analysis

Combine, transform, and analyze data from multiple runs.

## Combine datasets

```python
import polars as pl
from pyngb import read_ngb

files = ["a.ngb-ss3", "b.ngb-ss3"]
frames = []
for f in files:
    t = read_ngb(f)
    df = pl.from_arrow(t).with_columns(pl.lit(f).alias("source_file"))
    frames.append(df)

combined = pl.concat(frames)
print(combined.shape)
```

## Quick stats

```python
if {'time', 'sample_temperature'} <= set(combined.columns):
    stats = combined.select([
        pl.col('time').max().alias('duration_s'),
        pl.col('sample_temperature').min().alias('min_temp_c'),
        pl.col('sample_temperature').max().alias('max_temp_c'),
    ])
    print(stats)
```
````
# Data Analysis

Combine, transform, and analyze data from multiple runs.

## Combine datasets

```python
import polars as pl
from pyngb import read_ngb

files = ["a.ngb-ss3", "b.ngb-ss3"]
frames = []
for f in files:
    t = read_ngb(f)
    df = pl.from_arrow(t).with_columns(pl.lit(f).alias("source_file"))
    frames.append(df)

combined = pl.concat(frames)
print(combined.shape)
```
