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
