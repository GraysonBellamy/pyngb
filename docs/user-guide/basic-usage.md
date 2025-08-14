# Basic Usage

This page covers the essentials of loading and working with NGB files.

## Load a file

```python
from pyngb import read_ngb

table = read_ngb("sample.ngb-ss3")
print(table.num_rows, table.num_columns)
```

## Convert to Polars

```python
import polars as pl

df = pl.from_arrow(table)
print(df.describe())
```
