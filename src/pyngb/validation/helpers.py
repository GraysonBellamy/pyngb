"""Helper functions for validation."""

import polars as pl
import pyarrow as pa


def finite_values(series: pl.Series) -> pl.Series:
    """Return the non-null, finite values of a column.

    Validators compute statistics on the result so that nulls and NaNs in a
    measurement column degrade to explicit findings instead of poisoning every
    downstream comparison (None/NaN propagates through min/std/percentile and
    silently disables or crashes the checks).
    """
    s = series.drop_nulls()
    if s.dtype.is_float():
        s = s.filter(s.is_finite())
    return s


def _ensure_polars_dataframe(data: pa.Table | pl.DataFrame) -> pl.DataFrame:
    """Helper function to efficiently convert data to Polars DataFrame.

    Avoids unnecessary conversions when data is already a Polars DataFrame.

    Args:
        data: Input data as PyArrow Table or Polars DataFrame

    Returns:
        Polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, pa.Table):
        df_temp = pl.from_arrow(data)
        return df_temp if isinstance(df_temp, pl.DataFrame) else df_temp.to_frame()
    if isinstance(data, pl.Series):
        return pl.DataFrame(data)
    # If data is not a recognized type, assume it's already a DataFrame-like object
    # This should not happen in normal usage, but provides a fallback
    return pl.DataFrame(data)
