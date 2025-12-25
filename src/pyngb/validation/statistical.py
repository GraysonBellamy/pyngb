"""Statistical validation for STA data."""

import numpy as np
import polars as pl

from .base import ValidationResult


class StatisticalValidator:
    """Validates statistical properties and detects anomalies."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize statistical validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform statistical validation.

        Args:
            result: ValidationResult to store findings
        """
        self._check_outliers(result)

    def _check_outliers(self, result: ValidationResult) -> None:
        """Check for outliers using IQR method."""
        numeric_columns = [
            col
            for col, dtype in zip(self.df.columns, self.df.dtypes)
            if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
        ]

        for col in numeric_columns:
            data = self.df.select(col).to_numpy().flatten()

            # Check for outliers using IQR method
            if len(data) > 10:  # Only check if enough data points
                q1 = np.percentile(data, 25)
                q3 = np.percentile(data, 75)
                iqr = q3 - q1

                if iqr > 0:
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr

                    outliers: int = int(
                        np.sum((data < lower_bound) | (data > upper_bound))
                    )
                    outlier_percentage = (outliers / len(data)) * 100

                    if outlier_percentage > 5:
                        result.add_warning(
                            f"Column '{col}' has {outliers} outliers ({outlier_percentage:.1f}%)"
                        )
