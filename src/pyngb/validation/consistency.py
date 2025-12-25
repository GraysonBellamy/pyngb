"""Data consistency validation for STA data."""

import numpy as np
import polars as pl

from .base import ValidationResult


class ConsistencyValidator:
    """Validates consistency between different measurements."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize consistency validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform consistency validation.

        Args:
            result: ValidationResult to store findings
        """
        self._check_column_length(result)
        self._check_time_temperature_correlation(result)

    def _check_column_length(self, result: ValidationResult) -> None:
        """Check if all columns have the same length."""
        # This is guaranteed by DataFrame structure
        result.add_pass("All columns have consistent length")

    def _check_time_temperature_correlation(self, result: ValidationResult) -> None:
        """Check if temperature changes correlate with time."""
        if "time" not in self.df.columns or "sample_temperature" not in self.df.columns:
            return

        time_data = self.df.select("time").to_numpy().flatten()
        temp_data = self.df.select("sample_temperature").to_numpy().flatten()

        # Simple correlation check
        if len(time_data) > 1 and len(temp_data) > 1:
            correlation = np.corrcoef(time_data, temp_data)[0, 1]
            if abs(correlation) > 0.8:
                result.add_pass(
                    f"Time and temperature are well correlated (r={correlation:.3f})"
                )
            else:
                result.add_info(
                    f"Time and temperature correlation: r={correlation:.3f}"
                )
