"""Temperature data validation for STA data."""

import numpy as np
import polars as pl

from .base import ValidationResult


class TemperatureValidator:
    """Validates temperature measurements."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize temperature validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform temperature validation.

        Args:
            result: ValidationResult to store findings
        """
        if "sample_temperature" not in self.df.columns:
            return

        self._check_null_values(result)
        self._check_temperature_range(result)
        self._check_physical_validity(result)
        self._check_temperature_profile(result)

    def _check_null_values(self, result: ValidationResult) -> None:
        """Check for null values in temperature data."""
        temp_col = self.df.select("sample_temperature")
        null_count = temp_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(
                f"Temperature has {null_count} null values ({percentage:.1f}%)"
            )

    def _check_temperature_range(self, result: ValidationResult) -> None:
        """Check temperature range is reasonable."""
        temp_col = self.df.select("sample_temperature")
        temp_stats = temp_col.describe()
        temp_min = temp_stats.filter(pl.col("statistic") == "min")[
            "sample_temperature"
        ][0]
        temp_max = temp_stats.filter(pl.col("statistic") == "max")[
            "sample_temperature"
        ][0]

        # Check temperature range
        if temp_min == temp_max:
            result.add_error("Temperature is constant throughout experiment")
        elif temp_max - temp_min < 10:
            result.add_warning(f"Small temperature range: {temp_max - temp_min:.1f}°C")
        else:
            result.add_pass("Temperature range is reasonable")

    def _check_physical_validity(self, result: ValidationResult) -> None:
        """Check for physically realistic temperatures."""
        temp_col = self.df.select("sample_temperature")
        temp_stats = temp_col.describe()
        temp_min = temp_stats.filter(pl.col("statistic") == "min")[
            "sample_temperature"
        ][0]
        temp_max = temp_stats.filter(pl.col("statistic") == "max")[
            "sample_temperature"
        ][0]

        if temp_min < -273:  # Below absolute zero
            result.add_error(f"Temperature below absolute zero: {temp_min:.1f}°C")
        elif temp_min < -50:
            result.add_warning(f"Very low minimum temperature: {temp_min:.1f}°C")

        if temp_max > 2000:
            result.add_warning(f"Very high maximum temperature: {temp_max:.1f}°C")

    def _check_temperature_profile(self, result: ValidationResult) -> None:
        """Check temperature profile monotonicity."""
        temp_col = self.df.select("sample_temperature")
        temp_data = temp_col.to_numpy().flatten()
        temp_diff = np.diff(temp_data)

        if np.all(temp_diff >= 0):
            result.add_info("Temperature profile is monotonically increasing (heating)")
        elif np.all(temp_diff <= 0):
            result.add_info("Temperature profile is monotonically decreasing (cooling)")
        else:
            # Mixed heating/cooling
            heating_points: int = int(np.sum(temp_diff > 0))
            cooling_points: int = int(np.sum(temp_diff < 0))
            result.add_info(
                f"Mixed temperature profile: {heating_points} heating, {cooling_points} cooling points"
            )
