"""DSC data validation for STA data."""

import polars as pl

from .base import ValidationResult


class DSCValidator:
    """Validates DSC (Differential Scanning Calorimetry) measurements."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize DSC validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform DSC validation.

        Args:
            result: ValidationResult to store findings
        """
        if "dsc_signal" not in self.df.columns:
            return

        self._check_null_values(result)
        self._check_signal_variation(result)
        self._check_extreme_values(result)

    def _check_null_values(self, result: ValidationResult) -> None:
        """Check for null values in DSC data."""
        dsc_col = self.df.select("dsc_signal")
        null_count = dsc_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(f"DSC has {null_count} null values ({percentage:.1f}%)")

    def _check_signal_variation(self, result: ValidationResult) -> None:
        """Check for variation in DSC signal."""
        dsc_col = self.df.select("dsc_signal")
        dsc_stats = dsc_col.describe()
        dsc_std = dsc_stats.filter(pl.col("statistic") == "std")["dsc_signal"][0]

        if dsc_std < 0.001:
            result.add_warning(
                "DSC signal is nearly constant - no thermal events detected"
            )
        else:
            result.add_pass("DSC signal shows variation")

    def _check_extreme_values(self, result: ValidationResult) -> None:
        """Check for extreme DSC values."""
        dsc_col = self.df.select("dsc_signal")
        dsc_stats = dsc_col.describe()
        dsc_min = dsc_stats.filter(pl.col("statistic") == "min")["dsc_signal"][0]
        dsc_max = dsc_stats.filter(pl.col("statistic") == "max")["dsc_signal"][0]

        if abs(dsc_max) > 1000 or abs(dsc_min) > 1000:
            result.add_warning(
                f"Extreme DSC values detected: {dsc_min:.1f} to {dsc_max:.1f} μV"
            )
