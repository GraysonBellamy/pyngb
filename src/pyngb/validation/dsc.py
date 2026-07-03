"""DSC data validation for STA data."""

import numpy as np
import polars as pl

from .base import ValidationResult
from .helpers import finite_values


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

        self._check_missing_values(result)

        dsc = finite_values(self.df["dsc_signal"]).to_numpy()
        if len(dsc) == 0:
            result.add_error("DSC has no valid (non-null, finite) values")
            return

        self._check_signal_variation(result, dsc)
        self._check_extreme_values(result, dsc)

    def _check_missing_values(self, result: ValidationResult) -> None:
        """Check for null or non-finite values in DSC data."""
        dsc_col = self.df["dsc_signal"]
        null_count = dsc_col.null_count()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(f"DSC has {null_count} null values ({percentage:.1f}%)")
        non_finite = len(dsc_col) - null_count - len(finite_values(dsc_col))
        if non_finite > 0:
            result.add_warning(f"DSC has {non_finite} non-finite values")

    def _check_signal_variation(
        self, result: ValidationResult, dsc: np.ndarray
    ) -> None:
        """Check for variation in DSC signal."""
        if len(dsc) < 2:
            result.add_info("Single DSC value - cannot assess signal variation")
            return

        dsc_std = float(np.std(dsc, ddof=1))
        if dsc_std < 0.001:
            result.add_warning(
                "DSC signal is nearly constant - no thermal events detected"
            )
        else:
            result.add_pass("DSC signal shows variation")

    def _check_extreme_values(self, result: ValidationResult, dsc: np.ndarray) -> None:
        """Check for extreme DSC values."""
        dsc_min = float(dsc.min())
        dsc_max = float(dsc.max())

        if abs(dsc_max) > 1000 or abs(dsc_min) > 1000:
            result.add_warning(
                f"Extreme DSC values detected: {dsc_min:.1f} to {dsc_max:.1f} μV"
            )
