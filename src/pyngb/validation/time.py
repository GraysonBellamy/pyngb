"""Time data validation for STA data."""

import numpy as np
import polars as pl

from .base import ValidationResult
from .helpers import finite_values


class TimeValidator:
    """Validates time measurements."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize time validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform time validation.

        Args:
            result: ValidationResult to store findings
        """
        if "time" not in self.df.columns:
            return

        self._check_missing_values(result)

        # Nulls/NaNs are excluded (and reported above) so that a single bad
        # sample cannot poison np.diff into "goes backwards 0 times".
        time = finite_values(self.df["time"]).to_numpy()
        if len(time) < 2:
            return

        self._check_time_progression(result, time)
        self._check_time_intervals(result, time)

    def _check_missing_values(self, result: ValidationResult) -> None:
        """Check for null or non-finite values in time data."""
        time_col = self.df["time"]
        null_count = time_col.null_count()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(f"Time has {null_count} null values ({percentage:.1f}%)")
        non_finite = len(time_col) - null_count - len(finite_values(time_col))
        if non_finite > 0:
            result.add_warning(f"Time has {non_finite} non-finite values")

    def _check_time_progression(
        self, result: ValidationResult, time: np.ndarray
    ) -> None:
        """Check time progression is monotonic."""
        time_diff = np.diff(time)

        if np.all(time_diff >= 0):
            result.add_pass("Time progresses monotonically")
        else:
            backwards_count: int = int(np.sum(time_diff < 0))
            result.add_error(f"Time goes backwards {backwards_count} times")

    def _check_time_intervals(self, result: ValidationResult, time: np.ndarray) -> None:
        """Check for reasonable time intervals."""
        time_diff = np.diff(time)
        positive_intervals = time_diff[time_diff > 0]
        if len(positive_intervals) > 0:
            avg_interval = float(np.mean(positive_intervals))
            if avg_interval < 0.1:  # Less than 0.1 second intervals
                result.add_info(
                    f"Very high time resolution: {avg_interval:.3f}s average interval"
                )
            elif avg_interval > 60:  # More than 1 minute intervals
                result.add_warning(
                    f"Low time resolution: {avg_interval:.1f}s average interval"
                )
