"""Mass data validation for STA data."""

import numpy as np
import polars as pl

from ..constants import FileMetadata
from .base import ValidationResult
from .helpers import finite_values


class MassValidator:
    """Validates mass measurements."""

    def __init__(self, df: pl.DataFrame, metadata: FileMetadata | None = None) -> None:
        """Initialize mass validator.

        Args:
            df: Polars DataFrame to validate
            metadata: Optional file metadata
        """
        self.df = df
        self.metadata = metadata or {}

    def validate(self, result: ValidationResult) -> None:
        """Perform mass validation.

        Args:
            result: ValidationResult to store findings
        """
        if "mass" not in self.df.columns:
            return

        self._check_missing_values(result)

        mass = finite_values(self.df["mass"]).to_numpy()
        if len(mass) == 0:
            result.add_error("Mass has no valid (non-null, finite) values")
            return

        self._check_mass_against_metadata(result, mass)
        self._check_extreme_values(result, mass)
        self._check_mass_change(result, mass)

    def _check_missing_values(self, result: ValidationResult) -> None:
        """Check for null or non-finite values in mass data."""
        mass_col = self.df["mass"]
        null_count = mass_col.null_count()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(f"Mass has {null_count} null values ({percentage:.1f}%)")
        non_finite = len(mass_col) - null_count - len(finite_values(mass_col))
        if non_finite > 0:
            result.add_warning(f"Mass has {non_finite} non-finite values")

    def _check_mass_against_metadata(
        self, result: ValidationResult, mass: np.ndarray
    ) -> None:
        """Check mass against sample mass from metadata."""
        # Typed as object: embedded metadata can carry anything at runtime.
        sample_mass: object = self.metadata.get("sample_mass")
        if sample_mass is None:
            result.add_info(
                "No sample mass in metadata - skipping mass loss validation"
            )
            return
        if not isinstance(sample_mass, (int, float)):
            result.add_warning(
                f"Sample mass in metadata is not numeric ({sample_mass!r}) - "
                "cannot validate mass loss"
            )
            return
        if sample_mass <= 0:
            result.add_warning(
                "Sample mass in metadata is zero or negative - cannot validate mass loss"
            )
            return

        mass_min = float(mass.min())

        # Calculate total mass loss (most negative value represents maximum loss)
        max_mass_loss = abs(mass_min) if mass_min < 0 else 0
        mass_loss_percentage = (max_mass_loss / sample_mass) * 100

        # Check if mass loss exceeds sample mass (with 10% tolerance)
        if max_mass_loss > sample_mass * 1.1:
            result.add_error(
                f"Mass loss ({max_mass_loss:.3f}mg) exceeds sample mass ({sample_mass:.3f}mg) by more than tolerance"
            )
        elif mass_loss_percentage > 100:
            result.add_warning(
                f"Mass loss ({mass_loss_percentage:.1f}%) appears to exceed sample mass"
            )
        else:
            result.add_pass(
                f"Mass loss ({mass_loss_percentage:.1f}%) is within expected range"
            )

    def _check_extreme_values(self, result: ValidationResult, mass: np.ndarray) -> None:
        """Check for extremely high maximum mass values."""
        mass_max = float(mass.max())
        if mass_max > 1000:  # More than 1g
            result.add_warning(f"Very high mass reading: {mass_max:.1f}mg")

    def _check_mass_change(self, result: ValidationResult, mass: np.ndarray) -> None:
        """Check mass change patterns."""
        mass_change = float(mass[-1]) - float(mass[0])

        if abs(mass_change) < 0.001:  # Less than 1 μg change
            result.add_info(f"Very small mass change: {mass_change:.3f}mg")
        elif mass_change > 5:  # Mass gain > 5mg (unusual)
            result.add_warning(f"Significant mass gain: {mass_change:.3f}mg")
        else:
            result.add_pass("Mass change is within reasonable range")
