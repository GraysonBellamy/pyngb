"""Mass data validation for STA data."""

import polars as pl

from ..constants import FileMetadata
from .base import ValidationResult


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

        self._check_null_values(result)
        self._check_mass_against_metadata(result)
        self._check_extreme_values(result)
        self._check_mass_change(result)

    def _check_null_values(self, result: ValidationResult) -> None:
        """Check for null values in mass data."""
        mass_col = self.df.select("mass")
        null_count = mass_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            result.add_warning(f"Mass has {null_count} null values ({percentage:.1f}%)")

    def _check_mass_against_metadata(self, result: ValidationResult) -> None:
        """Check mass against sample mass from metadata."""
        mass_col = self.df.select("mass")
        mass_stats = mass_col.describe()
        mass_min = mass_stats.filter(pl.col("statistic") == "min")["mass"][0]

        if self.metadata and "sample_mass" in self.metadata:
            sample_mass = self.metadata["sample_mass"]

            # Calculate total mass loss (most negative value represents maximum loss)
            max_mass_loss = abs(mass_min) if mass_min < 0 else 0

            if sample_mass > 0:
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
            else:
                result.add_warning(
                    "Sample mass in metadata is zero or negative - cannot validate mass loss"
                )
        else:
            result.add_info(
                "No sample mass in metadata - skipping mass loss validation"
            )

    def _check_extreme_values(self, result: ValidationResult) -> None:
        """Check for extremely high maximum mass values."""
        mass_col = self.df.select("mass")
        mass_stats = mass_col.describe()
        mass_max = mass_stats.filter(pl.col("statistic") == "max")["mass"][0]

        if mass_max > 1000:  # More than 1g
            result.add_warning(f"Very high mass reading: {mass_max:.1f}mg")

    def _check_mass_change(self, result: ValidationResult) -> None:
        """Check mass change patterns."""
        mass_col = self.df.select("mass")
        initial_mass = mass_col[0, 0]
        final_mass = mass_col[-1, 0]

        mass_change = final_mass - initial_mass

        if abs(mass_change) < 0.001:  # Less than 1 μg change
            result.add_info(f"Very small mass change: {mass_change:.3f}mg")
        elif mass_change > 5:  # Mass gain > 5mg (unusual)
            result.add_warning(f"Significant mass gain: {mass_change:.3f}mg")
        else:
            result.add_pass("Mass change is within reasonable range")
