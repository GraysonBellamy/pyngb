"""Data structure validation for STA data."""

import polars as pl

from .base import ValidationResult


class StructureValidator:
    """Validates data structure and completeness."""

    def __init__(self, df: pl.DataFrame) -> None:
        """Initialize structure validator.

        Args:
            df: Polars DataFrame to validate
        """
        self.df = df

    def validate(self, result: ValidationResult) -> None:
        """Perform structure validation.

        Args:
            result: ValidationResult to store findings
        """
        self._check_data_existence(result)
        if self.df.height > 0:  # Only continue if data exists
            self._check_required_columns(result)
            self._check_data_types(result)
            self._check_duplicates(result)

    def _check_data_existence(self, result: ValidationResult) -> None:
        """Check if data exists."""
        if self.df.height == 0:
            result.add_error("Dataset is empty")

    def _check_required_columns(self, result: ValidationResult) -> None:
        """Check for required columns."""
        required_cols = ["time", "sample_temperature"]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            result.add_error(f"Missing required columns: {missing_cols}")
        else:
            result.add_pass("Required columns present")

    def _check_data_types(self, result: ValidationResult) -> None:
        """Check and report data types."""
        schema_info = []
        for col, dtype in zip(self.df.columns, self.df.dtypes):
            schema_info.append(f"{col}: {dtype}")
        result.add_info(f"Data schema: {', '.join(schema_info)}")

    def _check_duplicates(self, result: ValidationResult) -> None:
        """Check for duplicate rows."""
        duplicate_count = self.df.height - self.df.unique().height
        if duplicate_count > 0:
            result.add_warning(f"Found {duplicate_count} duplicate rows")
        else:
            result.add_pass("No duplicate rows")
