"""
Data validation and quality checking tools for STA data.
"""

from __future__ import annotations

import logging
from typing import Union

import numpy as np
import polars as pl
import pyarrow as pa

from .constants import FileMetadata

__all__ = [
    "QualityChecker",
    "ValidationResult",
    "check_dsc_data",
    "check_mass_data",
    "check_temperature_profile",
    "validate_sta_data",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _ensure_polars_dataframe(data: Union[pa.Table, pl.DataFrame]) -> pl.DataFrame:
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


class ValidationResult:
    """Container for validation results.

    Stores validation issues, warnings, and overall status.
    """

    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []
        self.passed_checks: list[str] = []

    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Validation error: {message}")

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")

    def add_info(self, message: str) -> None:
        """Add an info message."""
        self.info.append(message)
        logger.info(f"Validation info: {message}")

    def add_pass(self, check_name: str) -> None:
        """Mark a check as passed."""
        self.passed_checks.append(check_name)

    @property
    def is_valid(self) -> bool:
        """Return True if no errors were found."""
        return len(self.errors) == 0

    @property
    def has_warnings(self) -> bool:
        """Return True if warnings were found."""
        return len(self.warnings) > 0

    def summary(self) -> dict[str, int | bool]:
        """Get validation summary."""
        return {
            "is_valid": self.is_valid,
            "has_warnings": self.has_warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "checks_passed": len(self.passed_checks),
            "total_issues": len(self.errors) + len(self.warnings),
        }

    def report(self) -> str:
        """Generate a formatted validation report."""
        lines = ["=== STA Data Validation Report ===\n"]

        # Summary
        summary = self.summary()
        status = "✅ VALID" if summary["is_valid"] else "❌ INVALID"
        lines.append(f"Overall Status: {status}")
        lines.append(f"Checks Passed: {summary['checks_passed']}")
        lines.append(f"Errors: {summary['error_count']}")
        lines.append(f"Warnings: {summary['warning_count']}\n")

        # Errors
        if self.errors:
            lines.append("🔴 ERRORS:")
            for error in self.errors:
                lines.append(f"  • {error}")
            lines.append("")

        # Warnings
        if self.warnings:
            lines.append("🟡 WARNINGS:")
            for warning in self.warnings:
                lines.append(f"  • {warning}")
            lines.append("")

        # Info
        if self.info:
            lines.append("INFO:")
            for info in self.info:
                lines.append(f"  • {info}")
            lines.append("")

        return "\n".join(lines)


class QualityChecker:
    """Comprehensive quality checking for STA data.

    Performs various validation checks on STA data including:
    - Data completeness and structure
    - Physical validity of measurements
    - Temperature profile analysis
    - Statistical outlier detection
    - Metadata consistency

    Examples:
    >>> from pyngb import read_ngb
    >>> from pyngb.validation import QualityChecker
        >>>
        >>> table = read_ngb("sample.ngb-ss3")
        >>> checker = QualityChecker(table)
        >>> result = checker.full_validation()
        >>>
        >>> if not result.is_valid:
        ...     print("Data validation failed!")
        ...     print(result.report())
        >>>
        >>> # Quick validation
        >>> issues = checker.quick_check()
        >>> print(f"Found {len(issues)} issues")
    """

    df: pl.DataFrame
    metadata: FileMetadata
    result: ValidationResult

    def __init__(
        self, data: Union[pa.Table, pl.DataFrame], metadata: FileMetadata | None = None
    ):
        """Initialize quality checker.

        Args:
            data: STA data table or dataframe
            metadata: Optional metadata dictionary
        """
        if isinstance(data, pa.Table):
            df_temp = pl.from_arrow(data)
            # Ensure we have a DataFrame, not a Series
            self.df = (
                df_temp if isinstance(df_temp, pl.DataFrame) else df_temp.to_frame()
            )
            # Try to extract metadata from table
            if metadata is None:
                try:
                    if data.schema.metadata:  # type: ignore[attr-defined]
                        metadata = self._extract_metadata_from_table(data)
                except (AttributeError, KeyError):
                    # Schema has no metadata or metadata is not accessible
                    pass
        else:
            self.df = data

        self.metadata = metadata or {}
        self.result = ValidationResult()

    def _extract_metadata_from_table(self, table: pa.Table) -> FileMetadata:
        """Extract metadata from PyArrow table."""
        import json

        if b"file_metadata" in table.schema.metadata:
            metadata_json = table.schema.metadata[b"file_metadata"].decode()
            metadata: FileMetadata = json.loads(metadata_json)
            return metadata
        return {}

    def full_validation(self) -> ValidationResult:
        """Perform comprehensive validation of STA data.

        Returns:
            ValidationResult with detailed findings
        """
        self.result = ValidationResult()

        # Basic structure checks
        self._check_data_structure()

        # Column-specific checks
        if "sample_temperature" in self.df.columns:
            self._check_temperature_data()

        if "time" in self.df.columns:
            self._check_time_data()

        if "mass" in self.df.columns:
            self._check_mass_data()

        if "dsc_signal" in self.df.columns:
            self._check_dsc_data()

        # Cross-column consistency checks
        self._check_data_consistency()

        # Metadata validation
        if self.metadata:
            self._check_metadata_consistency()

        # Statistical checks
        self._check_statistical_properties()

        return self.result

    def quick_check(self) -> list[str]:
        """Perform quick validation and return list of issues.

        Returns:
            List of issue descriptions
        """
        issues = []

        # Check for required columns
        required_cols = ["time", "sample_temperature"]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")

        # Check for empty data
        if self.df.height == 0:
            issues.append("Dataset is empty")
            return issues

        # Check for null values
        null_counts = self.df.null_count()
        for row in null_counts.iter_rows(named=True):
            for col, count in row.items():
                if count > 0:
                    percentage = (count / self.df.height) * 100
                    issues.append(
                        f"Column '{col}' has {count} null values ({percentage:.1f}%)"
                    )

        # Quick temperature check
        if "sample_temperature" in self.df.columns:
            temp_stats = self.df.select("sample_temperature").describe()
            temp_min = temp_stats.filter(pl.col("statistic") == "min")[
                "sample_temperature"
            ][0]
            temp_max = temp_stats.filter(pl.col("statistic") == "max")[
                "sample_temperature"
            ][0]

            if temp_min == temp_max:
                issues.append("Temperature is constant (no heating/cooling)")
            elif temp_min < -50 or temp_max > 2000:
                issues.append(
                    f"Unusual temperature range: {temp_min:.1f} to {temp_max:.1f}°C"
                )

        return issues

    def _check_data_structure(self) -> None:
        """Check basic data structure."""
        # Check if data exists
        if self.df.height == 0:
            self.result.add_error("Dataset is empty")
            return

        # Check for required columns
        required_cols = ["time", "sample_temperature"]
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            self.result.add_error(f"Missing required columns: {missing_cols}")
        else:
            self.result.add_pass("Required columns present")

        # Check data types
        schema_info = []
        for col, dtype in zip(self.df.columns, self.df.dtypes):
            schema_info.append(f"{col}: {dtype}")
        self.result.add_info(f"Data schema: {', '.join(schema_info)}")

        # Check for duplicate rows
        duplicate_count = self.df.height - self.df.unique().height
        if duplicate_count > 0:
            self.result.add_warning(f"Found {duplicate_count} duplicate rows")
        else:
            self.result.add_pass("No duplicate rows")

    def _check_temperature_data(self) -> None:
        """Validate temperature measurements."""
        temp_col = self.df.select("sample_temperature")

        # Check for null values
        null_count = temp_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            self.result.add_warning(
                f"Temperature has {null_count} null values ({percentage:.1f}%)"
            )

        # Get temperature statistics
        temp_stats = temp_col.describe()
        temp_min = temp_stats.filter(pl.col("statistic") == "min")[
            "sample_temperature"
        ][0]
        temp_max = temp_stats.filter(pl.col("statistic") == "max")[
            "sample_temperature"
        ][0]

        # Check temperature range
        if temp_min == temp_max:
            self.result.add_error("Temperature is constant throughout experiment")
        elif temp_max - temp_min < 10:
            self.result.add_warning(
                f"Small temperature range: {temp_max - temp_min:.1f}°C"
            )
        else:
            self.result.add_pass("Temperature range is reasonable")

        # Check for physically realistic temperatures
        if temp_min < -273:  # Below absolute zero
            self.result.add_error(f"Temperature below absolute zero: {temp_min:.1f}°C")
        elif temp_min < -50:
            self.result.add_warning(f"Very low minimum temperature: {temp_min:.1f}°C")

        if temp_max > 2000:
            self.result.add_warning(f"Very high maximum temperature: {temp_max:.1f}°C")

        # Check for temperature profile monotonicity
        temp_data = temp_col.to_numpy().flatten()
        temp_diff = np.diff(temp_data)

        if np.all(temp_diff >= 0):
            self.result.add_info(
                "Temperature profile is monotonically increasing (heating)"
            )
        elif np.all(temp_diff <= 0):
            self.result.add_info(
                "Temperature profile is monotonically decreasing (cooling)"
            )
        else:
            # Mixed heating/cooling
            heating_points: int = int(np.sum(temp_diff > 0))
            cooling_points: int = int(np.sum(temp_diff < 0))
            self.result.add_info(
                f"Mixed temperature profile: {heating_points} heating, {cooling_points} cooling points"
            )

    def _check_time_data(self) -> None:
        """Validate time measurements."""
        time_col = self.df.select("time")

        # Check for null values
        null_count = time_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            self.result.add_warning(
                f"Time has {null_count} null values ({percentage:.1f}%)"
            )

        # Check time progression
        time_data = time_col.to_numpy().flatten()
        time_diff = np.diff(time_data)

        if np.all(time_diff >= 0):
            self.result.add_pass("Time progresses monotonically")
        else:
            backwards_count: int = int(np.sum(time_diff < 0))
            self.result.add_error(f"Time goes backwards {backwards_count} times")

        # Check for reasonable time intervals
        if len(time_diff) > 0:
            positive_intervals = time_diff[time_diff > 0]
            if len(positive_intervals) > 0:
                avg_interval = np.mean(positive_intervals)
                if avg_interval < 0.1:  # Less than 0.1 second intervals
                    self.result.add_info(
                        f"Very high time resolution: {avg_interval:.3f}s average interval"
                    )
                elif avg_interval > 60:  # More than 1 minute intervals
                    self.result.add_warning(
                        f"Low time resolution: {avg_interval:.1f}s average interval"
                    )

    def _check_mass_data(self) -> None:
        """Validate mass measurements."""
        mass_col = self.df.select("mass")

        # Check for null values
        null_count = mass_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            self.result.add_warning(
                f"Mass has {null_count} null values ({percentage:.1f}%)"
            )

        # Get mass statistics
        mass_stats = mass_col.describe()
        mass_min = mass_stats.filter(pl.col("statistic") == "min")["mass"][0]
        mass_max = mass_stats.filter(pl.col("statistic") == "max")["mass"][0]

        # Check mass against sample mass from metadata if available
        if (
            hasattr(self, "metadata")
            and self.metadata
            and "sample_mass" in self.metadata
        ):
            sample_mass = self.metadata["sample_mass"]

            # Calculate total mass loss (most negative value represents maximum loss)
            max_mass_loss = abs(mass_min) if mass_min < 0 else 0

            if sample_mass > 0:
                mass_loss_percentage = (max_mass_loss / sample_mass) * 100

                # Check if mass loss exceeds sample mass (with 10% tolerance for measurement uncertainty)
                if max_mass_loss > sample_mass * 1.1:
                    self.result.add_error(
                        f"Mass loss ({max_mass_loss:.3f}mg) exceeds sample mass ({sample_mass:.3f}mg) by more than tolerance"
                    )
                elif mass_loss_percentage > 100:
                    self.result.add_warning(
                        f"Mass loss ({mass_loss_percentage:.1f}%) appears to exceed sample mass"
                    )
                else:
                    self.result.add_pass(
                        f"Mass loss ({mass_loss_percentage:.1f}%) is within expected range"
                    )
            else:
                self.result.add_warning(
                    "Sample mass in metadata is zero or negative - cannot validate mass loss"
                )
        else:
            self.result.add_info(
                "No sample mass in metadata - skipping mass loss validation"
            )

        # Check for extremely high maximum mass values (instrument limits)
        if mass_max > 1000:  # More than 1g
            self.result.add_warning(f"Very high mass reading: {mass_max:.1f}mg")

        # Check mass loss/gain
        initial_mass = mass_col[0, 0]
        final_mass = mass_col[-1, 0]

        # For thermal analysis, initial mass is typically zeroed, so calculate relative to that zero point
        # Check for reasonable mass change patterns
        mass_change = final_mass - initial_mass

        if abs(mass_change) < 0.001:  # Less than 1 μg change
            self.result.add_info(f"Very small mass change: {mass_change:.3f}mg")
        elif mass_change > 5:  # Mass gain > 5mg (unusual)
            self.result.add_warning(f"Significant mass gain: {mass_change:.3f}mg")
        else:
            self.result.add_pass("Mass change is within reasonable range")

    def _check_dsc_data(self) -> None:
        """Validate DSC measurements."""
        dsc_col = self.df.select("dsc_signal")

        # Check for null values
        null_count = dsc_col.null_count().item()
        if null_count > 0:
            percentage = (null_count / self.df.height) * 100
            self.result.add_warning(
                f"DSC has {null_count} null values ({percentage:.1f}%)"
            )

        # Get DSC statistics
        dsc_stats = dsc_col.describe()
        dsc_min = dsc_stats.filter(pl.col("statistic") == "min")["dsc_signal"][0]
        dsc_max = dsc_stats.filter(pl.col("statistic") == "max")["dsc_signal"][0]
        dsc_std = dsc_stats.filter(pl.col("statistic") == "std")["dsc_signal"][0]

        # Check for constant DSC signal (no thermal events)
        if dsc_std < 0.001:
            self.result.add_warning(
                "DSC signal is nearly constant - no thermal events detected"
            )
        else:
            self.result.add_pass("DSC signal shows variation")

        # Check for extreme values
        if abs(dsc_max) > 1000 or abs(dsc_min) > 1000:
            self.result.add_warning(
                f"Extreme DSC values detected: {dsc_min:.1f} to {dsc_max:.1f} μV"
            )

    def _check_data_consistency(self) -> None:
        """Check consistency between different measurements."""
        # Check if all columns have the same length (should be guaranteed by DataFrame)
        self.result.add_pass("All columns have consistent length")

        # Check for synchronized time/temperature if both present
        if "time" in self.df.columns and "sample_temperature" in self.df.columns:
            # Check if temperature changes correlate with time
            time_data = self.df.select("time").to_numpy().flatten()
            temp_data = self.df.select("sample_temperature").to_numpy().flatten()

            # Simple correlation check
            if len(time_data) > 1 and len(temp_data) > 1:
                correlation = np.corrcoef(time_data, temp_data)[0, 1]
                if abs(correlation) > 0.8:
                    self.result.add_pass(
                        f"Time and temperature are well correlated (r={correlation:.3f})"
                    )
                else:
                    self.result.add_info(
                        f"Time and temperature correlation: r={correlation:.3f}"
                    )

    def _check_metadata_consistency(self) -> None:
        """Check metadata for consistency and completeness."""
        required_metadata = ["instrument", "sample_name", "operator"]
        missing_metadata = [
            field for field in required_metadata if not self.metadata.get(field)
        ]

        if missing_metadata:
            self.result.add_warning(f"Missing metadata fields: {missing_metadata}")
        else:
            self.result.add_pass("Essential metadata fields present")

    def _check_statistical_properties(self) -> None:
        """Check statistical properties for anomalies."""
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
                        self.result.add_warning(
                            f"Column '{col}' has {outliers} outliers ({outlier_percentage:.1f}%)"
                        )


def validate_sta_data(
    data: Union[pa.Table, pl.DataFrame], metadata: FileMetadata | None = None
) -> list[str]:
    """Quick validation function that returns a list of issues.

    Convenience function for basic validation without detailed reporting.

    Args:
        data: STA data table or dataframe
        metadata: Optional metadata dictionary

    Returns:
        List of validation issues found

    Examples:
    >>> from pyngb import read_ngb
    >>> from pyngb.validation import validate_sta_data
        >>>
        >>> table = read_ngb("sample.ngb-ss3")
        >>> issues = validate_sta_data(table)
        >>>
        >>> if issues:
        ...     print("Validation issues found:")
        ...     for issue in issues:
        ...         print(f"  - {issue}")
        ... else:
        ...     print("Data validation passed!")
    """
    checker = QualityChecker(data, metadata)
    return checker.quick_check()


# Specific validation functions
def check_temperature_profile(
    data: Union[pa.Table, pl.DataFrame],
) -> dict[str, str | float | bool]:
    """Check temperature profile for common issues.

    Args:
        data: STA data

    Returns:
        Dictionary with temperature profile analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "sample_temperature" not in df.columns:
        return {"error": "No sample_temperature column found"}

    temp_data = df.select("sample_temperature").to_numpy().flatten()

    # Handle NaN and infinite values
    temp_data_clean = temp_data[~np.isnan(temp_data) & ~np.isinf(temp_data)]

    if len(temp_data_clean) == 0:
        return {"error": "No valid temperature data (all NaN or infinite)"}

    analysis: dict[str, str | float | bool] = {
        "temperature_range": float(np.ptp(temp_data_clean)),
        "min_temperature": float(np.min(temp_data_clean)),
        "max_temperature": float(np.max(temp_data_clean)),
        "is_monotonic_increasing": bool(np.all(np.diff(temp_data_clean) >= 0)),
        "is_monotonic_decreasing": bool(np.all(np.diff(temp_data_clean) <= 0)),
        "average_rate": float(np.mean(np.diff(temp_data_clean)))
        if len(temp_data_clean) > 1
        else 0.0,
    }

    return analysis


def check_mass_data(
    data: Union[pa.Table, pl.DataFrame],
) -> dict[str, str | float | bool]:
    """Check mass data for quality issues.

    Args:
        data: STA data

    Returns:
        Dictionary with mass data analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "mass" not in df.columns:
        return {"error": "No mass column found"}

    mass_data = df.select("mass").to_numpy().flatten()

    # Handle NaN and infinite values
    mass_data_clean = mass_data[~np.isnan(mass_data) & ~np.isinf(mass_data)]

    if len(mass_data_clean) == 0:
        return {"error": "No valid mass data (all NaN or infinite)"}

    initial_mass = mass_data_clean[0]
    final_mass = mass_data_clean[-1]

    # For thermal analysis, mass change is measured from the zeroed starting point
    mass_change = final_mass - initial_mass

    analysis: dict[str, str | float | bool] = {
        "initial_mass": float(initial_mass),
        "final_mass": float(final_mass),
        "mass_change": float(mass_change),
        "mass_range": float(np.ptp(mass_data_clean)),
        "has_negative_values": bool(np.any(mass_data_clean < 0)),
    }

    return analysis


def check_dsc_data(data: Union[pa.Table, pl.DataFrame]) -> dict[str, str | float | int]:
    """Check DSC data for quality issues.

    Args:
        data: STA data

    Returns:
        Dictionary with DSC data analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "dsc_signal" not in df.columns:
        return {"error": "No dsc_signal column found"}

    dsc_data = df.select("dsc_signal").to_numpy().flatten()

    # Handle NaN and infinite values
    dsc_data_clean = dsc_data[~np.isnan(dsc_data) & ~np.isinf(dsc_data)]

    if len(dsc_data_clean) == 0:
        return {"error": "No valid DSC data (all NaN or infinite)"}

    # Simple peak detection
    peaks_positive = 0
    peaks_negative = 0

    for i in range(1, len(dsc_data_clean) - 1):
        if (
            dsc_data_clean[i] > dsc_data_clean[i - 1]
            and dsc_data_clean[i] > dsc_data_clean[i + 1]
        ):
            if dsc_data_clean[i] > np.std(dsc_data_clean):  # Significant peak
                peaks_positive += 1
        elif (
            dsc_data_clean[i] < dsc_data_clean[i - 1]
            and dsc_data_clean[i] < dsc_data_clean[i + 1]
            and abs(dsc_data_clean[i]) > np.std(dsc_data_clean)
        ):  # Significant through
            peaks_negative += 1

    analysis: dict[str, str | float | int] = {
        "signal_range": float(np.ptp(dsc_data_clean)),
        "signal_std": float(np.std(dsc_data_clean)),
        "peaks_detected": int(peaks_positive + peaks_negative),
        "positive_peaks": int(peaks_positive),
        "negative_peaks": int(peaks_negative),
        "signal_to_noise": float(
            np.max(np.abs(dsc_data_clean)) / np.std(dsc_data_clean)
        )
        if np.std(dsc_data_clean) > 0
        else 0.0,
    }

    return analysis
