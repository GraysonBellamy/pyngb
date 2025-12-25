"""Quality checker for STA data - orchestrates validators."""

import json

import polars as pl
import pyarrow as pa

from ..constants import FileMetadata
from .base import ValidationResult
from .consistency import ConsistencyValidator
from .dsc import DSCValidator
from .mass import MassValidator
from .metadata_validator import MetadataValidator
from .statistical import StatisticalValidator
from .structure import StructureValidator
from .temperature import TemperatureValidator
from .time import TimeValidator


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
        self, data: pa.Table | pl.DataFrame, metadata: FileMetadata | None = None
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
                    if data.schema.metadata:
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

        # Create validators
        structure_validator = StructureValidator(self.df)
        temperature_validator = TemperatureValidator(self.df)
        time_validator = TimeValidator(self.df)
        mass_validator = MassValidator(self.df, self.metadata)
        dsc_validator = DSCValidator(self.df)
        consistency_validator = ConsistencyValidator(self.df)
        metadata_validator = MetadataValidator(self.metadata)
        statistical_validator = StatisticalValidator(self.df)

        # Run validators
        structure_validator.validate(self.result)
        temperature_validator.validate(self.result)
        time_validator.validate(self.result)
        mass_validator.validate(self.result)
        dsc_validator.validate(self.result)
        consistency_validator.validate(self.result)
        metadata_validator.validate(self.result)
        statistical_validator.validate(self.result)

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
