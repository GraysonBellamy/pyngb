"""Metadata validation for STA data."""

from ..constants import FileMetadata
from .base import ValidationResult


class MetadataValidator:
    """Validates file metadata consistency and completeness."""

    def __init__(self, metadata: FileMetadata | None = None) -> None:
        """Initialize metadata validator.

        Args:
            metadata: Optional file metadata
        """
        self.metadata = metadata or {}

    def validate(self, result: ValidationResult) -> None:
        """Perform metadata validation.

        Args:
            result: ValidationResult to store findings
        """
        if not self.metadata:
            return

        self._check_required_fields(result)

    def _check_required_fields(self, result: ValidationResult) -> None:
        """Check for required metadata fields."""
        required_metadata = ["instrument", "sample_name", "operator"]
        missing_metadata = [
            field for field in required_metadata if not self.metadata.get(field)
        ]

        if missing_metadata:
            result.add_warning(f"Missing metadata fields: {missing_metadata}")
        else:
            result.add_pass("Essential metadata fields present")
