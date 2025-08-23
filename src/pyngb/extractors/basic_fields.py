"""
Basic field extractor for simple NGB metadata fields.

This extractor handles straightforward metadata fields that follow standard
patterns without requiring complex structural parsing logic.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, ClassVar

from ..binary import BinaryParser
from ..constants import PatternConfig
from .base import BaseMetadataExtractor, FileMetadata

__all__ = ["BasicFieldExtractor"]


class BasicFieldExtractor(BaseMetadataExtractor):
    """Extracts basic metadata fields using standard patterns.

    This extractor handles simple fields like instrument, project, date_performed,
    lab, operator, crucible_type, comment, furnace_type, carrier_type, sample_id,
    sample_name, and material that follow consistent binary patterns.

    Fields that require special handling (like masses, temperature programs, etc.)
    are handled by specialized extractors.

    Example:
        >>> config = PatternConfig()
        >>> parser = BinaryParser()
        >>> extractor = BasicFieldExtractor(config, parser)
        >>> metadata = {}
        >>> extractor.extract([table_data], metadata)
        >>> print(metadata['instrument'])  # 'NETZSCH STA 449 F3 Jupiter'
    """

    # Fields handled by this extractor (excluding complex ones)
    BASIC_FIELDS: ClassVar[list[str]] = [
        "instrument",
        "project",
        "date_performed",
        "lab",
        "operator",
        "crucible_type",
        "comment",
        "furnace_type",
        "carrier_type",
        "sample_id",
        "sample_name",
        "material",
    ]

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Basic Fields")
        self.config = config
        self.parser = parser
        self._compiled_patterns: dict[str, re.Pattern[bytes]] = {}

        # Compile patterns for basic fields only
        self._compile_basic_patterns()

    def _compile_basic_patterns(self) -> None:
        """Compile regex patterns for basic metadata fields."""
        END_FIELD = self.parser.markers.END_FIELD
        TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
        TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR

        for field_name in self.BASIC_FIELDS:
            if field_name in self.config.metadata_patterns:
                category, field_bytes = self.config.metadata_patterns[field_name]
                pattern = (
                    category
                    + rb".+?"
                    + field_bytes
                    + rb".+?"
                    + TYPE_PREFIX
                    + rb"(.+?)"
                    + TYPE_SEPARATOR
                    + rb"(.+?)"
                    + END_FIELD
                )
                self._compiled_patterns[field_name] = re.compile(pattern, re.DOTALL)

        self.logger.debug(
            f"Compiled {len(self._compiled_patterns)} basic field patterns"
        )

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if any basic fields can be extracted from the tables.

        Args:
            tables: List of binary table data

        Returns:
            True if at least one basic field pattern might be found
        """
        # Basic field extractor should always try to extract from non-empty tables
        # as it handles common metadata fields that are likely present
        return bool(tables)

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract basic metadata fields from tables.

        Args:
            tables: List of binary table data from NGB streams
            metadata: Metadata dictionary to update (modified in-place)
        """
        self.log_extraction_attempt(len(tables))

        extracted_count = 0

        for table in tables:
            for field_name, pattern in self._compiled_patterns.items():
                # Skip if field already extracted
                if field_name in metadata:
                    continue

                try:
                    matches = pattern.findall(table)
                    if matches:
                        # Take the first match for basic fields
                        data_type, value_bytes = matches[0]
                        value = self.parser.parse_value(data_type, value_bytes)

                        if value is not None:
                            # Special handling for specific fields
                            processed_value = self._process_field_value(
                                field_name, value
                            )
                            if processed_value is not None:
                                metadata[field_name] = processed_value  # type: ignore
                                extracted_count += 1
                                self.logger.debug(
                                    f"Extracted {field_name}: {processed_value}"
                                )

                except Exception as e:
                    self.logger.warning(f"Failed to extract {field_name}: {e}")
                    continue

        if extracted_count > 0:
            self.log_extraction_success(extracted_count)
        else:
            self.logger.debug("No basic fields extracted")

    def _process_field_value(self, field_name: str, value: Any) -> Any:
        """Process field values with any necessary transformations.

        Args:
            field_name: Name of the field being processed
            value: Raw extracted value

        Returns:
            Processed value ready for metadata dictionary
        """
        if field_name == "date_performed" and isinstance(value, int):
            # Convert Unix timestamp to ISO format
            try:
                return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
            except (ValueError, OSError) as e:
                self.logger.warning(
                    f"Invalid timestamp for date_performed: {value}, {e}"
                )
                return None

        # For string fields, ensure we have a clean string
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None

        return value

    def extract_single_field(self, tables: list[bytes], field_name: str) -> Any | None:
        """Extract a specific basic field from tables.

        This is a utility method for extracting individual fields when needed.

        Args:
            tables: List of binary table data
            field_name: Name of the field to extract

        Returns:
            Extracted field value or None if not found

        Raises:
            ValueError: If field_name is not a basic field
        """
        if field_name not in self.BASIC_FIELDS:
            raise ValueError(f"Field {field_name} is not a basic field")

        if field_name not in self._compiled_patterns:
            self.logger.warning(f"No pattern available for field {field_name}")
            return None

        pattern = self._compiled_patterns[field_name]

        for table in tables:
            try:
                matches = pattern.findall(table)
                if matches:
                    data_type, value_bytes = matches[0]
                    value = self.parser.parse_value(data_type, value_bytes)
                    return self._process_field_value(field_name, value)
            except Exception as e:
                self.logger.debug(f"Error extracting {field_name} from table: {e}")
                continue

        return None
