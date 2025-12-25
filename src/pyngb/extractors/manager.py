"""
Metadata extraction using specialized extractors.

This module provides the main MetadataExtractor that orchestrates a collection
of focused extractors, improving maintainability and extensibility.
"""

import logging

from ..binary import BinaryParser
from ..constants import PatternConfig
from .base import BaseMetadataExtractor, ExtractorManager, FileMetadata
from .basic_fields import BasicFieldExtractor
from .mass import MassExtractor
from .specialized import (
    ApplicationLicenseExtractor,
    CalibrationExtractor,
    MFCExtractor,
    PIDParameterExtractor,
)
from .temperature import TemperatureProgramExtractor

__all__ = ["MetadataExtractor"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class MetadataExtractor(BaseMetadataExtractor):
    """Modular metadata extractor using specialized extractors.

    This extractor uses a composition of specialized extractors to handle
    different types of metadata extraction, improving maintainability,
    testability, and extensibility.

    The extraction order is optimized to:
    1. Extract basic fields first (fastest, most reliable)
    2. Extract mass fields (complex but foundational)
    3. Extract temperature program (requires combined data)
    4. Extract specialized fields (MFC, PID, calibration)
    5. Extract application/license info (lowest priority)

    Example:
        >>> config = PatternConfig()
        >>> parser = BinaryParser()
        >>> extractor = MetadataExtractor(config, parser)
        >>> metadata = extractor.extract_metadata([table_data])
        >>> print(len(metadata))  # Number of extracted fields
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Refactored Metadata Extractor")
        self.config = config
        self.parser = parser

        # Create the extractor manager
        self.manager = ExtractorManager()

        # Register extractors in optimal order
        self._register_extractors()

    def _register_extractors(self) -> None:
        """Register all specialized extractors with the manager."""
        # Basic fields first (fastest, most reliable)
        self.manager.register(BasicFieldExtractor(self.config, self.parser))

        # Mass fields second (complex but foundational for other extractors)
        self.manager.register(MassExtractor(self.config, self.parser))

        # Temperature program third (requires combined data)
        self.manager.register(TemperatureProgramExtractor(self.config, self.parser))

        # Specialized extractors fourth
        self.manager.register(MFCExtractor(self.config, self.parser))
        self.manager.register(PIDParameterExtractor(self.config, self.parser))
        self.manager.register(CalibrationExtractor(self.config, self.parser))

        # Application/license info last (lowest priority)
        self.manager.register(ApplicationLicenseExtractor(self.config, self.parser))

        self.logger.debug(
            f"Registered {len(self.manager.extractors)} specialized extractors"
        )

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if any metadata can be extracted from the tables.

        Args:
            tables: List of binary table data

        Returns:
            True if at least one extractor can process the tables
        """
        if not tables:
            return False

        # Check if any extractor can handle the tables
        for extractor in self.manager.extractors:
            if extractor.can_extract(tables):
                return True

        return False

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract metadata using all registered extractors.

        Args:
            tables: List of binary table data from NGB streams
            metadata: Metadata dictionary to update (modified in-place)
        """
        # Delegate to the manager
        extracted_metadata = self.manager.extract_all(tables)

        # Update the provided metadata dictionary
        metadata.update(extracted_metadata)

    def extract_metadata(self, tables: list[bytes]) -> FileMetadata:
        """Extract all metadata from tables.

        Args:
            tables: List of binary table data from NGB streams

        Returns:
            FileMetadata dictionary with extracted fields
        """
        self.log_extraction_attempt(len(tables))

        # Use the manager to extract all metadata
        metadata = self.manager.extract_all(tables)

        field_count = len(metadata)
        if field_count > 0:
            self.log_extraction_success(field_count)
        else:
            self.logger.warning("No metadata fields extracted")

        return metadata
