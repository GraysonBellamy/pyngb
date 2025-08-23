"""
Base classes and protocols for metadata extraction.

This module provides the foundational interfaces and base classes used
by specialized metadata extractors throughout the pyngb package.
"""

from __future__ import annotations

import logging
from typing import Protocol


from ..constants import FileMetadata


__all__ = [
    "BaseMetadataExtractor",
    "ExtractorManager",
    "MetadataExtractorProtocol",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class MetadataExtractorProtocol(Protocol):
    """Protocol defining the interface for metadata extractors.

    All metadata extractors must implement this protocol to ensure
    consistent behavior and enable composition through the ExtractorManager.

    Example:
        >>> class CustomExtractor:
        ...     def can_extract(self, tables: list[bytes]) -> bool:
        ...         return b"custom_marker" in tables[0] if tables else False
        ...
        ...     def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        ...         if self.can_extract(tables):
        ...             metadata["custom_field"] = "extracted_value"
    """

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if this extractor can process the given tables.

        Args:
            tables: List of binary table data from NGB streams

        Returns:
            True if the extractor can extract metadata from these tables
        """
        ...

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract metadata from tables and update the metadata dictionary.

        Args:
            tables: List of binary table data from NGB streams
            metadata: Metadata dictionary to update (modified in-place)

        Note:
            This method should modify the metadata dictionary in-place.
            It should not return a new metadata dictionary.
        """
        ...

    @property
    def name(self) -> str:
        """Return the name of this extractor for logging/debugging."""
        ...


class BaseMetadataExtractor:
    """Base class providing common functionality for metadata extractors.

    This class provides shared infrastructure like logging, error handling,
    and common utilities that most extractors need.

    Attributes:
        name: Human-readable name of the extractor
        logger: Logger instance for this extractor

    Example:
        >>> class TemperatureExtractor(BaseMetadataExtractor):
        ...     def __init__(self):
        ...         super().__init__("Temperature Program")
        ...
        ...     def can_extract(self, tables: list[bytes]) -> bool:
        ...         return any(b"temperature" in table for table in tables)
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self.logger = logging.getLogger(f"{__name__}.{name.replace(' ', '')}")
        self.logger.addHandler(logging.NullHandler())

    @property
    def name(self) -> str:
        """Return the name of this extractor."""
        return self._name

    def log_extraction_attempt(self, tables_count: int) -> None:
        """Log that extraction is being attempted."""
        self.logger.debug(f"Attempting extraction from {tables_count} tables")

    def log_extraction_success(self, field_count: int) -> None:
        """Log successful extraction."""
        self.logger.debug(f"Successfully extracted {field_count} fields")

    def log_extraction_failure(self, error: Exception) -> None:
        """Log extraction failure."""
        self.logger.warning(f"Extraction failed: {error}")


class ExtractorManager:
    """Orchestrates multiple metadata extractors.

    The ExtractorManager coordinates the execution of multiple specialized
    extractors, ensuring they run in the correct order and handling any
    errors that occur during extraction.

    Attributes:
        extractors: List of registered metadata extractors

    Example:
        >>> from ..binary import BinaryParser
        >>> from ..constants import PatternConfig
        >>>
        >>> manager = ExtractorManager()
        >>> manager.register(BasicFieldExtractor(config, parser))
        >>> manager.register(MassExtractor(config, parser))
        >>>
        >>> metadata = manager.extract_all(tables)
    """

    def __init__(self) -> None:
        self.extractors: list[MetadataExtractorProtocol] = []
        self.logger = logging.getLogger(f"{__name__}.ExtractorManager")
        self.logger.addHandler(logging.NullHandler())

    def register(self, extractor: MetadataExtractorProtocol) -> None:
        """Register a metadata extractor.

        Args:
            extractor: The extractor to register
        """
        self.extractors.append(extractor)
        self.logger.debug(f"Registered extractor: {extractor.name}")

    def extract_all(self, tables: list[bytes]) -> FileMetadata:
        """Extract metadata using all registered extractors.

        Args:
            tables: List of binary table data from NGB streams

        Returns:
            FileMetadata dictionary with extracted fields

        Note:
            Extractors are run in registration order. Later extractors
            can override fields set by earlier ones.
        """
        metadata: FileMetadata = {}

        self.logger.debug(f"Starting extraction with {len(self.extractors)} extractors")

        for extractor in self.extractors:
            try:
                if extractor.can_extract(tables):
                    self.logger.debug(f"Running extractor: {extractor.name}")
                    initial_count = len(metadata)
                    extractor.extract(tables, metadata)
                    final_count = len(metadata)
                    self.logger.debug(
                        f"Extractor {extractor.name} added {final_count - initial_count} fields"
                    )
                else:
                    self.logger.debug(
                        f"Skipping extractor {extractor.name} - cannot extract"
                    )
            except Exception as e:
                self.logger.error(f"Extractor {extractor.name} failed: {e}")
                # Continue with other extractors rather than failing completely
                continue

        self.logger.info(f"Extraction complete: {len(metadata)} total fields")
        return metadata
