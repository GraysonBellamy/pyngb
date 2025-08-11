"""
Main NGB parser classes.
"""

from __future__ import annotations

import logging
import re
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import pyarrow as pa

from ..binary import BinaryParser
from ..constants import BinaryMarkers, PatternConfig, FileMetadata
from ..exceptions import NGBStreamNotFoundError
from ..extractors import MetadataExtractor, DataStreamProcessor

__all__ = ["NGBParser", "NGBParserExtended"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class NGBParser:
    """Main parser for NETZSCH STA NGB files with enhanced error handling.

    This is the primary interface for parsing NETZSCH NGB files. It orchestrates
    the parsing of metadata and measurement data from the various streams within
    an NGB file.

    The parser handles the complete workflow:
    1. Opens and validates the NGB ZIP archive
    2. Extracts metadata from stream_1.table
    3. Processes measurement data from stream_2.table and stream_3.table
    4. Returns structured data with embedded metadata

    Example:
        >>> parser = NGBParser()
        >>> metadata, data_table = parser.parse("sample.ngb-ss3")
        >>> print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
        >>> print(f"Data shape: {data_table.num_rows} x {data_table.num_columns}")
        Sample: Test Sample 1
        Data shape: 2500 x 8

    Advanced Configuration:
        >>> config = PatternConfig()
        >>> config.column_map["custom_id"] = "custom_column"
        >>> parser = NGBParser(config)

    Attributes:
        config: Pattern configuration for parsing
        markers: Binary markers for data identification
        binary_parser: Low-level binary parsing engine
        metadata_extractor: Metadata extraction engine
        data_processor: Data stream processing engine

    Thread Safety:
        This parser is not thread-safe. Create separate instances for
        concurrent parsing operations.
    """

    def __init__(self, config: Optional[PatternConfig] = None) -> None:
        self.config = config or PatternConfig()
        self.markers = BinaryMarkers()
        self.binary_parser = BinaryParser(self.markers)
        self.metadata_extractor = MetadataExtractor(self.config, self.binary_parser)
        self.data_processor = DataStreamProcessor(self.config, self.binary_parser)

    def parse(self, path: str) -> Tuple[FileMetadata, pa.Table]:
        """Parse NGB file and return metadata and Arrow table.

        Opens an NGB file, extracts all metadata and measurement data,
        and returns them as separate objects for flexible use.

        Args:
            path: Path to the .ngb-ss3 file to parse

        Returns:
            Tuple of (metadata_dict, pyarrow_table) where:
            - metadata_dict contains instrument settings, sample info, etc.
            - pyarrow_table contains the measurement data columns

        Raises:
            FileNotFoundError: If the specified file doesn't exist
            NGBStreamNotFoundError: If required streams are missing
            NGBCorruptedFileError: If file structure is invalid
            zipfile.BadZipFile: If file is not a valid ZIP archive

        Example:
            >>> metadata, data = parser.parse("experiment.ngb-ss3")
            >>> print(f"Instrument: {metadata.get('instrument', 'Unknown')}")
            >>> print(f"Columns: {data.column_names}")
            >>> print(f"Temperature range: {data['temperature'].min()} to {data['temperature'].max()}")
            Instrument: NETZSCH STA 449 F3 Jupiter
            Columns: ['time', 'temperature', 'mass', 'dsc', 'purge_flow']
            Temperature range: 25.0 to 800.0

        Performance:
            Typical parsing times:
            - Small files (<1MB): <0.1 seconds
            - Medium files (1-10MB): 0.1-1 seconds
            - Large files (10-100MB): 1-10 seconds
        """
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"File not found: {path}")

        metadata: FileMetadata = {}

        # Import polars here to avoid top-level import
        import polars as pl

        data_df = pl.DataFrame()

        try:
            with zipfile.ZipFile(path, "r") as z:
                # Validate NGB file structure
                available_streams = z.namelist()
                logger.debug(f"Available streams: {available_streams}")

                # stream_1: metadata
                if "Streams/stream_1.table" in available_streams:
                    with z.open("Streams/stream_1.table") as stream:
                        stream_data = stream.read()
                        tables = self.binary_parser.split_tables(stream_data)
                        metadata = self.metadata_extractor.extract_metadata(tables)
                else:
                    raise NGBStreamNotFoundError(
                        "stream_1.table not found - metadata unavailable"
                    )

                # stream_2: primary data
                if "Streams/stream_2.table" in available_streams:
                    with z.open("Streams/stream_2.table") as stream:
                        stream_data = stream.read()
                        data_df = self.data_processor.process_stream_2(stream_data)

                # stream_3: additional data merged into existing df
                if "Streams/stream_3.table" in z.namelist():
                    with z.open("Streams/stream_3.table") as stream:
                        stream_data = stream.read()
                        data_df = self.data_processor.process_stream_3(
                            stream_data, data_df
                        )

        except Exception as e:
            logger.error("Failed to parse NGB file: %s", e)
            raise

        return metadata, data_df.to_arrow()


class NGBParserExtended(NGBParser):
    """Extended parser with additional capabilities."""

    def __init__(
        self, config: Optional[PatternConfig] = None, cache_patterns: bool = True
    ):
        super().__init__(config)
        self.cache_patterns = cache_patterns
        self._pattern_cache: Dict[str, re.Pattern] = {}

    def add_custom_column_mapping(self, hex_id: str, column_name: str) -> None:
        """Add custom column mapping at runtime."""
        self.config.column_map[hex_id] = column_name

    def add_metadata_pattern(
        self, field_name: str, category: bytes, field: bytes
    ) -> None:
        """Add custom metadata pattern at runtime."""
        self.config.metadata_patterns[field_name] = (category, field)

    def parse_with_validation(self, path: str) -> Tuple[FileMetadata, pa.Table]:
        """Parse with additional validation."""
        metadata, data = self.parse(path)

        # Validate required columns
        required_columns = ["time", "temperature"]
        schema = data.schema
        missing = [col for col in required_columns if col not in schema.names]
        if missing:
            logger.warning("Missing required columns: %s", missing)

        # Validate data ranges
        if "temperature" in schema.names:
            temp_col = data.column("temperature").to_pylist()
            if temp_col and (min(temp_col) < -273.15 or max(temp_col) > 3000):
                logger.warning("Temperature values outside expected range")

        return metadata, data
