"""
Main NGB parser classes.
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

import polars as pl
import pyarrow as pa

from ..binary import BinaryParser
from ..constants import BinaryMarkers, FileMetadata, PatternConfig
from ..exceptions import NGBStreamNotFoundError
from ..extractors import DataStreamProcessor, MetadataExtractor

__all__ = ["NGBParser"]

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

    def __init__(self, config: PatternConfig | None = None) -> None:
        self.config = config or PatternConfig()
        self.markers = BinaryMarkers()
        self.binary_parser = BinaryParser(self.markers)
        self.metadata_extractor = MetadataExtractor(self.config, self.binary_parser)
        self.data_processor = DataStreamProcessor(self.config, self.binary_parser)

    def validate_ngb_structure(self, zip_file: zipfile.ZipFile) -> list[str]:
        """Validate that the ZIP file has the expected NGB structure.

        Args:
            zip_file: Open ZIP file to validate

        Returns:
            List of available streams

        Raises:
            NGBStreamNotFoundError: If required streams are missing
        """
        available_streams = zip_file.namelist()
        logger.debug(f"Available streams: {available_streams}")

        # Check for required streams
        # stream_1 and stream_2 are required for basic operation; stream_3 is optional
        required_streams = ["Streams/stream_1.table", "Streams/stream_2.table"]
        missing_streams = [
            stream for stream in required_streams if stream not in available_streams
        ]

        if missing_streams:
            raise NGBStreamNotFoundError(f"Missing required streams: {missing_streams}")

        return available_streams

    def parse(self, path: str) -> tuple[FileMetadata, pa.Table]:
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
            >>> print(f"Temperature range: {data['sample_temperature'].min()} to {data['sample_temperature'].max()}")
            Instrument: NETZSCH STA 449 F3 Jupiter
            Columns: ['time', 'sample_temperature', 'mass', 'dsc_signal', 'purge_flow']
            Temperature range: 25.0 to 800.0
        """
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"File not found: {path}")

        metadata: FileMetadata = {}
        data_df = pl.DataFrame()

        try:
            with zipfile.ZipFile(path, "r") as z:
                # Validate NGB file structure
                available_streams = self.validate_ngb_structure(z)

                # stream_1: metadata
                with z.open("Streams/stream_1.table") as stream:
                    stream_data = stream.read()
                    tables = self.binary_parser.split_tables(stream_data)
                    metadata = self.metadata_extractor.extract_metadata(tables)

                # stream_2: primary data
                if "Streams/stream_2.table" in available_streams:
                    with z.open("Streams/stream_2.table") as stream:
                        stream_data = stream.read()
                        data_df = self.data_processor.process_stream_2(stream_data)

                # stream_3: additional data merged into existing df
                if "Streams/stream_3.table" in available_streams:
                    with z.open("Streams/stream_3.table") as stream:
                        stream_data = stream.read()
                        data_df = self.data_processor.process_stream_3(
                            stream_data, data_df
                        )

        except zipfile.BadZipFile as e:
            logger.error("Invalid ZIP archive: %s", e)
            raise
        except NGBStreamNotFoundError:
            # Re-raise our custom exceptions as-is
            raise
        except Exception as e:
            logger.error("Failed to parse NGB file: %s", e)
            raise

        # Convert to PyArrow at API boundary for cross-language compatibility
        # and metadata embedding. This is the single conversion point from
        # internal Polars processing to external PyArrow interface.
        return metadata, data_df.to_arrow()
