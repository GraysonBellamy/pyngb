"""
Temperature program extractor for NGB metadata.

This extractor handles the temperature program stages extraction including
stage types, temperatures, heating rates, acquisition rates, and times.
"""

import re
import struct
from typing import Any

from ..binary import BinaryParser
from ..constants import TEMP_PROG_TYPE_PREFIX, PatternConfig
from .base import BaseMetadataExtractor, FileMetadata

__all__ = ["TemperatureProgramExtractor"]


class TemperatureProgramExtractor(BaseMetadataExtractor):
    """Extracts temperature program metadata from NGB tables.

    The temperature program contains stages with information about:
    - Stage type (heating, cooling, isothermal)
    - Temperature setpoints
    - Heating/cooling rates
    - Acquisition rates
    - Time durations

    Example:
        >>> config = PatternConfig()
        >>> parser = BinaryParser()
        >>> extractor = TemperatureProgramExtractor(config, parser)
        >>> metadata = {}
        >>> extractor.extract([table_data], metadata)
        >>> print(metadata['temperature_program']['stage_0'])
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Temperature Program")
        self.config = config
        self.parser = parser
        self._compiled_temp_prog: dict[str, re.Pattern[bytes]] = {}

        # Compile temperature program patterns
        self._compile_temperature_patterns()

    def _compile_temperature_patterns(self) -> None:
        """Compile regex patterns for temperature program extraction."""
        for fname, pat_bytes in self.config.temp_prog_patterns.items():
            # Temperature program structure:
            # TEMP_PROG_TYPE_PREFIX + field_code + TYPE_SEPARATOR + data_type + field_separator + VALUE_PREFIX + value
            pat = (
                re.escape(TEMP_PROG_TYPE_PREFIX)
                + re.escape(pat_bytes)  # field code (e.g., \\x17\\x0e for temperature)
                + re.escape(self.config.temp_prog_type_separator)  # 00 00 01 00 00 00
                + rb"(.)"  # data type (1 byte, captured)
                + re.escape(self.config.temp_prog_field_separator)  # 00 17 fc ff ff
                + re.escape(self.config.temp_prog_value_prefix)  # 04 80 01
                + rb"(.{4})"  # value (4 bytes, captured)
            )
            self._compiled_temp_prog[fname] = re.compile(pat, re.DOTALL)

        self.logger.debug(
            f"Compiled {len(self._compiled_temp_prog)} temperature program patterns"
        )

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if temperature program can be extracted from the tables.

        Args:
            tables: List of binary table data

        Returns:
            True if temperature program patterns might be found
        """
        if not tables:
            return False

        combined_data = b"".join(tables)

        # Check for temperature program category bytes
        if b"\xf4\x01" in combined_data or b"\xf5\x01" in combined_data:
            return True

        # Check for temperature program type prefix
        return TEMP_PROG_TYPE_PREFIX in combined_data

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract temperature program from tables.

        Args:
            tables: List of binary table data from NGB streams
            metadata: Metadata dictionary to update (modified in-place)
        """
        self.log_extraction_attempt(len(tables))

        if not tables:
            return

        # Combine all table data for temperature program extraction
        combined_data = b"".join(tables)

        # Extract temperature program stages
        temp_prog = self._extract_temperature_program(combined_data)

        if temp_prog:
            metadata["temperature_program"] = temp_prog
            stage_count = len(temp_prog)
            self.log_extraction_success(stage_count)
            self.logger.debug(
                f"Extracted temperature program with {stage_count} stages"
            )
        else:
            self.logger.debug("No temperature program found")

    def _extract_temperature_program(self, data: bytes) -> dict[str, dict[str, Any]]:
        """Extract temperature program stages from binary data.

        Builds a nested dict: temperature_program[stage_i][field] = value
        where i is the index of the match for any of the temperature program
        fields. This keeps ordering without assuming all fields are present.

        Args:
            data: Combined binary data from all tables

        Returns:
            Dictionary of temperature program stages
        """
        temp_prog: dict[str, dict[str, Any]] = {}

        # Collect matches per field
        field_matches: dict[str, list[tuple[bytes, bytes]]] = {}

        for field_name, pattern in self._compiled_temp_prog.items():
            found = list(pattern.findall(data))
            if found:
                field_matches[field_name] = found
                self.logger.debug(f"Found {len(found)} matches for {field_name}")

        if not field_matches:
            return temp_prog

        # Determine max stage count among fields
        max_len = max(len(v) for v in field_matches.values())
        self.logger.debug(f"Processing {max_len} temperature program stages")

        for i in range(max_len):
            stage_key = f"stage_{i}"
            stage: dict[str, Any] = {}

            for field_name, matches in field_matches.items():
                if i < len(matches):
                    data_type, value_bytes = matches[i]

                    # Temperature program uses data type 0x0c which isn't handled by default parser
                    # Manually parse as 32-bit float for now
                    if data_type == b"\x0c" and len(value_bytes) == 4:
                        try:
                            value = struct.unpack("<f", value_bytes)[0]
                        except struct.error:
                            continue
                    else:
                        value = self.parser.parse_value(data_type, value_bytes)

                    if value is not None:
                        stage[field_name] = value

            if stage:  # Only add stage if it has data
                temp_prog[stage_key] = stage

        return temp_prog
