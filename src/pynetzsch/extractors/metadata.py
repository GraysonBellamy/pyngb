"""
Metadata extraction from NGB binary tables.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from ..binary import BinaryParser
from ..constants import PatternConfig, FileMetadata
from ..exceptions import NGBParseError

__all__ = ["MetadataExtractor"]

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extracts metadata from NGB tables with improved type safety."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        self.config = config
        self.parser = parser
        self._compiled_meta: Dict[str, re.Pattern[bytes]] = {}
        self._compiled_temp_prog: Dict[str, re.Pattern[bytes]] = {}
        self._compiled_cal_consts: Dict[str, re.Pattern[bytes]] = {}

        # Precompile patterns used in tight loops for speed (logic unchanged).
        END_FIELD = self.parser.markers.END_FIELD
        TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
        TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR

        for fname, (category, field_bytes) in self.config.metadata_patterns.items():
            pat = (
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
            self._compiled_meta[fname] = re.compile(pat, re.DOTALL)

        for fname, pat_bytes in self.config.temp_prog_patterns.items():
            pat = (
                pat_bytes
                + rb".+?"
                + TYPE_PREFIX
                + rb"(.+?)"
                + TYPE_SEPARATOR
                + rb"(.+?)"
                + END_FIELD
            )
            self._compiled_temp_prog[fname] = re.compile(pat, re.DOTALL)

        for fname, pat_bytes in self.config.cal_constants_patterns.items():
            pat = (
                pat_bytes
                + rb".+?"
                + TYPE_PREFIX
                + rb"(.+?)"
                + TYPE_SEPARATOR
                + rb"(.+?)"
                + END_FIELD
            )
            self._compiled_cal_consts[fname] = re.compile(pat, re.DOTALL)

    def extract_field(self, table: bytes, field_name: str) -> Optional[Any]:
        """Extract a single metadata field (value only)."""
        if field_name not in self._compiled_meta:
            raise NGBParseError(f"Unknown metadata field: {field_name}")

        pattern = self._compiled_meta[field_name]
        matches = pattern.findall(table)
        if matches:
            data_type, value = matches[0]
            return self.parser.parse_value(data_type, value)
        return None

    def extract_metadata(self, tables: List[bytes]) -> FileMetadata:
        """Extract all metadata from tables with type safety."""
        metadata: FileMetadata = {}

        for table in tables:
            # Standard fields
            for field_name in self._compiled_meta.keys():
                try:
                    value = self.extract_field(table, field_name)
                    if value is not None:
                        if field_name == "date_performed" and isinstance(value, int):
                            value = datetime.fromtimestamp(
                                value, tz=timezone.utc
                            ).isoformat()
                        metadata[field_name] = value  # type: ignore
                except NGBParseError as e:
                    logger.warning(f"Failed to extract field {field_name}: {e}")

            # Temperature program
            self._extract_temperature_program(table, metadata)

            # Calibration constants
            self._extract_calibration_constants(table, metadata)

        return metadata

    def _extract_temperature_program(
        self, table: bytes, metadata: FileMetadata
    ) -> None:
        """Extract temperature program section."""
        CATEGORY = b"\x0c\x2b"
        if CATEGORY not in table:
            return

        step_num = table[0:2].decode("ascii", errors="ignore")[0] if table else "0"
        temp_prog = metadata.setdefault("temperature_program", {})
        step_key = f"step_{step_num}"
        step_data = temp_prog.setdefault(step_key, {})

        for field_name, pattern in self._compiled_temp_prog.items():
            match = pattern.search(table)
            if match:
                data_type, value_bytes = match.groups()
                value = self.parser.parse_value(data_type, value_bytes)
                if value is not None:
                    step_data[field_name] = value

    def _extract_calibration_constants(
        self, table: bytes, metadata: FileMetadata
    ) -> None:
        """Extract calibration constants section."""
        CATEGORY = b"\xf5\x01"
        if CATEGORY not in table:
            return

        cal_constants = metadata.setdefault("calibration_constants", {})
        for field_name, pattern in self._compiled_cal_consts.items():
            match = pattern.search(table)
            if match:
                data_type, value_bytes = match.groups()
                value = self.parser.parse_value(data_type, value_bytes)
                if value is not None:
                    cal_constants[field_name] = value
