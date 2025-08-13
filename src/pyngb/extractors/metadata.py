"""
Metadata extraction from NGB binary tables.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from ..binary import BinaryParser
from ..constants import (
    FileMetadata,
    PatternConfig,
    SAMPLE_CRUCIBLE_SIG_FRAGMENT,
    REF_CRUCIBLE_SIG_FRAGMENT,
)
from ..exceptions import NGBParseError

__all__ = ["MetadataExtractor"]

logger = logging.getLogger(__name__)

SAMPLE_SIG_FRAGMENT = SAMPLE_CRUCIBLE_SIG_FRAGMENT
REF_SIG_FRAGMENT = REF_CRUCIBLE_SIG_FRAGMENT


class MetadataExtractor:
    """Extracts metadata from NGB tables with improved type safety."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        self.config = config
        self.parser = parser
        self._compiled_meta: dict[str, re.Pattern[bytes]] = {}
        self._compiled_temp_prog: dict[str, re.Pattern[bytes]] = {}
        self._compiled_cal_consts: dict[str, re.Pattern[bytes]] = {}

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

    def extract_field(self, table: bytes, field_name: str) -> Any | None:
        """Extract a single metadata field (value only)."""
        if field_name not in self._compiled_meta:
            raise NGBParseError(f"Unknown metadata field: {field_name}")

        pattern = self._compiled_meta[field_name]
        matches = pattern.findall(table)
        if matches:
            data_type, value = matches[0]
            return self.parser.parse_value(data_type, value)
        return None

    def extract_metadata(self, tables: list[bytes]) -> FileMetadata:
        """Extract all metadata from tables with type safety."""
        metadata: FileMetadata = {}
        crucible_masses: list[tuple[int, float]] = []
        for table in tables:
            for field_name, pattern in self._compiled_meta.items():
                try:
                    matches = pattern.findall(table)
                    if not matches:
                        continue
                    for idx, (data_type, value_bytes) in enumerate(matches):
                        value = self.parser.parse_value(data_type, value_bytes)
                        if value is None:
                            continue
                        if field_name == "date_performed" and isinstance(value, int):
                            value = datetime.fromtimestamp(
                                value, tz=timezone.utc
                            ).isoformat()
                        if field_name == "crucible_mass" and isinstance(
                            value, (int, float)
                        ):
                            search_from = 0
                            match_obj = pattern.search(table, search_from)
                            skip = idx
                            while match_obj is not None and skip > 0:
                                search_from = match_obj.end()
                                match_obj = pattern.search(table, search_from)
                                skip -= 1
                            pos = match_obj.start() if match_obj else 0
                            crucible_masses.append((pos, float(value)))
                        elif field_name == "sample_mass" and isinstance(
                            value, (int, float)
                        ):
                            # Defer storing; structural pass may override. Store only if absent.
                            if "sample_mass" not in metadata:
                                metadata["sample_mass"] = float(value)  # type: ignore
                        else:
                            if field_name not in metadata:
                                metadata[field_name] = value  # type: ignore
                except NGBParseError as e:
                    logger.warning(f"Failed to extract field {field_name}: {e}")

            self._extract_temperature_program(table, metadata)
            self._extract_calibration_constants(table, metadata)

        # Structural classification for crucible masses (no numeric heuristics)
        if crucible_masses:
            combined = b"".join(tables)
            crucible_pattern = self._compiled_meta.get("crucible_mass")
            occurrences: list[dict[str, Any]] = []
            if crucible_pattern:
                for m in crucible_pattern.finditer(combined):  # type: ignore[attr-defined]
                    try:
                        data_type, value_bytes = m.groups()
                    except ValueError:
                        continue
                    value = self.parser.parse_value(data_type, value_bytes)
                    if not isinstance(value, (int, float)):
                        continue
                    start = m.start()
                    pre = combined[max(0, start - 64) : start]
                    occurrences.append(
                        {"byte_pos": start, "value": float(value), "pre": pre}
                    )

            # Use module-level signature constants

            sample_sig_occ: list[dict[str, Any]] = []
            ref_sig_occ: list[dict[str, Any]] = []
            zero_occ: list[dict[str, Any]] = []
            for occ in occurrences:
                pre = occ["pre"]
                if SAMPLE_SIG_FRAGMENT in pre:
                    sample_sig_occ.append(occ)
                if REF_SIG_FRAGMENT in pre:
                    ref_sig_occ.append(occ)
                if abs(occ["value"]) < 1e-12:
                    zero_occ.append(occ)

            if sample_sig_occ:
                metadata["crucible_mass"] = sorted(
                    sample_sig_occ, key=lambda o: o["byte_pos"]
                )[0]["value"]  # type: ignore
            if ref_sig_occ:
                ref_occ = sorted(ref_sig_occ, key=lambda o: o["byte_pos"])[0]
                metadata["reference_crucible_mass"] = ref_occ["value"]  # type: ignore
                # Attempt structural extraction of preceding field's numeric value as reference_mass
                start = ref_occ["byte_pos"]
                window_start = max(0, start - 256)
                pre_block = combined[window_start:start]
                END_FIELD = self.parser.markers.END_FIELD
                TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
                TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR
                # Strategy: walk backwards finding pattern TYPE_PREFIX <dtype> ... TYPE_SEPARATOR <value> END_FIELD
                search_region = pre_block
                ref_mass_found = False
                # Iterate over possible field endings from newest to oldest within window
                idx = len(search_region)
                while not ref_mass_found:
                    end_idx = search_region.rfind(END_FIELD, 0, idx)
                    if end_idx == -1:
                        break
                    # Find preceding type prefix for this field
                    type_prefix_idx = search_region.rfind(TYPE_PREFIX, 0, end_idx)
                    if type_prefix_idx == -1:
                        idx = end_idx
                        continue
                    data_type_idx = type_prefix_idx + len(TYPE_PREFIX)
                    if data_type_idx >= end_idx:
                        idx = end_idx
                        continue
                    data_type = search_region[data_type_idx : data_type_idx + 1]
                    sep_idx = search_region.find(
                        TYPE_SEPARATOR, data_type_idx + 1, end_idx
                    )
                    if sep_idx == -1:
                        idx = end_idx
                        continue
                    value_start = sep_idx + len(TYPE_SEPARATOR)
                    value_end = end_idx
                    raw_value = search_region[value_start:value_end]
                    parsed = self.parser.parse_value(data_type, raw_value)
                    if isinstance(parsed, (int, float)):
                        metadata.setdefault("reference_mass", float(parsed))  # type: ignore
                        ref_mass_found = True
                    idx = end_idx

            # Attempt analogous structural extraction of sample_mass immediately preceding sample crucible mass
            if sample_sig_occ:
                sample_occ = sorted(sample_sig_occ, key=lambda o: o["byte_pos"])[0]
                start_s = sample_occ["byte_pos"]
                window_start_s = max(0, start_s - 256)
                pre_block_s = combined[window_start_s:start_s]
                END_FIELD = self.parser.markers.END_FIELD
                TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
                TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR
                idx_s = len(pre_block_s)
                found_sample_mass = False
                while not found_sample_mass:
                    end_idx_s = pre_block_s.rfind(END_FIELD, 0, idx_s)
                    if end_idx_s == -1:
                        break
                    type_prefix_idx_s = pre_block_s.rfind(TYPE_PREFIX, 0, end_idx_s)
                    if type_prefix_idx_s == -1:
                        idx_s = end_idx_s
                        continue
                    data_type_idx_s = type_prefix_idx_s + len(TYPE_PREFIX)
                    if data_type_idx_s >= end_idx_s:
                        idx_s = end_idx_s
                        continue
                    data_type_s = pre_block_s[data_type_idx_s : data_type_idx_s + 1]
                    sep_idx_s = pre_block_s.find(
                        TYPE_SEPARATOR, data_type_idx_s + 1, end_idx_s
                    )
                    if sep_idx_s == -1:
                        idx_s = end_idx_s
                        continue
                    value_start_s = sep_idx_s + len(TYPE_SEPARATOR)
                    value_end_s = end_idx_s
                    raw_value_s = pre_block_s[value_start_s:value_end_s]
                    parsed_s = self.parser.parse_value(data_type_s, raw_value_s)
                    if (
                        isinstance(parsed_s, (int, float))
                        and "sample_mass" not in metadata
                    ):
                        metadata["sample_mass"] = float(parsed_s)  # type: ignore
                        found_sample_mass = True
                    idx_s = end_idx_s
            if (
                "crucible_mass" in metadata
                and "reference_crucible_mass" not in metadata
                and zero_occ
            ):
                metadata["reference_crucible_mass"] = sorted(
                    zero_occ, key=lambda o: o["byte_pos"]
                )[0]["value"]  # type: ignore
            if "crucible_mass" not in metadata and occurrences:
                metadata["crucible_mass"] = sorted(
                    occurrences, key=lambda o: o["byte_pos"]
                )[0]["value"]  # type: ignore

        return metadata

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

    def _extract_temperature_program(
        self, table: bytes, metadata: FileMetadata
    ) -> None:
        """Extract temperature program stages (lightweight implementation).

        Builds a nested dict: temperature_program[stage_i][field] = value
        where i is the index of the match for any of the temperature program
        fields. This keeps ordering without assuming all fields present.
        """
        if b"\xf4\x01" not in table and b"\xf5\x01" not in table:
            # Heuristic: skip if likely no temp program category bytes
            pass
        temp_prog = metadata.setdefault("temperature_program", {})  # type: ignore
        # Collect matches per field
        field_matches: dict[str, list[tuple[bytes, bytes]]] = {}
        for field_name, pattern in self._compiled_temp_prog.items():
            found = list(pattern.findall(table))
            if found:
                field_matches[field_name] = found
        if not field_matches:
            return
        # Determine max stage count among fields
        max_len = max(len(v) for v in field_matches.values())
        for i in range(max_len):
            stage_key = f"stage_{i}"
            stage = temp_prog.setdefault(stage_key, {})  # type: ignore
            for field_name, matches in field_matches.items():
                if i < len(matches):
                    data_type, value_bytes = matches[i]
                    value = self.parser.parse_value(data_type, value_bytes)
                    if value is not None:
                        stage[field_name] = value
