"""
Metadata extraction from NGB binary tables.
"""

from __future__ import annotations

import logging
import re
import struct
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

        # Compile temperature program patterns with correct structure
        TEMP_PROG_TYPE_PREFIX = (
            b"\x03\x80\x01"  # Different from regular metadata TYPE_PREFIX
        )
        for fname, pat_bytes in self.config.temp_prog_patterns.items():
            # Temperature program structure:
            # TEMP_PROG_TYPE_PREFIX + field_code + TYPE_SEPARATOR + data_type + field_separator + VALUE_PREFIX + value
            pat = (
                re.escape(TEMP_PROG_TYPE_PREFIX)
                + re.escape(pat_bytes)  # field code (e.g., \x17\x0e for temperature)
                + re.escape(self.config.temp_prog_type_separator)  # 00 00 01 00 00 00
                + rb"(.)"  # data type (1 byte, captured)
                + re.escape(self.config.temp_prog_field_separator)  # 00 17 fc ff ff
                + re.escape(self.config.temp_prog_value_prefix)  # 04 80 01
                + rb"(.{4})"  # value (4 bytes, captured)
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

        # Combine all table data for temperature program extraction
        combined_data = b"".join(tables)

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

            # Extract calibration constants from individual tables (preserves existing behavior)
            self._extract_calibration_constants(table, metadata)

        # Extract temperature program from combined data (FIX: ensures complete extraction)
        self._extract_temperature_program(combined_data, metadata)

        # Extract MFC metadata with structural disambiguation
        self._extract_mfc_metadata(tables, metadata)

        # Extract control parameters (furnace and sample PID settings)
        self._extract_control_parameters(tables, metadata)

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

    def _extract_mfc_metadata(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> None:
        """Extract MFC metadata using sequence-based field assignment."""
        try:
            # Step 1: Find field name definitions in order
            field_definitions = []
            for field_name in ["Purge 1", "Purge 2", "Protective"]:
                field_bytes = field_name.encode("utf-16le")

                for i, table_data in enumerate(tables):
                    if field_bytes in table_data:
                        field_key = field_name.lower().replace(" ", "_")
                        field_definitions.append(
                            {"table": i, "field": field_key, "name": field_name}
                        )
                        break  # Take first occurrence

            # Step 2: Find MFC range tables in order using signature-based identification
            range_tables = []
            for i, table_data in enumerate(tables):
                # Look for MFC signature (03 80 01 48 10)
                has_mfc_signature = False
                for j in range(len(table_data) - 4):
                    if table_data[j : j + 3] == b"\x03\x80\x01":
                        sig_bytes = table_data[j + 3 : j + 5]
                        if len(sig_bytes) == 2:
                            sig_val = struct.unpack("<H", sig_bytes)[0]
                            if sig_val == 0x1048:  # MFC range signature
                                has_mfc_signature = True
                                break

                if has_mfc_signature:
                    # Find range values in this table
                    for range_val in [250.0, 252.5]:
                        range_bytes = struct.pack("<f", range_val)
                        if range_bytes in table_data:
                            range_tables.append({"table": i, "range": range_val})
                            break

            # Step 3: Build gas context map for gas assignment
            gas_context_map = {}
            for i, table_data in enumerate(tables):
                if len(table_data) > 20:
                    try:
                        # Check for gas context signature
                        if table_data[1] == 0x1B:
                            # Look for gas names in UTF-16LE
                            for gas_name in [
                                "NITROGEN",
                                "OXYGEN",
                                "ARGON",
                                "HELIUM",
                                "CARBON_DIOXIDE",
                            ]:
                                gas_bytes = gas_name.encode("utf-16le")
                                if gas_bytes in table_data:
                                    gas_context_map[i] = gas_name
                                    break
                    except (IndexError, UnicodeDecodeError):
                        continue

            # Step 4: Map fields to ranges using ORDINAL/SEQUENTIAL assignment
            # 1st field → 1st range, 2nd field → 2nd range, etc.
            mfc_fields: dict[str, str | float] = {}
            for field_idx, range_info in enumerate(
                range_tables[:3]
            ):  # Take first 3 ranges
                if field_idx < len(field_definitions):
                    field_info = field_definitions[field_idx]
                    field_key = str(field_info["field"])
                    range_table = int(range_info["table"])
                    range_value = range_info["range"]

                    # Find gas type for this range table
                    gas_type = None
                    for context_table in reversed(range(range_table)):
                        if context_table in gas_context_map:
                            gas_type = gas_context_map[context_table]
                            break

                    # Assign gas and range to the field
                    if gas_type:
                        gas_field = f"{field_key}_mfc_gas"
                        range_field = f"{field_key}_mfc_range"
                        mfc_fields[gas_field] = str(gas_type)
                        mfc_fields[range_field] = float(range_value)

            # Update metadata with extracted MFC fields - type: ignore for dynamic keys
            metadata.update(mfc_fields)  # type: ignore[typeddict-item]

        except Exception as e:
            logger.warning(f"Failed to extract MFC metadata: {e}")

    def _extract_control_parameters(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> None:
        """Extract control parameters (furnace and sample PID settings) using signature identification."""
        try:
            # Control parameter signatures:
            # 0x0fe7 = XP (proportional gain)
            # 0x0fe8 = TN (integral time)
            # 0x0fe9 = TV (derivative time)

            control_signatures = {0x0FE7: "xp", 0x0FE8: "tn", 0x0FE9: "tv"}

            # Track control tables found (first = furnace, second = sample)
            control_tables = []

            for table_num, table_data in enumerate(tables):
                if len(table_data) == 0:
                    continue

                # Look for control parameter signatures in this table
                control_params_found = {}

                for i in range(len(table_data) - 4):
                    if table_data[i : i + 3] == b"\x03\x80\x01":
                        sig_bytes = table_data[i + 3 : i + 5]
                        if len(sig_bytes) == 2:
                            sig_val = struct.unpack("<H", sig_bytes)[0]

                            if sig_val in control_signatures:
                                # Look for float value after this signature
                                for offset in range(5, min(200, len(table_data) - i)):
                                    test_pos = i + offset
                                    if test_pos + 4 <= len(table_data):
                                        try:
                                            float_val = struct.unpack(
                                                "<f",
                                                table_data[test_pos : test_pos + 4],
                                            )[0]
                                            # Check if this looks like a control parameter value (typically 4.00-6.00 range)
                                            if 3.0 <= float_val <= 7.0:
                                                param_name = control_signatures[sig_val]
                                                control_params_found[param_name] = (
                                                    float_val
                                                )
                                                break
                                        except struct.error:
                                            continue

                # If we found control parameters in this table, add it to our list
                if (
                    len(control_params_found) == 3
                ):  # Should have all 3 parameters (xp, tn, tv)
                    control_tables.append((table_num, control_params_found))

            # Assign control parameters based on order (first = furnace, second = sample)
            if len(control_tables) >= 2:
                # First control table = furnace parameters
                furnace_params = control_tables[0][1]
                for param_name, value in furnace_params.items():
                    # Type ignore for dynamic key assignment
                    metadata[f"furnace_{param_name}"] = value  # type: ignore[literal-required]

                # Second control table = sample parameters
                sample_params = control_tables[1][1]
                for param_name, value in sample_params.items():
                    # Type ignore for dynamic key assignment
                    metadata[f"sample_{param_name}"] = value  # type: ignore[literal-required]

        except Exception as e:
            logger.warning(f"Failed to extract control parameters: {e}")

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

                    # Temperature program uses data type 0x0c which isn't handled by default parser
                    # Manually parse as 32-bit float for now
                    if data_type == b"\x0c" and len(value_bytes) == 4:
                        import struct

                        value = struct.unpack("<f", value_bytes)[0]
                    else:
                        value = self.parser.parse_value(data_type, value_bytes)

                    if value is not None:
                        stage[field_name] = value
