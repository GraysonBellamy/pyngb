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
    APP_LICENSE_CATEGORY,
    APP_LICENSE_FIELD,
    APP_LICENSE_SEARCH_RANGE,
    CRUCIBLE_MASS_PREVIEW_SIZE,
    CRUCIBLE_MASS_SEARCH_WINDOW,
    GAS_CONTEXT_SIGNATURE,
    GAS_TYPES,
    MFC_FIELD_NAMES,
    MFC_SIGNATURE,
    REF_CRUCIBLE_SIG_FRAGMENT,
    SAMPLE_CRUCIBLE_SIG_FRAGMENT,
    STRING_DATA_TYPE,
    TEMP_PROG_TYPE_PREFIX,
    FileMetadata,
    PatternConfig,
)
from ..exceptions import NGBParseError

__all__ = ["MetadataExtractor"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

SAMPLE_SIG_FRAGMENT = SAMPLE_CRUCIBLE_SIG_FRAGMENT
REF_SIG_FRAGMENT = REF_CRUCIBLE_SIG_FRAGMENT

# Binary signatures for PID control parameters
#  - 0x0FE7: XP (proportional gain)
#  - 0x0FE8: TN (integral time)
#  - 0x0FE9: TV (derivative time)
PID_SIGNATURES: tuple[tuple[int, str], ...] = (
    (0x0FE7, "xp"),
    (0x0FE8, "tn"),
    (0x0FE9, "tv"),
)


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

        # Specialized extraction: application_version and licensed_to from container 0003_18fc
        try:
            self._extract_app_and_license(combined_data, metadata)
        except Exception as e:
            logger.debug("App/license extraction skipped: %s", e)

        # Extract control parameters (furnace and sample PID settings)
        self._extract_and_attach_pid_parameters(tables, metadata)

        # Structural classification for crucible masses (no numeric heuristics)
        self._extract_crucible_masses_structural(tables, metadata)

        return metadata

    def _extract_app_and_license(self, data: bytes, metadata: FileMetadata) -> None:
        """Extract application_version and licensed_to from 0003_18fc container.

        Uses existing markers and BinaryParser.parse_value to decode STRING values,
        then selects target strings via simple content rules.
        """
        pattern = re.compile(
            re.escape(APP_LICENSE_CATEGORY)
            + rb".{0,"
            + str(APP_LICENSE_SEARCH_RANGE).encode()
            + rb"}?"
            + re.escape(APP_LICENSE_FIELD)
            + rb".{0,"
            + str(APP_LICENSE_SEARCH_RANGE).encode()
            + rb"}?"
            + re.escape(self.parser.markers.TYPE_PREFIX)
            + rb"(.)"
            + re.escape(self.parser.markers.TYPE_SEPARATOR)
            + rb"(.*?)"
            + re.escape(self.parser.markers.END_FIELD),
            re.DOTALL,
        )

        strings: list[str] = []
        for m in pattern.finditer(data):
            dt, val = m.groups()
            if dt != STRING_DATA_TYPE:
                continue
            parsed = self.parser.parse_value(dt, val)
            if isinstance(parsed, str) and parsed:
                strings.append(parsed)

        if not strings:
            return

        # application_version: match leading 'Version x.y.z'
        app = next(
            (s for s in strings if re.match(r"^\s*Version\s+\d+\.\d+\.\d+", s)), None
        )
        if app and "application_version" not in metadata:
            metadata["application_version"] = app

        # licensed_to: pick multi-line non-Version string with letters and country-like tail
        license_candidates = [
            s for s in strings if ("\n" in s and not s.lstrip().startswith("Version"))
        ]
        if license_candidates and "licensed_to" not in metadata:
            # choose the longest reasonable candidate
            metadata["licensed_to"] = max(license_candidates, key=len)

    def _extract_mfc_metadata(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> None:
        """Extract MFC metadata using structural parsing and signature identification."""
        try:
            # Step 1: Find field name definitions in order
            field_definitions = self._find_mfc_field_definitions(tables)

            # Step 2: Find MFC range tables using signature-based identification
            range_tables = self._find_mfc_range_tables(tables)

            # Step 3: Build gas context map for gas assignment
            gas_context_map = self._build_gas_context_map(tables)

            # Step 4: Map fields to ranges using structural assignment
            mfc_fields = self._map_mfc_fields_to_ranges(
                field_definitions, range_tables, gas_context_map
            )

            # Update metadata with extracted MFC fields
            if mfc_fields:
                # Use type-safe assignment instead of dynamic keys
                for key, value in mfc_fields.items():
                    if key.endswith("_mfc_gas") and isinstance(value, str):
                        if key == "purge_1_mfc_gas":
                            metadata["purge_1_mfc_gas"] = value
                        elif key == "purge_2_mfc_gas":
                            metadata["purge_2_mfc_gas"] = value
                        elif key == "protective_mfc_gas":
                            metadata["protective_mfc_gas"] = value
                    elif key.endswith("_mfc_range") and isinstance(value, float):
                        if key == "purge_1_mfc_range":
                            metadata["purge_1_mfc_range"] = value
                        elif key == "purge_2_mfc_range":
                            metadata["purge_2_mfc_range"] = value
                        elif key == "protective_mfc_range":
                            metadata["protective_mfc_range"] = value

        except Exception as e:
            logger.warning(f"Failed to extract MFC metadata: {e}")

    def _find_mfc_field_definitions(self, tables: list[bytes]) -> list[dict[str, Any]]:
        """Find MFC field name definitions in tables."""
        field_definitions = []
        for field_name in MFC_FIELD_NAMES:
            field_bytes = field_name.encode("utf-16le")

            for i, table_data in enumerate(tables):
                if field_bytes in table_data:
                    field_key = field_name.lower().replace(" ", "_")
                    field_definitions.append(
                        {"table": i, "field": field_key, "name": field_name}
                    )
                    break  # Take first occurrence

        return field_definitions

    def _find_mfc_range_tables(self, tables: list[bytes]) -> list[dict[str, Any]]:
        """Find MFC range tables using signature identification."""
        range_tables = []

        for i, table_data in enumerate(tables):
            # Look for MFC signature pattern
            if self._has_mfc_signature(table_data):
                # Extract range value using structural parsing
                range_value = self._extract_mfc_range_value(table_data)
                if range_value is not None:
                    range_tables.append({"table": i, "range": range_value})

        return range_tables

    def _has_mfc_signature(self, table_data: bytes) -> bool:
        """Check if table has MFC signature pattern."""
        for j in range(len(table_data) - 4):
            if table_data[j : j + 3] == TEMP_PROG_TYPE_PREFIX:
                sig_bytes = table_data[j + 3 : j + 5]
                if len(sig_bytes) == 2:
                    sig_val = struct.unpack("<H", sig_bytes)[0]
                    if sig_val == MFC_SIGNATURE:
                        return True
        return False

    def _extract_mfc_range_value(self, table_data: bytes) -> float | None:
        """Extract MFC range value using structural parsing.

        Looks for the float value that follows the MFC signature
        using proper structural patterns instead of hardcoded values.
        """
        for j in range(len(table_data) - 4):
            if table_data[j : j + 3] == TEMP_PROG_TYPE_PREFIX:
                sig_bytes = table_data[j + 3 : j + 5]
                if len(sig_bytes) == 2:
                    sig_val = struct.unpack("<H", sig_bytes)[0]
                    if sig_val == MFC_SIGNATURE:
                        # Look for float value after signature using structural parsing
                        # The value should be a 4-byte float following the signature
                        for offset in range(5, min(50, len(table_data) - j)):
                            test_pos = j + offset
                            if test_pos + 4 <= len(table_data):
                                try:
                                    float_val = struct.unpack(
                                        "<f", table_data[test_pos : test_pos + 4]
                                    )[0]
                                    # Validate that this is a reasonable flow rate value
                                    # (typically 0.1 to 1000 ml/min for MFCs)
                                    if 0.1 <= float_val <= 1000.0:
                                        return float(float_val)
                                except struct.error:
                                    continue
        return None

    def _build_gas_context_map(self, tables: list[bytes]) -> dict[int, str]:
        """Build gas context map for MFC gas assignment."""
        gas_context_map = {}

        for i, table_data in enumerate(tables):
            if len(table_data) > 20:
                try:
                    # Check for gas context signature
                    if table_data[1] == GAS_CONTEXT_SIGNATURE:
                        # Look for gas names in UTF-16LE
                        for gas_name in GAS_TYPES:
                            gas_bytes = gas_name.encode("utf-16le")
                            if gas_bytes in table_data:
                                gas_context_map[i] = gas_name
                                break
                except (IndexError, UnicodeDecodeError):
                    continue

        return gas_context_map

    def _map_mfc_fields_to_ranges(
        self,
        field_definitions: list[dict[str, Any]],
        range_tables: list[dict[str, Any]],
        gas_context_map: dict[int, str],
    ) -> dict[str, str | float]:
        """Map MFC fields to ranges using structural assignment."""
        mfc_fields: dict[str, str | float] = {}

        # Map fields to ranges using ordinal assignment
        # 1st field → 1st range, 2nd field → 2nd range, etc.
        for field_idx, range_info in enumerate(range_tables[:3]):  # Take first 3 ranges
            if field_idx < len(field_definitions):
                field_info = field_definitions[field_idx]
                field_key = str(field_info["field"])
                range_table = int(range_info["table"])
                range_value = range_info["range"]

                # Find gas type for this range table
                gas_type = self._find_gas_type_for_table(range_table, gas_context_map)

                # Assign gas and range to the field
                if gas_type:
                    gas_field = f"{field_key}_mfc_gas"
                    range_field = f"{field_key}_mfc_range"
                    mfc_fields[gas_field] = str(gas_type)
                    mfc_fields[range_field] = float(range_value)

        return mfc_fields

    def _find_gas_type_for_table(
        self, range_table: int, gas_context_map: dict[int, str]
    ) -> str | None:
        """Find gas type for a given range table."""
        # Look backwards from the range table to find the most recent gas context
        for context_table in reversed(range(range_table)):
            if context_table in gas_context_map:
                return gas_context_map[context_table]
        return None

    def _extract_and_attach_pid_parameters(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> None:
        """Extract XP/TN/TV parameters and attach them to metadata.

        This orchestrates PID control parameter extraction by:
        - Concatenating the provided tables in order
        - Scanning the byte stream for known PID signatures
        - Mapping the first occurrence per parameter to furnace_* fields
          and the second occurrence (if present) to sample_* fields

        Rules
        -----
        - First 'xp' → metadata['furnace_xp'], second → 'sample_xp'
        - First 'tn' → metadata['furnace_tn'], second → 'sample_tn'
        - First 'tv' → metadata['furnace_tv'], second → 'sample_tv'
        - Missing parameters are skipped without writing keys

        Notes
        -----
        - Idempotent for the same input tables
        - Relies on fixed binary signatures emitted by the instrument
        """
        try:
            combined_data = b"".join(tables)

            matches = self._scan_pid_parameters(combined_data)
            if not matches:
                return

            # Group by parameter name
            xp_params = [p for p in matches if p["param_name"] == "xp"]
            tn_params = [p for p in matches if p["param_name"] == "tn"]
            tv_params = [p for p in matches if p["param_name"] == "tv"]

            # Sort by position to preserve occurrence order
            xp_params.sort(key=lambda x: x["position"])
            tn_params.sort(key=lambda x: x["position"])
            tv_params.sort(key=lambda x: x["position"])

            # Furnace = first occurrence; Sample = second occurrence
            if len(xp_params) >= 1:
                metadata["furnace_xp"] = xp_params[0]["value"]
            if len(tn_params) >= 1:
                metadata["furnace_tn"] = tn_params[0]["value"]
            if len(tv_params) >= 1:
                metadata["furnace_tv"] = tv_params[0]["value"]

            if len(xp_params) >= 2:
                metadata["sample_xp"] = xp_params[1]["value"]
            if len(tn_params) >= 2:
                metadata["sample_tn"] = tn_params[1]["value"]
            if len(tv_params) >= 2:
                metadata["sample_tv"] = tv_params[1]["value"]

        except Exception as e:
            logger.warning(f"Failed to extract control parameters: {e}")

    def _scan_pid_parameters(self, data: bytes) -> list[dict[str, Any]]:
        """Scan a binary payload for PID control parameters.

        This function searches for fixed binary signatures corresponding to
        XP (proportional gain), TN (integral time), and TV (derivative time).

        Pattern
        -------
        Each occurrence is encoded as:
          03 80 01 + <signature (little-endian uint16)> +
          00 00 01 00 00 00 0C 00 17 FC FF FF 04 80 01 +
          <4-byte float (little-endian)>

        Returns
        -------
        list[dict[str, Any]]
            A list of matches with keys:
            - 'param_name': 'xp' | 'tn' | 'tv'
            - 'value': float (decoded from 4-byte little-endian)
            - 'position': int (byte offset of the match)
            - 'signature': int (the 16-bit signature value)
        """
        control_params: list[dict[str, Any]] = []

        for sig_val, param_name in PID_SIGNATURES:
            # Build the signature pattern
            sig_bytes = struct.pack("<H", sig_val)
            pattern = (
                b"\x03\x80\x01"
                + sig_bytes
                + b"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x04\x80\x01"
            )

            # Find all occurrences of this pattern
            start = 0
            while True:
                pos = data.find(pattern, start)
                if pos == -1:
                    break

                # Extract the value (4 bytes after the pattern)
                value_pos = pos + len(pattern)
                if value_pos + 4 <= len(data):
                    try:
                        value = struct.unpack("<f", data[value_pos : value_pos + 4])[0]
                        control_params.append(
                            {
                                "param_name": param_name,
                                "value": value,
                                "position": pos,
                                "signature": sig_val,
                            }
                        )
                    except struct.error:
                        pass

                start = pos + 1

        return control_params

    # Note: assignment is handled directly in _extract_and_attach_pid_parameters

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
        temp_prog = metadata.setdefault("temperature_program", {})  # type: ignore[assignment]
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

    def _extract_structural_field_value(
        self,
        data: bytes,
        start_pos: int,
        search_window: int = CRUCIBLE_MASS_SEARCH_WINDOW,
    ) -> float | None:
        """Extract a field value using structural parsing from a given position.

        Walks backwards from start_pos to find the most recent complete field
        with pattern: TYPE_PREFIX <dtype> ... TYPE_SEPARATOR <value> END_FIELD

        Args:
            data: Binary data to search in
            start_pos: Position to start searching backwards from
            search_window: Number of bytes to search backwards

        Returns:
            Parsed numeric value if found, None otherwise
        """
        window_start = max(0, start_pos - search_window)
        search_region = data[window_start:start_pos]

        # Walk backwards finding pattern TYPE_PREFIX <dtype> ... TYPE_SEPARATOR <value> END_FIELD
        idx = len(search_region)
        while idx > 0:
            # Find the most recent field ending
            end_idx = search_region.rfind(self.parser.markers.END_FIELD, 0, idx)
            if end_idx == -1:
                break

            # Find preceding type prefix for this field
            type_prefix_idx = search_region.rfind(
                self.parser.markers.TYPE_PREFIX, 0, end_idx
            )
            if type_prefix_idx == -1:
                idx = end_idx
                continue

            data_type_idx = type_prefix_idx + len(self.parser.markers.TYPE_PREFIX)
            if data_type_idx >= end_idx:
                idx = end_idx
                continue

            # Extract data type
            data_type = search_region[data_type_idx : data_type_idx + 1]

            # Find type separator
            sep_idx = search_region.find(
                self.parser.markers.TYPE_SEPARATOR, data_type_idx + 1, end_idx
            )
            if sep_idx == -1:
                idx = end_idx
                continue

            # Extract and parse value
            value_start = sep_idx + len(self.parser.markers.TYPE_SEPARATOR)
            value_end = end_idx
            raw_value = search_region[value_start:value_end]

            try:
                parsed = self.parser.parse_value(data_type, raw_value)
                if isinstance(parsed, (int, float)):
                    return float(parsed)
            except (NGBParseError, ValueError):
                pass

            idx = end_idx

        return None

    def _classify_crucible_occurrences(
        self, occurrences: list[dict[str, Any]], combined_data: bytes
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Classify crucible mass occurrences by their structural context.

        Args:
            occurrences: List of crucible mass occurrences with byte positions
            combined_data: Combined binary data for context analysis

        Returns:
            Tuple of (sample_occurrences, reference_occurrences, zero_occurrences)
        """
        sample_sig_occ: list[dict[str, Any]] = []
        ref_sig_occ: list[dict[str, Any]] = []
        zero_occ: list[dict[str, Any]] = []

        for occ in occurrences:
            start = occ["byte_pos"]
            pre = combined_data[max(0, start - CRUCIBLE_MASS_PREVIEW_SIZE) : start]

            # Classify by signature fragments
            if SAMPLE_SIG_FRAGMENT in pre:
                sample_sig_occ.append(occ)
            elif REF_CRUCIBLE_SIG_FRAGMENT in pre:
                ref_sig_occ.append(occ)
            elif (
                abs(occ["value"]) < 1e-12
            ):  # Only use zero as fallback, not for searching
                zero_occ.append(occ)

        return sample_sig_occ, ref_sig_occ, zero_occ

    def _extract_crucible_masses_structural(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> None:
        """Extract crucible masses using structural parsing instead of value heuristics.

        This method replaces the complex heuristic-based approach with proper
        structural parsing that looks for specific binary patterns and signatures.
        """
        if not tables:
            return

        combined_data = b"".join(tables)
        crucible_pattern = self._compiled_meta.get("crucible_mass")

        if not crucible_pattern:
            return

        # Find all crucible mass occurrences
        occurrences: list[dict[str, Any]] = []
        for match in crucible_pattern.finditer(combined_data):
            try:
                data_type, value_bytes = match.groups()
                value = self.parser.parse_value(data_type, value_bytes)
                if isinstance(value, (int, float)):
                    occurrences.append(
                        {
                            "byte_pos": match.start(),
                            "value": float(value),
                            "match": match,
                        }
                    )
            except (ValueError, NGBParseError):
                continue

        if not occurrences:
            return

        # Classify occurrences by structural context
        sample_occ, ref_occ, zero_occ = self._classify_crucible_occurrences(
            occurrences, combined_data
        )

        # Extract sample crucible mass (highest priority)
        if sample_occ:
            sample_occ_sorted = sorted(sample_occ, key=lambda o: o["byte_pos"])
            sample_occ_first = sample_occ_sorted[0]
            metadata["crucible_mass"] = sample_occ_first["value"]

            # Try to extract sample_mass from preceding field if not already present
            if "sample_mass" not in metadata:
                sample_mass = self._extract_structural_field_value(
                    combined_data, sample_occ_first["byte_pos"]
                )
                if sample_mass is not None:
                    metadata["sample_mass"] = sample_mass

        # Extract reference crucible mass
        if ref_occ:
            ref_occ_sorted = sorted(ref_occ, key=lambda o: o["byte_pos"])
            ref_occ_first = ref_occ_sorted[0]
            metadata["reference_crucible_mass"] = ref_occ_first["value"]

            # Try to extract reference_mass from preceding field
            ref_mass = self._extract_structural_field_value(
                combined_data, ref_occ_first["byte_pos"]
            )
            if ref_mass is not None:
                metadata["reference_mass"] = ref_mass

        # Fallback: use zero value as reference if no reference found
        if (
            "crucible_mass" in metadata
            and "reference_crucible_mass" not in metadata
            and zero_occ
        ):
            zero_occ_sorted = sorted(zero_occ, key=lambda o: o["byte_pos"])
            metadata["reference_crucible_mass"] = zero_occ_sorted[0]["value"]

        # Final fallback: use first occurrence if no sample crucible mass found
        if "crucible_mass" not in metadata and occurrences:
            first_occ = sorted(occurrences, key=lambda o: o["byte_pos"])[0]
            metadata["crucible_mass"] = first_occ["value"]
