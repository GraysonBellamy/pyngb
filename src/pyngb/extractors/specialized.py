"""
Specialized extractors for various NGB metadata types.

This module contains extractors for MFC (Mass Flow Controller) metadata,
PID control parameters, calibration constants, and application/license information.
"""

import re
import struct
from typing import Any, ClassVar

from ..binary import BinaryParser
from ..constants import (
    APP_LICENSE_CATEGORY,
    APP_LICENSE_FIELD,
    GAS_TYPES,
    MFC_FIELD_NAMES,
    SENSITIVITY_RECORD_SUFFIX,
    STRING_DATA_TYPE,
    TEMP_CAL_COEFF_SIGNATURE,
    TEMP_CAL_FIXPOINT_CATEGORIES,
    TEMP_CAL_RECORD_SUFFIX,
    TEMP_PROG_TYPE_PREFIX,
    PatternConfig,
    PatternOffsets,
    TemperatureCalibration,
    TemperatureFixpoint,
)
from .base import BaseMetadataExtractor, FileMetadata

__all__ = [
    "ApplicationLicenseExtractor",
    "CalibrationExtractor",
    "MFCExtractor",
    "PIDParameterExtractor",
    "TemperatureCalibrationExtractor",
]


class MFCExtractor(BaseMetadataExtractor):
    """Extracts Mass Flow Controller (MFC) metadata.

    This extractor handles the complex task of extracting MFC gas types and ranges
    using structural parsing and signature identification.
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("MFC Metadata")
        self.config = config
        self.parser = parser
        self.pattern_offsets = PatternOffsets()

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if MFC metadata can be extracted."""
        if not tables:
            return False

        combined_data = b"".join(tables)

        # Check for MFC field names
        for field_name in MFC_FIELD_NAMES:
            field_bytes = field_name.encode("utf-16le")
            if field_bytes in combined_data:
                return True

        # Check for MFC signatures
        return bool(self._has_mfc_signature_in_data(combined_data))

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract MFC metadata from tables."""
        self.log_extraction_attempt(len(tables))

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
            extracted_count = 0
            if mfc_fields:
                for key, value in mfc_fields.items():
                    if key.endswith("_mfc_gas") and isinstance(value, str):
                        if key == "purge_1_mfc_gas":
                            metadata["purge_1_mfc_gas"] = value
                            extracted_count += 1
                        elif key == "purge_2_mfc_gas":
                            metadata["purge_2_mfc_gas"] = value
                            extracted_count += 1
                        elif key == "protective_mfc_gas":
                            metadata["protective_mfc_gas"] = value
                            extracted_count += 1
                    elif key.endswith("_mfc_range") and isinstance(value, float):
                        if key == "purge_1_mfc_range":
                            metadata["purge_1_mfc_range"] = value
                            extracted_count += 1
                        elif key == "purge_2_mfc_range":
                            metadata["purge_2_mfc_range"] = value
                            extracted_count += 1
                        elif key == "protective_mfc_range":
                            metadata["protective_mfc_range"] = value
                            extracted_count += 1

            if extracted_count > 0:
                self.log_extraction_success(extracted_count)
            else:
                self.logger.debug("No MFC fields extracted")

        except Exception as e:
            self.log_extraction_failure(e)

    def _has_mfc_signature_in_data(self, data: bytes) -> bool:
        """Check if data contains MFC signature patterns."""
        for j in range(len(data) - 4):
            if data[j : j + 3] == TEMP_PROG_TYPE_PREFIX:
                sig_bytes = data[j + 3 : j + 5]
                if len(sig_bytes) == 2:
                    try:
                        sig_val = struct.unpack("<H", sig_bytes)[0]
                        if sig_val == self.pattern_offsets.MFC_SIGNATURE:
                            return True
                    except struct.error:
                        continue
        return False

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
            if self._has_mfc_signature(table_data):
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
                    try:
                        sig_val = struct.unpack("<H", sig_bytes)[0]
                        if sig_val == self.pattern_offsets.MFC_SIGNATURE:
                            return True
                    except struct.error:
                        continue
        return False

    def _extract_mfc_range_value(self, table_data: bytes) -> float | None:
        """Extract MFC range value using structural parsing."""
        for j in range(len(table_data) - 4):
            if table_data[j : j + 3] == TEMP_PROG_TYPE_PREFIX:
                sig_bytes = table_data[j + 3 : j + 5]
                if len(sig_bytes) == 2:
                    try:
                        sig_val = struct.unpack("<H", sig_bytes)[0]
                        if sig_val == self.pattern_offsets.MFC_SIGNATURE:
                            # Look for float value after signature
                            for offset in range(5, min(50, len(table_data) - j)):
                                test_pos = j + offset
                                if test_pos + 4 <= len(table_data):
                                    try:
                                        float_val = struct.unpack(
                                            "<f", table_data[test_pos : test_pos + 4]
                                        )[0]
                                        # Validate reasonable flow rate value
                                        if 0.1 <= float_val <= 1000.0:
                                            return float(float_val)
                                    except struct.error:
                                        continue
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
                    if table_data[1] == self.pattern_offsets.GAS_CONTEXT_SIGNATURE:
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


class PIDParameterExtractor(BaseMetadataExtractor):
    """Extracts PID control parameters (XP, TN, TV) for furnace and sample."""

    # Binary signatures for PID control parameters
    PID_SIGNATURES: ClassVar[list[tuple[int, str]]] = [
        (0x0FE7, "xp"),  # proportional gain
        (0x0FE8, "tn"),  # integral time
        (0x0FE9, "tv"),  # derivative time
    ]

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("PID Parameters")
        self.config = config
        self.parser = parser

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if PID parameters can be extracted."""
        if not tables:
            return False

        combined_data = b"".join(tables)

        # Check for PID signatures
        for sig_val, _ in self.PID_SIGNATURES:
            sig_bytes = struct.pack("<H", sig_val)
            pattern = b"\x03\x80\x01" + sig_bytes
            if pattern in combined_data:
                return True

        return False

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract PID control parameters from tables."""
        self.log_extraction_attempt(len(tables))

        try:
            combined_data = b"".join(tables)
            matches = self._scan_pid_parameters(combined_data)

            if not matches:
                self.logger.debug("No PID parameters found")
                return

            # Group by parameter name
            xp_params = [p for p in matches if p["param_name"] == "xp"]
            tn_params = [p for p in matches if p["param_name"] == "tn"]
            tv_params = [p for p in matches if p["param_name"] == "tv"]

            # Sort by position to preserve occurrence order
            xp_params.sort(key=lambda x: x["position"])
            tn_params.sort(key=lambda x: x["position"])
            tv_params.sort(key=lambda x: x["position"])

            extracted_count = 0

            # Furnace = first occurrence; Sample = second occurrence
            if len(xp_params) >= 1:
                metadata["furnace_xp"] = xp_params[0]["value"]
                extracted_count += 1
            if len(tn_params) >= 1:
                metadata["furnace_tn"] = tn_params[0]["value"]
                extracted_count += 1
            if len(tv_params) >= 1:
                metadata["furnace_tv"] = tv_params[0]["value"]
                extracted_count += 1

            if len(xp_params) >= 2:
                metadata["sample_xp"] = xp_params[1]["value"]
                extracted_count += 1
            if len(tn_params) >= 2:
                metadata["sample_tn"] = tn_params[1]["value"]
                extracted_count += 1
            if len(tv_params) >= 2:
                metadata["sample_tv"] = tv_params[1]["value"]
                extracted_count += 1

            if extracted_count > 0:
                self.log_extraction_success(extracted_count)
            else:
                self.logger.debug("No PID parameters extracted")

        except Exception as e:
            self.log_extraction_failure(e)

    def _scan_pid_parameters(self, data: bytes) -> list[dict[str, Any]]:
        """Scan binary data for PID control parameters."""
        control_params: list[dict[str, Any]] = []

        for sig_val, param_name in self.PID_SIGNATURES:
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


class CalibrationExtractor(BaseMetadataExtractor):
    """Extracts calibration constants (p0-p5)."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Calibration Constants")
        self.config = config
        self.parser = parser
        self._compiled_cal_consts: dict[str, re.Pattern[bytes]] = {}

        # Compile calibration constant patterns
        self._compile_calibration_patterns()

    def _compile_calibration_patterns(self) -> None:
        """Compile regex patterns for calibration constants."""
        TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
        TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR
        END_FIELD = self.parser.markers.END_FIELD

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

        self.logger.debug(
            f"Compiled {len(self._compiled_cal_consts)} calibration patterns"
        )

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if calibration constants can be extracted."""
        if not tables:
            return False

        # Check for calibration category marker
        CATEGORY = b"\xf5\x01"
        return any(CATEGORY in table for table in tables)

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract calibration constants from tables."""
        self.log_extraction_attempt(len(tables))

        CATEGORY = b"\xf5\x01"
        extracted_count = 0

        for table in tables:
            if CATEGORY not in table:
                continue

            cal_constants: dict[str, float] = {}

            for field_name, pattern in self._compiled_cal_consts.items():
                try:
                    match = pattern.search(table)
                    if match:
                        data_type, value_bytes = match.groups()
                        value = self.parser.parse_value(data_type, value_bytes)
                        if value is not None and isinstance(value, (int, float)):
                            cal_constants[field_name] = float(value)
                            extracted_count += 1
                except Exception as e:
                    self.logger.debug(
                        f"Error extracting calibration constant {field_name}: {e}"
                    )
                    continue

            if cal_constants:
                metadata["calibration_constants"] = cal_constants
                break  # Take first table with calibration constants

        if extracted_count > 0:
            self.log_extraction_success(extracted_count)
        else:
            self.logger.debug("No calibration constants extracted")


class TemperatureCalibrationExtractor(BaseMetadataExtractor):
    """Extracts the temperature-calibration block for traceability/QA only.

    Pulls three things from ``stream_1``:

    1. ``coefficients`` - the [B0, B1, B2] polynomial stored as a float32 data
       array on field ``be 04`` inside an ``f7 01`` table.
    2. ``fixpoints`` - the phase-transition standards stored in the ``30 75`` ..
       ``34 75`` tables. Each is one row of the Proteus calibration table:
       ``actual`` vs ``measured`` with a ``weight``, producing a ``corrected``
       value (``corrected = measured + correction(measured)``). Standards vary per
       calibration and are read from the file, never hard-coded.
    3. record paths - the external temperature-calibration record (``.ngb-ts3``)
       and the DSC sensitivity record (``.ngb-es3``).

    IMPORTANT: The ``sample_temperature`` channel stored in NGB files is already
    temperature-corrected by Proteus, so these coefficients must NOT be applied to
    it (that would double-correct). They are extracted for provenance only. The
    Proteus correction (NOT applied here) is::

        correction[°C] = 1e-3*B0 + 1e-5*B1*T_exp + 1e-8*B2*T_exp**2

    The DSC sensitivity calibration (see ``CalibrationExtractor``) remains the only
    calibration that genuinely must be applied downstream.
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Temperature Calibration")
        self.config = config
        self.parser = parser
        self._compiled_fields: dict[str, re.Pattern[bytes]] = {}
        self._compile_fixpoint_patterns()

    def _compile_fixpoint_patterns(self) -> None:
        """Compile scalar-field regexes for each fixpoint field id."""
        TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
        TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR
        END_FIELD = self.parser.markers.END_FIELD

        for fname, field_id in self.config.temperature_cal_patterns.items():
            pat = (
                re.escape(field_id)
                + rb".{0,12}?"
                + re.escape(TYPE_PREFIX)
                + rb"(.)"
                + re.escape(TYPE_SEPARATOR)
                + rb"(.+?)"
                + re.escape(END_FIELD)
            )
            self._compiled_fields[fname] = re.compile(pat, re.DOTALL)

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if temperature-calibration data is present."""
        if not tables:
            return False
        combined = b"".join(tables)
        return (
            TEMP_CAL_COEFF_SIGNATURE in combined
            or TEMP_CAL_RECORD_SUFFIX.encode("utf-16le") in combined
            or SENSITIVITY_RECORD_SUFFIX.encode("utf-16le") in combined
        )

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract the temperature-calibration block from tables."""
        self.log_extraction_attempt(len(tables))

        try:
            combined = b"".join(tables)

            cal: TemperatureCalibration = {}

            coefficients = self._extract_coefficients(combined)
            if coefficients is not None:
                cal["coefficients"] = coefficients

            fixpoints = self._extract_fixpoints(tables)
            if fixpoints:
                cal["fixpoints"] = fixpoints

            record_path = self._extract_path(combined, TEMP_CAL_RECORD_SUFFIX)
            if record_path:
                cal["record_path"] = record_path

            extracted_count = 0
            if cal:
                metadata["temperature_calibration"] = cal
                extracted_count += len(cal)

            sensitivity_path = self._extract_path(combined, SENSITIVITY_RECORD_SUFFIX)
            if sensitivity_path:
                metadata["sensitivity_record_path"] = sensitivity_path
                extracted_count += 1

            if extracted_count > 0:
                self.log_extraction_success(extracted_count)
            else:
                self.logger.debug("No temperature-calibration data extracted")

        except Exception as e:
            self.log_extraction_failure(e)

    def _extract_coefficients(self, data: bytes) -> list[float] | None:
        """Decode the be 04 float32 data array of calibration coefficients."""
        idx = data.find(TEMP_CAL_COEFF_SIGNATURE)
        if idx == -1:
            return None
        pos = idx + len(TEMP_CAL_COEFF_SIGNATURE)
        if pos + 4 > len(data):
            return None
        count = struct.unpack("<I", data[pos : pos + 4])[0]
        pos += 4
        if count <= 0 or count % 4 or pos + count > len(data):
            self.logger.debug(f"Invalid coefficient array length: {count}")
            return None
        n = count // 4
        return [float(x) for x in struct.unpack(f"<{n}f", data[pos : pos + count])]

    def _extract_fixpoints(self, tables: list[bytes]) -> list[TemperatureFixpoint]:
        """Extract fixpoint standards in standard order (ascending temperature).

        Each standard lives in its own table categorised ``30 75`` .. ``34 75``.
        Because the ``30 75`` category is reused by the sample tables, a fixpoint
        table is confirmed by also carrying the actual- and corrected-temperature
        fields - the sample tables do not.
        """
        actual_id = self.config.temperature_cal_patterns["actual_c"]
        corrected_id = self.config.temperature_cal_patterns["corrected_c"]

        fixpoints: list[TemperatureFixpoint] = []
        for category in TEMP_CAL_FIXPOINT_CATEGORIES:
            for table in tables:
                if (
                    table[:32].find(category) == -1
                    or actual_id not in table
                    or corrected_id not in table
                ):
                    continue
                row = self._parse_fixpoint_row(table)
                if row:
                    fixpoints.append(row)
                break  # one table per category
        return fixpoints

    def _parse_fixpoint_row(self, table: bytes) -> TemperatureFixpoint:
        """Parse the scalar fields of a single fixpoint table."""
        row: TemperatureFixpoint = {}
        for fname, pattern in self._compiled_fields.items():
            match = pattern.search(table)
            if not match:
                continue
            data_type, value_bytes = match.groups()
            value = self.parser.parse_value(data_type, value_bytes)
            if value is None:
                continue
            if fname == "name" and isinstance(value, str):
                name = value.strip()
                if name:
                    row["name"] = name
            elif fname != "name" and isinstance(value, (int, float)):
                row[fname] = float(value)  # type: ignore[literal-required]
        return row

    @staticmethod
    def _extract_path(data: bytes, suffix: str) -> str | None:
        """Recover a UTF-16LE record path ending in ``suffix``."""
        marker = suffix.encode("utf-16le")
        idx = data.find(marker)
        if idx == -1:
            return None
        end = idx + len(marker)
        # Walk backwards over printable UTF-16LE characters to the path start.
        start = idx
        while start >= 2 and data[start - 1] == 0 and 32 <= data[start - 2] < 127:
            start -= 2
        text = data[start:end].decode("utf-16le", errors="ignore")
        return text or None


class ApplicationLicenseExtractor(BaseMetadataExtractor):
    """Extracts application version and license information."""

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Application & License")
        self.config = config
        self.parser = parser
        self.pattern_offsets = PatternOffsets()

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if application/license info can be extracted."""
        if not tables:
            return False

        combined_data = b"".join(tables)

        # Check for application/license category and field markers
        return bool(
            APP_LICENSE_CATEGORY in combined_data and APP_LICENSE_FIELD in combined_data
        )

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract application version and license information."""
        self.log_extraction_attempt(len(tables))

        try:
            combined_data = b"".join(tables)
            extracted_count = 0

            pattern = re.compile(
                re.escape(APP_LICENSE_CATEGORY)
                + rb".{0,"
                + str(self.pattern_offsets.APP_LICENSE_SEARCH_RANGE).encode()
                + rb"}?"
                + re.escape(APP_LICENSE_FIELD)
                + rb".{0,"
                + str(self.pattern_offsets.APP_LICENSE_SEARCH_RANGE).encode()
                + rb"}?"
                + re.escape(self.parser.markers.TYPE_PREFIX)
                + rb"(.)"
                + re.escape(self.parser.markers.TYPE_SEPARATOR)
                + rb"(.*?)"
                + re.escape(self.parser.markers.END_FIELD),
                re.DOTALL,
            )

            strings: list[str] = []
            for m in pattern.finditer(combined_data):
                dt, val = m.groups()
                if dt != STRING_DATA_TYPE:
                    continue
                parsed = self.parser.parse_value(dt, val)
                if isinstance(parsed, str) and parsed:
                    strings.append(parsed)

            if not strings:
                self.logger.debug("No application/license strings found")
                return

            # application_version: match leading 'Version x.y.z'
            app = next(
                (s for s in strings if re.match(r"^\s*Version\s+\d+\.\d+\.\d+", s)),
                None,
            )
            if app and "application_version" not in metadata:
                metadata["application_version"] = app
                extracted_count += 1

            # licensed_to: pick multi-line non-Version string
            license_candidates = [
                s
                for s in strings
                if ("\n" in s and not s.lstrip().startswith("Version"))
            ]
            if license_candidates and "licensed_to" not in metadata:
                # Choose the longest reasonable candidate
                metadata["licensed_to"] = max(license_candidates, key=len)
                extracted_count += 1

            if extracted_count > 0:
                self.log_extraction_success(extracted_count)
            else:
                self.logger.debug("No application/license info extracted")

        except Exception as e:
            self.log_extraction_failure(e)
