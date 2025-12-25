"""
Mass extractor for NGB metadata including complex crucible mass disambiguation.

This extractor handles the complex logic for distinguishing between sample and
reference crucible masses using structural binary signatures.
"""

import re
from typing import Any

from ..binary import BinaryParser
from ..constants import (
    PatternOffsets,
    REF_CRUCIBLE_SIG_FRAGMENT,
    SAMPLE_CRUCIBLE_SIG_FRAGMENT,
    PatternConfig,
)
from ..exceptions import NGBParseError
from .base import BaseMetadataExtractor, FileMetadata

__all__ = ["MassExtractor"]


class MassExtractor(BaseMetadataExtractor):
    """Extracts mass-related metadata fields using structural parsing.

    This extractor handles the complex task of distinguishing between:
    - sample_mass and crucible_mass (sample side)
    - reference_mass and reference_crucible_mass (reference side)

    The distinction is made using binary signature fragments that appear
    before the mass values in the NGB file structure.

    Example:
        >>> config = PatternConfig()
        >>> parser = BinaryParser()
        >>> extractor = MassExtractor(config, parser)
        >>> metadata = {}
        >>> extractor.extract([table_data], metadata)
        >>> print(metadata['sample_mass'], metadata['crucible_mass'])
    """

    def __init__(self, config: PatternConfig, parser: BinaryParser) -> None:
        super().__init__("Mass Fields")
        self.config = config
        self.parser = parser
        self.pattern_offsets = PatternOffsets()
        self._compiled_crucible_pattern: re.Pattern[bytes] | None = None
        self._compiled_sample_pattern: re.Pattern[bytes] | None = None

        # Compile mass-related patterns
        self._compile_mass_patterns()

    def _compile_mass_patterns(self) -> None:
        """Compile regex patterns for mass field extraction."""
        END_FIELD = self.parser.markers.END_FIELD
        TYPE_PREFIX = self.parser.markers.TYPE_PREFIX
        TYPE_SEPARATOR = self.parser.markers.TYPE_SEPARATOR

        # Crucible mass pattern (handles both sample and reference)
        if "crucible_mass" in self.config.metadata_patterns:
            category, field_bytes = self.config.metadata_patterns["crucible_mass"]
            pattern = (
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
            self._compiled_crucible_pattern = re.compile(pattern, re.DOTALL)

        # Sample mass pattern
        if "sample_mass" in self.config.metadata_patterns:
            category, field_bytes = self.config.metadata_patterns["sample_mass"]
            pattern = (
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
            self._compiled_sample_pattern = re.compile(pattern, re.DOTALL)

        self.logger.debug("Compiled mass extraction patterns")

    def can_extract(self, tables: list[bytes]) -> bool:
        """Check if mass fields can be extracted from the tables.

        Args:
            tables: List of binary table data

        Returns:
            True if mass-related patterns might be found
        """
        if not tables:
            return False

        # Check for mass-related binary patterns
        mass_markers = [
            self.config.metadata_patterns.get("sample_mass", (b"", b""))[1],
            self.config.metadata_patterns.get("crucible_mass", (b"", b""))[1],
        ]

        combined_data = b"".join(tables)

        for marker in mass_markers:
            if marker and marker in combined_data:
                return True

        # Also check for signature fragments
        return bool(
            SAMPLE_CRUCIBLE_SIG_FRAGMENT in combined_data
            or REF_CRUCIBLE_SIG_FRAGMENT in combined_data
        )

    def extract(self, tables: list[bytes], metadata: FileMetadata) -> None:
        """Extract mass metadata fields from tables.

        Args:
            tables: List of binary table data from NGB streams
            metadata: Metadata dictionary to update (modified in-place)
        """
        self.log_extraction_attempt(len(tables))

        if not tables:
            return

        extracted_count = 0

        # Extract simple sample mass first
        extracted_count += self._extract_simple_sample_mass(tables, metadata)

        # Extract complex crucible masses with structural disambiguation
        extracted_count += self._extract_crucible_masses_structural(tables, metadata)

        if extracted_count > 0:
            self.log_extraction_success(extracted_count)
        else:
            self.logger.debug("No mass fields extracted")

    def _extract_simple_sample_mass(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> int:
        """Extract simple sample mass field.

        Args:
            tables: List of binary table data
            metadata: Metadata dictionary to update

        Returns:
            Number of fields extracted
        """
        if "sample_mass" in metadata or not self._compiled_sample_pattern:
            return 0

        for table in tables:
            try:
                matches = self._compiled_sample_pattern.findall(table)
                if matches:
                    data_type, value_bytes = matches[0]
                    value = self.parser.parse_value(data_type, value_bytes)
                    if isinstance(value, (int, float)) and value > 0:
                        metadata["sample_mass"] = float(value)
                        self.logger.debug(f"Extracted sample_mass: {value}")
                        return 1
            except Exception as e:
                self.logger.debug(f"Error extracting sample_mass: {e}")
                continue

        return 0

    def _extract_crucible_masses_structural(
        self, tables: list[bytes], metadata: FileMetadata
    ) -> int:
        """Extract crucible masses using structural parsing.

        This method implements the complex logic for distinguishing between
        sample and reference crucible masses using binary signature fragments.

        Args:
            tables: List of binary table data
            metadata: Metadata dictionary to update

        Returns:
            Number of fields extracted
        """
        if not self._compiled_crucible_pattern:
            return 0

        combined_data = b"".join(tables)
        extracted_count = 0

        # Find all crucible mass occurrences
        occurrences = self._find_crucible_occurrences(combined_data)
        if not occurrences:
            return 0

        # Classify occurrences by structural context
        sample_occ, ref_occ, zero_occ = self._classify_crucible_occurrences(
            occurrences, combined_data
        )

        # Extract sample crucible mass (highest priority)
        if sample_occ and "crucible_mass" not in metadata:
            sample_occ_sorted = sorted(sample_occ, key=lambda o: o["byte_pos"])
            sample_occ_first = sample_occ_sorted[0]
            metadata["crucible_mass"] = sample_occ_first["value"]
            extracted_count += 1
            self.logger.debug(f"Extracted crucible_mass: {sample_occ_first['value']}")

            # Try to extract sample_mass from preceding field if not already present
            if "sample_mass" not in metadata:
                sample_mass = self._extract_structural_field_value(
                    combined_data, sample_occ_first["byte_pos"]
                )
                if sample_mass is not None:
                    metadata["sample_mass"] = sample_mass
                    extracted_count += 1
                    self.logger.debug(
                        f"Extracted sample_mass from structure: {sample_mass}"
                    )

        # Extract reference crucible mass
        if ref_occ and "reference_crucible_mass" not in metadata:
            ref_occ_sorted = sorted(ref_occ, key=lambda o: o["byte_pos"])
            ref_occ_first = ref_occ_sorted[0]
            metadata["reference_crucible_mass"] = ref_occ_first["value"]
            extracted_count += 1
            self.logger.debug(
                f"Extracted reference_crucible_mass: {ref_occ_first['value']}"
            )

            # Try to extract reference_mass from preceding field
            ref_mass = self._extract_structural_field_value(
                combined_data, ref_occ_first["byte_pos"]
            )
            if ref_mass is not None:
                metadata["reference_mass"] = ref_mass
                extracted_count += 1
                self.logger.debug(
                    f"Extracted reference_mass from structure: {ref_mass}"
                )

        # Fallback: use zero value as reference if no reference found
        if (
            "crucible_mass" in metadata
            and "reference_crucible_mass" not in metadata
            and zero_occ
        ):
            zero_occ_sorted = sorted(zero_occ, key=lambda o: o["byte_pos"])
            metadata["reference_crucible_mass"] = zero_occ_sorted[0]["value"]
            extracted_count += 1
            self.logger.debug("Used zero value as reference_crucible_mass fallback")

        # Final fallback: use first occurrence if no sample crucible mass found
        if "crucible_mass" not in metadata and occurrences:
            first_occ = sorted(occurrences, key=lambda o: o["byte_pos"])[0]
            metadata["crucible_mass"] = first_occ["value"]
            extracted_count += 1
            self.logger.debug(
                f"Used first occurrence as crucible_mass fallback: {first_occ['value']}"
            )

        return extracted_count

    def _find_crucible_occurrences(self, combined_data: bytes) -> list[dict[str, Any]]:
        """Find all crucible mass occurrences in the data.

        Args:
            combined_data: Combined binary data from all tables

        Returns:
            List of occurrence dictionaries with byte_pos, value, and match
        """
        if not self._compiled_crucible_pattern:
            return []

        occurrences: list[dict[str, Any]] = []

        for match in self._compiled_crucible_pattern.finditer(combined_data):
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

        self.logger.debug(f"Found {len(occurrences)} crucible mass occurrences")
        return occurrences

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
            preview_start = max(
                0, start - self.pattern_offsets.CRUCIBLE_MASS_PREVIEW_SIZE
            )
            preview_data = combined_data[preview_start:start]

            # Classify by signature fragments
            if SAMPLE_CRUCIBLE_SIG_FRAGMENT in preview_data:
                sample_sig_occ.append(occ)
            elif REF_CRUCIBLE_SIG_FRAGMENT in preview_data:
                ref_sig_occ.append(occ)
            elif abs(occ["value"]) < 1e-12:  # Zero values as fallback
                zero_occ.append(occ)

        self.logger.debug(
            f"Classified: {len(sample_sig_occ)} sample, {len(ref_sig_occ)} reference, {len(zero_occ)} zero"
        )
        return sample_sig_occ, ref_sig_occ, zero_occ

    def _extract_structural_field_value(
        self, data: bytes, start_pos: int
    ) -> float | None:
        """Extract a field value using structural parsing from a given position.

        Walks backwards from start_pos to find the most recent complete field
        with pattern: TYPE_PREFIX <dtype> ... TYPE_SEPARATOR <value> END_FIELD

        Args:
            data: Binary data to search in
            start_pos: Position to start searching backwards from

        Returns:
            Parsed numeric value if found, None otherwise
        """
        search_window = self.pattern_offsets.CRUCIBLE_MASS_SEARCH_WINDOW
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
