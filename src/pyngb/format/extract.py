"""Metadata extraction: FileMetadata as queries over the document model.

``build_metadata`` applies the declarative :data:`~pyngb.format.maps.FIELD_MAP`
first, then eight plain extraction functions. Each function is one metadata
concern expressed against tables and fields; adding a Phase-2 field means
adding a function to :data:`_EXTRACTORS` (or an entry to FIELD_MAP), nothing
else. Every function is wrapped in a warn-and-continue net — all FileMetadata
fields are optional by contract, so a single misbehaving rule never sinks the
rest of the extraction.

The rules preserve the legacy extractor semantics exactly (the parity goldens
pin them): first-match-wins in stream order, occurrence-order classification
for PID and crucible masses, ordinal pairing for MFC controllers.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator
from datetime import datetime, timezone

import numpy as np

from ..constants import (
    FileMetadata,
    SensitivityCalibration,
    TemperatureCalibration,
    TemperatureFixpoint,
)
from .document import NGBDocument, Table
from .grammar import DType
from .maps import (
    APP_LICENSE_CATEGORY,
    CAL_CONSTANTS,
    CAL_CONSTANTS_CATEGORY,
    CORRECTION_LINK_CATEGORY,
    CORRECTION_LINK_FIELD,
    CRUCIBLE_CATEGORY,
    CRUCIBLE_MASS_FIELD,
    FIELD_MAP,
    FIXPOINT_CATEGORIES,
    FIXPOINT_FIELDS,
    GAS_CONTEXT_HIGH_BYTE,
    GAS_TYPES,
    KNOWN_FIELD_IDS,  # noqa: F401  (re-exported for census tooling)
    MFC_FIELD_NAMES,
    MFC_FLOW_PARAM_NAMES,
    MFC_FLOW_VALUE_FIELD,
    MFC_RANGE_FIELD,
    PID_FIELDS,
    PROVENANCE_FIELDS,
    REF_NEIGHBOR_FIELD,
    SAMPLE_NEIGHBOR_FIELD,
    SENSITIVITY_SUFFIX,
    STAGE_FIELDS,
    TEMP_CAL_CATEGORY,
    TEMP_CAL_COEFF_FIELD,
    TEMP_CAL_SUFFIX,
    TIMEZONE_CATEGORY,
    TIMEZONE_FIELDS,
)

__all__ = ["build_metadata"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

#: The metadata stream.
_STREAM = 1

#: Scalar dtypes the legacy parser decoded to numbers; "last numeric field"
#: rules must skip everything else (u16/u8 came back as raw bytes and were
#: never accepted).
_NUMERIC_DTYPES = frozenset({DType.I32, DType.F32, DType.F64})

_VERSION_RE = re.compile(r"^\s*Version\s+\d+\.\d+\.\d+")


def _numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _apply_field_map(doc: NGBDocument, metadata: FileMetadata) -> None:
    """First stream-1 table of the category carrying the field wins."""
    for meta in FIELD_MAP:
        if meta.key in metadata:
            continue
        table = doc.first(_STREAM, category=meta.category, with_fields=(meta.field_id,))
        if table is None:
            continue
        value = meta.convert(table.value(meta.field_id))
        if value is not None:
            metadata[meta.key] = value  # type: ignore[literal-required]


# -- Masses --------------------------------------------------------------------


def _last_numeric_value(table: Table) -> float | None:
    """The last numeric scalar of a table (legacy backwards-walk equivalent)."""
    for entry in reversed(table.fields.values()):
        if entry.dtype in _NUMERIC_DTYPES and entry.value is not None:
            return _numeric(entry.value)
    return None


def extract_masses(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Crucible/reference masses via neighbor-table classification.

    Both crucible tables share category 0x177E and field 0x0C9E; the trailing
    field of the PRECEDING table tells them apart (0x0C83 -> sample crucible
    follows, 0x10C4 -> reference crucible follows). ``reference_mass`` is the
    last numeric scalar of the table preceding the reference crucible table.
    Fallbacks ported from the legacy extractor: a zero-valued unclassified
    occurrence stands in for the reference crucible; the first occurrence
    stands in for the sample crucible when classification fails entirely.
    """
    tables = doc.tables_of(_STREAM)
    occurrences = [
        (table, value)
        for table in doc.find(
            _STREAM, category=CRUCIBLE_CATEGORY, with_fields=(CRUCIBLE_MASS_FIELD,)
        )
        if (value := _numeric(table.value(CRUCIBLE_MASS_FIELD))) is not None
    ]
    if not occurrences:
        return

    sample_occ: list[tuple[Table, float]] = []
    ref_occ: list[tuple[Table, float]] = []
    zero_occ: list[tuple[Table, float]] = []
    for table, value in occurrences:
        neighbor = tables[table.index - 1] if table.index > 0 else None
        if neighbor is not None and neighbor.has_fields(SAMPLE_NEIGHBOR_FIELD):
            sample_occ.append((table, value))
        elif neighbor is not None and neighbor.has_fields(REF_NEIGHBOR_FIELD):
            ref_occ.append((table, value))
        elif abs(value) < 1e-12:
            zero_occ.append((table, value))

    if sample_occ and "crucible_mass" not in metadata:
        sample_table, sample_value = sample_occ[0]
        metadata["crucible_mass"] = sample_value
        # Structural fallback (matters for baseline files, where the sample
        # table's own mass field is rejected as non-positive): the last
        # numeric of the table preceding the sample crucible table — the
        # -1000.0 no-sample sentinel in .ngb-bs3 fixtures. No positivity
        # check, faithfully to the legacy walk.
        if "sample_mass" not in metadata and sample_table.index > 0:
            sample_mass = _last_numeric_value(tables[sample_table.index - 1])
            if sample_mass is not None:
                metadata["sample_mass"] = sample_mass

    if ref_occ and "reference_crucible_mass" not in metadata:
        ref_table, ref_value = ref_occ[0]
        metadata["reference_crucible_mass"] = ref_value
        if ref_table.index > 0:
            reference_mass = _last_numeric_value(tables[ref_table.index - 1])
            if reference_mass is not None:
                metadata["reference_mass"] = reference_mass

    if (
        "crucible_mass" in metadata
        and "reference_crucible_mass" not in metadata
        and zero_occ
    ):
        metadata["reference_crucible_mass"] = zero_occ[0][1]

    if "crucible_mass" not in metadata:
        metadata["crucible_mass"] = occurrences[0][1]


# -- Temperature program ---------------------------------------------------------


def extract_temperature_program(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Stage N = the Nth table carrying all five stage fields (times x60).

    Only f32-typed values are included: stage_type is stored as i32 and the
    legacy extractor's pattern hard-coded the f32 dtype byte, so it never
    captured it — the goldens pin stages without a stage_type key. (All five
    fields still identify a stage table; decoding stage_type is Phase-2.)
    """
    program: dict[str, dict[str, float]] = {}
    stage_tables = doc.find(_STREAM, with_fields=tuple(STAGE_FIELDS.values()))
    for index, table in enumerate(stage_tables):
        stage: dict[str, float] = {}
        for name, field_id in STAGE_FIELDS.items():
            entry = table.get(field_id)
            if entry is None or entry.dtype != DType.F32:
                continue
            value = _numeric(entry.value)
            if value is None:
                continue
            # Stage durations are stored in minutes; the public API exposes
            # seconds, consistent with the time column.
            stage[name] = value * 60.0 if name == "time" else value
        if stage:
            program[f"stage_{index}"] = stage
    if program:
        metadata["temperature_program"] = program  # type: ignore[typeddict-item]


# -- PID control parameters --------------------------------------------------------


def extract_pid(doc: NGBDocument, metadata: FileMetadata) -> None:
    """First table with all three PID fields = furnace, second = sample."""
    pid_tables = list(doc.find(_STREAM, with_fields=tuple(PID_FIELDS.values())))
    for prefix, table in zip(("furnace", "sample"), pid_tables[:2]):
        for name, field_id in PID_FIELDS.items():
            value = _numeric(table.value(field_id))
            if value is not None:
                metadata[f"{prefix}_{name}"] = value  # type: ignore[literal-required]


# -- MFC (mass flow controllers) ----------------------------------------------------


def extract_mfc(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Gas/range by ordinal pairing; flow setpoints by parameter name.

    Controller name tables (in MFC_FIELD_NAMES order) pair positionally with
    the first three range tables (field 0x1048, plausible 0.1..1000 ml/min)
    in stream order. Each range table takes its gas from the nearest
    preceding gas-context table (category high byte 0x1B carrying a known
    gas name); legacy quirk preserved: when no gas context precedes a range
    table, neither the gas nor the range key is emitted.
    """
    tables = doc.tables_of(_STREAM)
    # Every rule below is a substring search over table strings; decode each
    # table's string list exactly once.
    strings_of = [table.strings() for table in tables]

    def has_string_containing(index: int, needle: str) -> bool:
        return any(needle in text for text in strings_of[index])

    field_keys = [
        name.lower().replace(" ", "_")
        for name in MFC_FIELD_NAMES
        if any(has_string_containing(t.index, name) for t in tables)
    ]

    range_tables: list[tuple[Table, float]] = []
    for table in doc.find(_STREAM, with_fields=(MFC_RANGE_FIELD,)):
        entry = table.get(MFC_RANGE_FIELD)
        value = _numeric(entry.value) if entry is not None else None
        if entry is not None and entry.dtype == DType.F32 and value is not None:
            if 0.1 <= value <= 1000.0:
                range_tables.append((table, value))
            else:
                logger.debug(
                    f"MFC range value {value} outside plausible bounds; ignoring"
                )

    gas_by_index: dict[int, str] = {}
    for table in tables:
        if (table.category >> 8) == GAS_CONTEXT_HIGH_BYTE:
            for gas in GAS_TYPES:
                if has_string_containing(table.index, gas):
                    gas_by_index[table.index] = gas
                    break

    for key, (range_table, range_value) in zip(field_keys, range_tables[:3]):
        nearest_gas = next(
            (
                gas_by_index[i]
                for i in range(range_table.index - 1, -1, -1)
                if i in gas_by_index
            ),
            None,
        )
        if nearest_gas is not None:
            metadata[f"{key}_mfc_gas"] = nearest_gas  # type: ignore[literal-required]
            metadata[f"{key}_mfc_range"] = range_value  # type: ignore[literal-required]

    for meta_key, param_name in MFC_FLOW_PARAM_NAMES.items():
        for table in tables:
            if not has_string_containing(table.index, param_name):
                continue
            value = _numeric(table.value(MFC_FLOW_VALUE_FIELD))
            if value is not None and 0.0 <= value <= 1000.0:
                metadata[meta_key] = value  # type: ignore[literal-required]
            break  # one table per parameter name


# -- DSC sensitivity calibration constants --------------------------------------------


def extract_calibration_constants(doc: NGBDocument, metadata: FileMetadata) -> None:
    """p0-p5 from the first category-0x01F5 table that yields any of them."""
    for table in doc.by_category(_STREAM, CAL_CONSTANTS_CATEGORY):
        constants = {
            name: value
            for name, field_id in CAL_CONSTANTS.items()
            if (value := _numeric(table.value(field_id))) is not None
        }
        if constants:
            metadata["calibration_constants"] = constants  # type: ignore[typeddict-item]
            return


# -- Temperature calibration ------------------------------------------------------------


def _string_fields_ending_in(table: Table, suffix: str) -> Iterator[str]:
    for entry in table.fields.values():
        if (
            entry.dtype == DType.STRING
            and isinstance(entry.value, str)
            and entry.value.endswith(suffix)
        ):
            yield entry.value


def _find_record_table(doc: NGBDocument, suffix: str) -> tuple[Table, str] | None:
    """The calibration-source table whose record path ends in ``suffix``."""
    for table in doc.tables_of(_STREAM):
        path = next(_string_fields_ending_in(table, suffix), None)
        if path is not None:
            return table, path
    return None


def _extract_provenance(table: Table) -> dict[str, str | float]:
    provenance: dict[str, str | float] = {}
    for name, candidates in PROVENANCE_FIELDS.items():
        value = next(
            (v for fid in candidates if (v := table.value(fid)) is not None), None
        )
        if name == "date_measured":
            if isinstance(value, int) and value > 0:
                provenance[name] = datetime.fromtimestamp(
                    value, tz=timezone.utc
                ).isoformat()
        elif name == "heating_rate":
            if isinstance(value, (int, float)) and value > 0:
                provenance[name] = float(value)
        elif isinstance(value, str) and value.strip():
            provenance[name] = value.strip()
    return provenance


def extract_temperature_calibration(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Coefficients, fixpoints, and provenance (traceability/QA only).

    The sample_temperature channel is already corrected by Proteus; the
    coefficients are captured for provenance and never applied to the data.
    """
    cal: TemperatureCalibration = {}

    coeff_table = doc.first(
        _STREAM, category=TEMP_CAL_CATEGORY, with_fields=(TEMP_CAL_COEFF_FIELD,)
    )
    if coeff_table is not None:
        entry = coeff_table.get(TEMP_CAL_COEFF_FIELD)
        if entry is not None and entry.dtype == DType.U8 and entry.raw.nbytes:
            if entry.raw.nbytes % 4 == 0:
                cal["coefficients"] = [
                    float(x) for x in np.frombuffer(entry.raw, dtype="<f4")
                ]
            else:
                logger.debug(f"Invalid coefficient array length: {entry.raw.nbytes}")

    actual_id = FIXPOINT_FIELDS["actual_c"]
    corrected_id = FIXPOINT_FIELDS["corrected_c"]
    fixpoints: list[TemperatureFixpoint] = []
    for category in FIXPOINT_CATEGORIES:
        table = doc.first(
            _STREAM, category=category, with_fields=(actual_id, corrected_id)
        )
        if table is None:
            continue
        row: TemperatureFixpoint = {}
        for name, field_id in FIXPOINT_FIELDS.items():
            value = table.value(field_id)
            if name == "name":
                if isinstance(value, str) and value.strip():
                    row["name"] = value.strip()
            elif (numeric := _numeric(value)) is not None:
                row[name] = numeric  # type: ignore[literal-required]
        if row:
            fixpoints.append(row)
    if fixpoints:
        cal["fixpoints"] = fixpoints

    ts3 = _find_record_table(doc, TEMP_CAL_SUFFIX)
    if ts3 is not None:
        table, path = ts3
        cal["record_path"] = path
        cal.update(_extract_provenance(table))  # type: ignore[typeddict-item]
    if cal:
        metadata["temperature_calibration"] = cal

    sensitivity: SensitivityCalibration = {}
    es3 = _find_record_table(doc, SENSITIVITY_SUFFIX)
    if es3 is not None:
        table, path = es3
        sensitivity["record_path"] = path
        sensitivity.update(_extract_provenance(table))  # type: ignore[typeddict-item]
    if sensitivity:
        metadata["sensitivity_calibration"] = sensitivity


# -- Run environment -----------------------------------------------------------------------


def extract_run_environment(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Timezone snapshot and the linked correction file."""
    tz_table = doc.first(_STREAM, category=TIMEZONE_CATEGORY)
    if tz_table is not None:
        name = tz_table.value(TIMEZONE_FIELDS["name"])
        if isinstance(name, str) and name.strip():
            metadata["timezone"] = name.strip()
        bias = tz_table.value(TIMEZONE_FIELDS["bias"])
        if isinstance(bias, int):
            # Windows convention: UTC = local + bias, so the offset is -bias;
            # when daylight time is active the DST bias applies on top.
            offset = -bias
            dst_bias = tz_table.value(TIMEZONE_FIELDS["dst_bias"])
            if tz_table.value(TIMEZONE_FIELDS["state"]) == 2 and isinstance(
                dst_bias, int
            ):
                offset -= dst_bias
            metadata["utc_offset_minutes"] = offset

    link_table = doc.first(
        _STREAM, category=CORRECTION_LINK_CATEGORY, with_fields=(CORRECTION_LINK_FIELD,)
    )
    if link_table is not None:
        value = link_table.value(CORRECTION_LINK_FIELD)
        if isinstance(value, str) and value.strip():
            metadata["correction_file_path"] = value.strip()


# -- Application / license ---------------------------------------------------------------------


def extract_app_license(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Version banner and license block from the 0x0300 table's strings."""
    strings = [
        text
        for table in doc.by_category(_STREAM, APP_LICENSE_CATEGORY)
        for text in table.strings()
    ]
    if not strings:
        # Fallback: the category assumption failed; apply the same selection
        # rules over every stream-1 string field.
        strings = [text for table in doc.tables_of(_STREAM) for text in table.strings()]
    if not strings:
        return

    app = next((s for s in strings if _VERSION_RE.match(s)), None)
    if app and "application_version" not in metadata:
        metadata["application_version"] = app

    license_candidates = [
        s for s in strings if "\n" in s and not s.lstrip().startswith("Version")
    ]
    if license_candidates and "licensed_to" not in metadata:
        metadata["licensed_to"] = max(license_candidates, key=len)


# -- Entry point ---------------------------------------------------------------------------------

_EXTRACTORS: tuple[Callable[[NGBDocument, FileMetadata], None], ...] = (
    extract_masses,
    extract_temperature_program,
    extract_pid,
    extract_mfc,
    extract_calibration_constants,
    extract_temperature_calibration,
    extract_run_environment,
    extract_app_license,
)


def build_metadata(doc: NGBDocument) -> FileMetadata:
    """Extract all file metadata from a parsed document (stream 1 only).

    Never raises for missing or malformed metadata: every FileMetadata field
    is optional by contract, so each extraction rule that fails logs a
    warning and the rest proceed. (``file_hash`` is not set here — the API
    loaders add it, since it hashes the file, not the document.)
    """
    metadata: FileMetadata = {}
    _apply_field_map(doc, metadata)
    for extractor in _EXTRACTORS:
        try:
            extractor(doc, metadata)
        except Exception as e:
            logger.warning(f"{extractor.__name__} failed: {e}")
    return metadata
