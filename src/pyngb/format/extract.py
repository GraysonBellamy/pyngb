"""Metadata extraction: FileMetadata as queries over the document model.

``build_metadata`` applies the declarative :data:`~pyngb.format.maps.FIELD_MAP`
first, then eight plain extraction functions. Each function is one metadata
concern expressed against tables and fields; adding a Phase-2 field means
adding a function to :data:`_EXTRACTORS` (or an entry to FIELD_MAP), nothing
else. Every function is wrapped in a warn-and-continue net — all FileMetadata
fields are optional by contract, so a single misbehaving rule never sinks the
rest of the extraction.

Most rules preserve the legacy extractor semantics (the parity goldens pin
them): first-match-wins in stream order, occurrence-order classification for
PID and crucible masses. Two deliberately do not: the temperature program is
keyed by the category-encoded stage ordinal (stream order mislabels edited
programs), and the MFC keys come from the self-describing device tree with
flows read from the per-stage device states (the legacy string/ordinal
heuristics reported stale ``LastUsedFlow`` config as the run's flow).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from itertools import islice

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
    DEVICE_DEF_TYPE,
    DEVICE_ID_FIELD,
    DEVICE_KIND_FIELD,
    DEVICE_STATE_TYPE,
    FIELD_MAP,
    FIXPOINT_CATEGORIES,
    FIXPOINT_FIELDS,
    GAS_FORMULA_FIELD,
    GAS_GUID_FIELD,
    GAS_NAME_FIELD,
    GAS_RECORD_GUID_FIELD,
    GAS_RECORD_TYPE,
    KNOWN_FIELD_IDS,  # noqa: F401  (re-exported for census tooling)
    MFC_DEVICE_KIND,
    MFC_RANGE_FIELD,
    MFC_RANGE_TYPE,
    MFC_ROLES,
    PID_FIELDS,
    PROVENANCE_FIELDS,
    REF_NEIGHBOR_FIELD,
    SAMPLE_NEIGHBOR_FIELD,
    SENSITIVITY_SUFFIX,
    STAGE_CATEGORY_BASE,
    STAGE_FIELDS,
    STAGE_FLOW_FIELD,
    STAGE_TABLE_TYPE,
    STAGE_TYPE_BODY,
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


def _stage_ordinal(table: Table) -> int | None:
    """The program ordinal of a stage table; None for non-stage tables.

    A stage table is identified by its type ref AND the full five-field
    signature (field 0x083F doubles as the device id elsewhere); its
    category encodes the ordinal (STAGE_CATEGORY_BASE + N). Every stage
    consumer resolves tables through this one predicate so the program
    keys and the per-stage flow attribution can never disagree.
    """
    if table.type_ref != STAGE_TABLE_TYPE:
        return None
    if not table.has_fields(*STAGE_FIELDS.values()):
        return None
    ordinal = table.category - STAGE_CATEGORY_BASE
    if ordinal < 0:
        logger.debug(
            f"stage-shaped table with non-stage category 0x{table.category:04X}"
        )
        return None
    return ordinal


def _stage_tables(doc: NGBDocument) -> list[tuple[int, Table]]:
    """(ordinal, table) per stage, in program order; first wins a duplicate.

    Stream order is edit order, not program order — two fixtures store
    0, 2, 3, 4, 1.
    """
    stages: dict[int, Table] = {}
    for table in doc.tables_of(_STREAM):
        ordinal = _stage_ordinal(table)
        if ordinal is None:
            continue
        if ordinal in stages:
            logger.warning(f"duplicate stage ordinal {ordinal}; keeping the first")
            continue
        stages[ordinal] = table
    return sorted(stages.items())


def _mfc_role(table: Table) -> str | None:
    """The metadata key prefix for a device table's id, or None if unmapped."""
    device_id = table.value(DEVICE_ID_FIELD)
    return MFC_ROLES.get(device_id) if isinstance(device_id, int) else None


def _stage_mfc_flows(doc: NGBDocument) -> dict[int, dict[str, float]]:
    """Per-stage MFC flow setpoints: {stage ordinal: {role: ml/min}}.

    After each stage table the file snapshots every device as a
    type-0x2B11 state table whose following range table (type 0x2B0A)
    carries the stage's flow setpoint in field 0x1047.
    """
    tables = doc.tables_of(_STREAM)
    flows: dict[int, dict[str, float]] = {}
    current: int | None = None
    for table in tables:
        ordinal = _stage_ordinal(table)
        if ordinal is not None:
            current = ordinal
            continue
        if current is None or table.type_ref != DEVICE_STATE_TYPE:
            continue
        role = _mfc_role(table)
        if role is None:
            continue  # non-MFC device, or unmapped id (extract_mfc warns once)
        follower = tables[table.index + 1] if table.index + 1 < len(tables) else None
        if follower is None or follower.type_ref != MFC_RANGE_TYPE:
            continue
        entry = follower.get(STAGE_FLOW_FIELD)
        if entry is None or entry.dtype != DType.F32:
            continue
        value = _numeric(entry.value)
        if value is not None:
            flows.setdefault(current, {}).setdefault(role, value)
    return flows


def extract_temperature_program(doc: NGBDocument, metadata: FileMetadata) -> None:
    """Stage N = the stage table with category STAGE_CATEGORY_BASE + N.

    Each stage carries the four f32 program fields (times x60: stored in
    minutes, exposed in seconds), the i32 stage_type (0 = initial, 1 =
    ramp/isothermal, 2 = final/emergency-reset entry), and the stage's MFC
    flow setpoints from the device-state snapshots that follow it.
    """
    stages: dict[int, dict[str, float | int]] = {}
    for ordinal, table in _stage_tables(doc):
        stage: dict[str, float | int] = {}
        for name, field_id in STAGE_FIELDS.items():
            entry = table.get(field_id)
            if entry is None:
                continue
            if name == "stage_type":
                if entry.dtype == DType.I32 and isinstance(entry.value, int):
                    stage[name] = entry.value
                continue
            if entry.dtype != DType.F32:
                continue
            value = _numeric(entry.value)
            if value is None:
                continue
            stage[name] = value * 60.0 if name == "time" else value
        if stage:
            stages[ordinal] = stage
    if not stages:
        return
    for ordinal, stage_flows in _stage_mfc_flows(doc).items():
        target = stages.get(ordinal)
        if target is None:
            continue
        for role, flow in stage_flows.items():
            target[f"{role}_mfc_flow"] = flow
    metadata["temperature_program"] = {  # type: ignore[typeddict-item]
        f"stage_{ordinal}": stage for ordinal, stage in sorted(stages.items())
    }


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
    """Gas identity and range from the device tree; flow from the stage states.

    Every file carries one self-describing device block: a type-0x2B07
    definition table per device, MFCs identified by kind code 2 and a fixed
    device-id -> role map. The definition's own gas name plus the range
    table and gas record that follow it (before the next definition) give
    the identity keys directly — no string matching, no ordinal pairing.
    The gas record must GUID-match the definition (gas records of the same
    shape occur inside calibration-context blocks elsewhere in the stream).

    ``*_mfc_flow`` is the setpoint the run actually used. It is derived
    from the per-stage flows already merged into ``temperature_program``
    (extract_temperature_program runs first in _EXTRACTORS), so the scalar
    can never contradict the per-stage values it summarizes: the key is
    emitted only when every body stage (stage_type 1 — the initial stage
    may hold gas off, and the final type-2 stage is the never-executed
    emergency-reset entry) carries the same flow for that MFC. A program
    that varies a flow per stage, or with body stages missing their state
    snapshots, gets no scalar key. The ``*_LastUsedFlow`` device parameters
    are deliberately not read: they are persisted config, stale for MFCs
    the run did not use, and Proteus 8.0.3 writes them even for hardware
    that does not exist.
    """
    tables = doc.tables_of(_STREAM)
    for table in tables:
        if table.type_ref != DEVICE_DEF_TYPE:
            continue
        if table.value(DEVICE_KIND_FIELD) != MFC_DEVICE_KIND:
            continue
        role = _mfc_role(table)
        if role is None:
            logger.warning(
                f"MFC definition with unmapped device id "
                f"{table.value(DEVICE_ID_FIELD)!r}; skipped "
                "(an unrecognized fourth controller?)"
            )
            continue

        # The definition's own block ends at the next definition or state
        # table; within it, the range table and gas record are identified
        # by their type refs, and the gas record must GUID-match the
        # definition (records of the same shape occur in calibration
        # contexts; a definition without a GUID takes no formula).
        guid = table.value(GAS_GUID_FIELD)
        range_value: float | None = None
        formula: str | None = None
        for follower in islice(tables, table.index + 1, None):
            if follower.type_ref in (DEVICE_DEF_TYPE, DEVICE_STATE_TYPE):
                break
            if range_value is None and follower.type_ref == MFC_RANGE_TYPE:
                entry = follower.get(MFC_RANGE_FIELD)
                if entry is not None and entry.dtype == DType.F32:
                    range_value = _numeric(entry.value)
            if formula is None and follower.type_ref == GAS_RECORD_TYPE:
                record_guid = follower.value(GAS_RECORD_GUID_FIELD)
                if isinstance(guid, str) and guid and record_guid == guid:
                    value = follower.value(GAS_FORMULA_FIELD)
                    if isinstance(value, str) and value.strip():
                        formula = value.strip()
                else:
                    logger.debug(
                        f"{role}: ignoring gas record with GUID {record_guid!r} "
                        f"(definition GUID {guid!r})"
                    )
            if range_value is not None and formula is not None:
                break

        gas = table.value(GAS_NAME_FIELD)
        if isinstance(gas, str) and gas.strip() and f"{role}_mfc_gas" not in metadata:
            metadata[f"{role}_mfc_gas"] = gas.strip()  # type: ignore[literal-required]
        if range_value is not None and f"{role}_mfc_range" not in metadata:
            metadata[f"{role}_mfc_range"] = range_value  # type: ignore[literal-required]
        if formula is not None and f"{role}_mfc_gas_formula" not in metadata:
            metadata[f"{role}_mfc_gas_formula"] = formula  # type: ignore[literal-required]

    # Run-level flow setpoints, summarized from the per-stage values in
    # temperature_program (single source of truth).
    program = metadata.get("temperature_program") or {}
    body = [
        stage
        for stage in program.values()
        if stage.get("stage_type") == STAGE_TYPE_BODY
    ]
    if not body:  # no typed body stages: fall back to every snapshotted stage
        body = [
            stage
            for stage in program.values()
            if any(f"{role}_mfc_flow" in stage for role in MFC_ROLES.values())
        ]
    for role in MFC_ROLES.values():
        key = f"{role}_mfc_flow"
        values = {stage[key] for stage in body if key in stage}
        if body and len(values) == 1 and all(key in stage for stage in body):
            metadata[key] = values.pop()  # type: ignore[literal-required]


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
