"""Declarative format knowledge: every known field id, category, and type ref.

This module is the single place where NGB semantics (which category/field
holds which metadata key, which type refs mark channel tables, which column
name a channel id maps to) are written down. Everything here is a frozen
module-level constant; extending pyngb's format coverage means editing these
tables, never writing a new scanner.

All ids are plain ints (the on-disk encoding is little-endian u16; the
tokenizer decodes them, so nothing here deals in bytes).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Final, NamedTuple

from .grammar import DType

__all__ = [
    "APP_LICENSE_CATEGORY",
    "CAL_CONSTANTS",
    "CAL_CONSTANTS_CATEGORY",
    "CHANNEL_HEADER_TYPE",
    "CHANNEL_MAP",
    "CORRECTION_LINK_CATEGORY",
    "CORRECTION_LINK_FIELD",
    "CRUCIBLE_CATEGORY",
    "CRUCIBLE_MASS_FIELD",
    "DATA_FIELDS",
    "DEVICE_DEF_TYPE",
    "DEVICE_HW_FIELD",
    "DEVICE_ID_FIELD",
    "DEVICE_KIND_FIELD",
    "DEVICE_PARAM_NAME_FIELD",
    "DEVICE_PARAM_VALUE_FIELD",
    "DEVICE_STATE_TYPE",
    "FIELD_MAP",
    "FIXPOINT_CATEGORIES",
    "FIXPOINT_FIELDS",
    "GAS_DENSITY_FIELD",
    "GAS_FORMULA_FIELD",
    "GAS_GUID_FIELD",
    "GAS_NAME_FIELD",
    "GAS_RECORD_GUID_FIELD",
    "GAS_RECORD_TYPE",
    "KNOWN_FIELD_IDS",
    "MFC_DEVICE_KIND",
    "MFC_RANGE_FACTOR_FIELD",
    "MFC_RANGE_FIELD",
    "MFC_RANGE_MAX_FIELD",
    "MFC_RANGE_TYPE",
    "MFC_ROLES",
    "PID_FIELDS",
    "PROVENANCE_FIELDS",
    "REF_NEIGHBOR_FIELD",
    "SAMPLE_NEIGHBOR_FIELD",
    "SEGMENT_VALUES_TYPE",
    "SENSITIVITY_SUFFIX",
    "SENS_FIXPOINT_EXCLUDES",
    "SENS_FIXPOINT_FIELDS",
    "SENS_FIXPOINT_REQUIRES",
    "STAGE_CATEGORY_BASE",
    "STAGE_FIELDS",
    "STAGE_FLOW_FIELD",
    "STAGE_TABLE_TYPE",
    "STAGE_TYPE_BODY",
    "STAGE_TYPE_FINAL",
    "STAGE_TYPE_INITIAL",
    "TEMP_CAL_CATEGORY",
    "TEMP_CAL_COEFF_FIELD",
    "TEMP_CAL_SUFFIX",
    "TEMP_FIXPOINT_EXCLUDES",
    "TEMP_FIXPOINT_REQUIRES",
    "TIMEZONE_CATEGORY",
    "TIMEZONE_FIELDS",
    "MetaField",
    "channel_name",
]

# -- Data streams (2 and 3) ---------------------------------------------------

#: Type ref of a channel-header table. The header's category low byte is the
#: channel id; its data follows in per-segment value tables.
CHANNEL_HEADER_TYPE: Final = 0x2B22
#: Type ref of a per-segment value table (categories are segment ordinals).
SEGMENT_VALUES_TYPE: Final = 0x2B23

#: The (field_id, dtype) pairs that carry channel data. Each segment-value
#: table holds exactly ONE of these: 0x0F40 (f64) for f64 channels
#: (time, mass), 0x0F3D (f32) for f32 channels (temperatures, dsc, flows).
DATA_FIELDS: Final[frozenset[tuple[int, DType]]] = frozenset(
    {(0x0F40, DType.F64), (0x0F3D, DType.F32)}
)

#: Channel id (low byte of the header table's category) -> column name.
#: 0x87 is a data-less trailing header and intentionally unmapped; unmapped
#: ids pass through as two-digit hex column names (e.g. the all-zero
#: stream_3 channel "31") via :func:`channel_name`.
CHANNEL_MAP: Final[dict[int, str]] = {
    # stream_2 channels
    0x8C: "time",
    0x8D: "sample_temperature",
    0x8E: "dsc_signal",
    0x9C: "purge_flow_1",
    0x9D: "purge_flow_2",
    0x9E: "protective_flow",
    0x90: "mass",
    # stream_3 channels
    0x30: "furnace_temperature",
    0x32: "furnace_power",
    0x33: "h_foil_temperature",
    0x34: "uc_module",
    0x35: "environmental_pressure",
    0x36: "environmental_acceleration_x",
    0x37: "environmental_acceleration_y",
    0x38: "environmental_acceleration_z",
}


def channel_name(category: int) -> str:
    """Public column name for a channel-header table's category."""
    channel_id = category & 0xFF
    return CHANNEL_MAP.get(channel_id, f"{channel_id:02x}")


# -- Basic stream-1 metadata fields -------------------------------------------


def _iso_utc(value: object) -> str | None:
    """Unix timestamp -> ISO-8601 UTC string (None on anything implausible)."""
    if not isinstance(value, int):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def _clean_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _positive_float(value: object) -> float | None:
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    return None


class MetaField(NamedTuple):
    """One directly-mapped metadata field.

    Resolution rule (applied by extract.build_metadata): the first stream-1
    table, in stream order, whose category matches AND which carries the
    field id wins; ``convert`` post-processes the decoded value and may
    reject it by returning None.
    """

    key: str
    category: int
    field_id: int
    convert: Callable[[object], object | None]


FIELD_MAP: Final[tuple[MetaField, ...]] = (
    MetaField("instrument", 0x1775, 0x1059, _clean_str),
    MetaField("project", 0x1772, 0x083C, _clean_str),
    MetaField("date_performed", 0x1772, 0x083E, _iso_utc),
    MetaField("lab", 0x1772, 0x0834, _clean_str),
    MetaField("operator", 0x1772, 0x0835, _clean_str),
    MetaField("crucible_type", 0x177E, 0x0840, _clean_str),
    MetaField("comment", 0x1772, 0x083D, _clean_str),
    MetaField("furnace_type", 0x177A, 0x0840, _clean_str),
    MetaField("carrier_type", 0x1779, 0x0840, _clean_str),
    MetaField("sample_id", 0x7530, 0x0898, _clean_str),
    MetaField("sample_name", 0x7530, 0x0840, _clean_str),
    MetaField("material", 0x7530, 0x0962, _clean_str),
    MetaField("sample_mass", 0x7530, 0x0C9E, _positive_float),
)

# -- Control parameters (PID) --------------------------------------------------

#: PID scalar field ids (f32). Tables carrying all three appear twice in
#: stream 1: first occurrence = furnace controller, second = sample.
PID_FIELDS: Final[dict[str, int]] = {"xp": 0x0FE7, "tn": 0x0FE8, "tv": 0x0FE9}

# -- Temperature program -------------------------------------------------------

#: A stage table has type ref STAGE_TABLE_TYPE and carries all five stage
#: fields; both are required (0x083F doubles as the device id elsewhere).
#: Its category encodes the program ordinal: stage N has category
#: STAGE_CATEGORY_BASE + N. Stream order is NOT program order (edited
#: programs are serialized in edit order; two fixtures store 0, 2, 3, 4, 1
#: — verified against the recorded temperature data). Durations are stored
#: in minutes and exposed in seconds (x60), consistent with the public
#: time column.
STAGE_TABLE_TYPE: Final = 0x2B0C
STAGE_CATEGORY_BASE: Final = 0x7530
STAGE_FIELDS: Final[dict[str, int]] = {
    "stage_type": 0x083F,
    "temperature": 0x0E17,
    "heating_rate": 0x0E13,
    "acquisition_rate": 0x0E14,
    "time": 0x0E15,
}

#: stage_type (i32) codes. INITIAL is the zero-duration starting condition
#: (may hold gas off); BODY stages are the executed program (ramp when
#: heating_rate > 0, isothermal when 0); FINAL carries the emergency-reset
#: temperature — a limit, never executed as a stage.
STAGE_TYPE_INITIAL: Final = 0
STAGE_TYPE_BODY: Final = 1
STAGE_TYPE_FINAL: Final = 2

# -- DSC sensitivity calibration constants -------------------------------------

CAL_CONSTANTS_CATEGORY: Final = 0x01F5
CAL_CONSTANTS: Final[dict[str, int]] = {
    "p0": 0x044F,
    "p1": 0x0450,
    "p2": 0x0451,
    "p3": 0x0452,
    "p4": 0x0453,
    "p5": 0x04C3,
}

# -- Temperature calibration ----------------------------------------------------

#: The [B0, B1, B2] correction polynomial lives in an 0x01F7 table as a
#: dtype-0x10 byte array on field 0x04BE, reinterpreted as little-endian f32.
TEMP_CAL_CATEGORY: Final = 0x01F7
TEMP_CAL_COEFF_FIELD: Final = 0x04BE

#: Fixpoint tables are categorised 0x7530..0x753F (one standard per category,
#: ascending temperature), and each category can hold one table of EACH
#: fixpoint family: a temperature fixpoint and a DSC sensitivity fixpoint
#: (enthalpy standard; see :class:`pyngb.constants.SensitivityFixpoint` for
#: the column semantics and verified identities). The two families reuse
#: field ids 0x0443 and 0x0445-0x0447 with family-specific meanings, and the
#: categories are also shared with unrelated tables (sample, stage states),
#: so family membership is decided by the REQUIRES/EXCLUDES sets below: a
#: table belongs to a family when it carries all of the family's required
#: fields and not the other family's marker.
FIXPOINT_CATEGORIES: Final = tuple(range(0x7530, 0x7540))
FIXPOINT_FIELDS: Final[dict[str, int]] = {
    "name": 0x0443,
    "actual_c": 0x0444,
    "measured_c": 0x0445,
    "weight": 0x0446,
    "corrected_c": 0x0447,
}
SENS_FIXPOINT_FIELDS: Final[dict[str, int]] = {
    "name": 0x0443,
    "temperature_c": 0x0454,
    "enthalpy": 0x0455,
    "peak_area": 0x0456,
    "measured_sensitivity": 0x0445,
    "weight": 0x0446,
    "fitted_sensitivity": 0x0447,
}
TEMP_FIXPOINT_REQUIRES: Final = (
    FIXPOINT_FIELDS["actual_c"],
    FIXPOINT_FIELDS["corrected_c"],
)
TEMP_FIXPOINT_EXCLUDES: Final = SENS_FIXPOINT_FIELDS["temperature_c"]
SENS_FIXPOINT_REQUIRES: Final = (SENS_FIXPOINT_FIELDS["temperature_c"],)
SENS_FIXPOINT_EXCLUDES: Final = FIXPOINT_FIELDS["actual_c"]

#: External calibration record path suffixes (values of string fields in the
#: 0x01F5 calibration-source tables).
TEMP_CAL_SUFFIX: Final = ".ngb-ts3"
SENSITIVITY_SUFFIX: Final = ".ngb-es3"

#: Calibration provenance scalar field ids within the calibration-source
#: table (the table whose record path ends in the suffix). Candidate ids are
#: tried in order: the ts3 table stores the crucible in 0x0433, es3 in 0x044C.
PROVENANCE_FIELDS: Final[dict[str, tuple[int, ...]]] = {
    "date_measured": (0x083E,),  # i32 Unix timestamp
    "gas": (0x0431,),  # string
    "crucible_type": (0x044C, 0x0433),  # string
    "heating_rate": (0x0435,),  # f32, K/min
    "comment": (0x083D,),  # string
}

# -- Run environment -------------------------------------------------------------

#: Timezone snapshot table (Windows TIME_ZONE_INFORMATION-style fields).
#: utc_offset_minutes = -(bias + dst_bias if daylight active else bias).
TIMEZONE_CATEGORY: Final = 0x1859
TIMEZONE_FIELDS: Final[dict[str, int]] = {
    "name": 0x1135,  # string, e.g. "Eastern Daylight Time"
    "bias": 0x1134,  # i32 minutes (UTC = local + bias)
    "dst_bias": 0x1137,  # i32 minutes, additional bias when DST active
    "state": 0x1138,  # i32: 1 = standard time, 2 = daylight time
}

#: Linked correction/measurement file (measurement-definition table).
CORRECTION_LINK_CATEGORY: Final = 0x1770
CORRECTION_LINK_FIELD: Final = 0x0843

# -- MFC device tree ----------------------------------------------------------

#: The run's gas hardware is one self-describing block in stream 1: a
#: type-0x2B07 definition table per device (categories 0x1BAC+), each MFC
#: definition immediately followed by its range table (type 0x2B0A) and
#: its gas record (type 0x2B81, category 0x1BE4). After every
#: temperature-program stage table the same categories reappear as
#: type-0x2B11 state tables whose following range table carries that
#: stage's flow setpoint. Gas records of the same shape also occur inside
#: calibration-context blocks elsewhere in the stream; extraction anchors
#: on the definitions and never touches those.
DEVICE_DEF_TYPE: Final = 0x2B07
DEVICE_STATE_TYPE: Final = 0x2B11
MFC_RANGE_TYPE: Final = 0x2B0A
GAS_RECORD_TYPE: Final = 0x2B81

#: Definition/state fields. DEVICE_ID_FIELD shares its id with stage_type;
#: both are table-scoped. Device kinds observed: 2 = MFC (the only kind
#: with a gas and range); 10 and 8 are non-MFC devices (ids 37/49).
DEVICE_ID_FIELD: Final = 0x083F  # i32
DEVICE_KIND_FIELD: Final = 0x104B  # i32
DEVICE_HW_FIELD: Final = 0x1075  # i32 hardware type code (104 <-> MFC400)
MFC_DEVICE_KIND: Final = 2

#: Device id -> metadata key prefix. Fixed Proteus channel ids, confirmed
#: three independent ways on the 2022 fixture (device-parameter names,
#: recorded flow channels, category order). An MFC definition with an id
#: not listed here (a real fourth MFC) is logged, never silently dropped.
MFC_ROLES: Final[dict[int, str]] = {
    30: "purge_1",
    31: "purge_2",
    32: "protective",
}

#: Gas identity. The definition carries name + GUID; the gas record repeats
#: them (GUID in 0x17FC, which is also the generic GUID field of session
#: tables) and adds the short formula and density.
GAS_NAME_FIELD: Final = 0x0840  # string; shared id with sample_name
GAS_GUID_FIELD: Final = 0x0C8F
GAS_RECORD_GUID_FIELD: Final = 0x17FC
GAS_FORMULA_FIELD: Final = 0x0C88  # string, e.g. "N2"
GAS_DENSITY_FIELD: Final = 0x1040  # f32, g/l

#: Range-table fields (all f32, ml/min). STAGE_FLOW_FIELD appears only in
#: the range tables of per-stage state groups, never in the definitions.
MFC_RANGE_FIELD: Final = 0x1048  # full scale
MFC_RANGE_MAX_FIELD: Final = 0x104D  # overrange limit (full scale x 1.02)
MFC_RANGE_FACTOR_FIELD: Final = 0x104C  # gas correction factor
STAGE_FLOW_FIELD: Final = 0x1047

#: Device-parameter tables (type 0x2B65: UTF-16LE name in 0x1062, f32 value
#: in 0x1061). Documented but NOT extracted: their *_LastUsedFlow values
#: are persisted instrument config (the last flow ever used on a channel,
#: not this run's setpoint), and Proteus 8.0.3 writes parameter blocks for
#: hardware that does not exist ("Purge 3"). The device tree above is
#: authoritative.
DEVICE_PARAM_NAME_FIELD: Final = 0x1062
DEVICE_PARAM_VALUE_FIELD: Final = 0x1061

# -- Crucible masses -----------------------------------------------------------------

#: Both crucible-mass tables share category 0x177E and carry the f64 mass in
#: field 0x0C9E; they are told apart by the trailing field of the PRECEDING
#: table: 0x0C83 (f32) -> the sample crucible follows, 0x10C4 (u16) -> the
#: reference crucible follows.
CRUCIBLE_CATEGORY: Final = 0x177E
CRUCIBLE_MASS_FIELD: Final = 0x0C9E
SAMPLE_NEIGHBOR_FIELD: Final = 0x0C83
REF_NEIGHBOR_FIELD: Final = 0x10C4

# -- Application / license ------------------------------------------------------------

#: The stream-1 table whose string fields carry the Proteus version banner
#: and the license block.
APP_LICENSE_CATEGORY: Final = 0x0300

# -- Unknown-field enumeration ----------------------------------------------------------

#: Every field id this module knows about. NGBDocument.unknown_fields() and
#: the census goldens report fields NOT in this set - the systematic
#: enumeration of format knowledge still to be mapped (the Phase-2 backlog).
KNOWN_FIELD_IDS: Final[frozenset[int]] = frozenset(
    {meta.field_id for meta in FIELD_MAP}
    | set(PID_FIELDS.values())
    | set(STAGE_FIELDS.values())
    | set(CAL_CONSTANTS.values())
    | {TEMP_CAL_COEFF_FIELD}
    | set(FIXPOINT_FIELDS.values())
    | set(SENS_FIXPOINT_FIELDS.values())
    | {fid for candidates in PROVENANCE_FIELDS.values() for fid in candidates}
    | set(TIMEZONE_FIELDS.values())
    | {CORRECTION_LINK_FIELD}
    | {DEVICE_ID_FIELD, DEVICE_KIND_FIELD, DEVICE_HW_FIELD}
    | {GAS_NAME_FIELD, GAS_GUID_FIELD, GAS_RECORD_GUID_FIELD}
    | {GAS_FORMULA_FIELD, GAS_DENSITY_FIELD}
    | {MFC_RANGE_FIELD, MFC_RANGE_MAX_FIELD, MFC_RANGE_FACTOR_FIELD}
    | {STAGE_FLOW_FIELD}
    | {DEVICE_PARAM_NAME_FIELD, DEVICE_PARAM_VALUE_FIELD}
    | {SAMPLE_NEIGHBOR_FIELD, REF_NEIGHBOR_FIELD}
    | {field_id for field_id, _ in DATA_FIELDS}
)
