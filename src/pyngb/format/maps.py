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
    "FIELD_MAP",
    "FIXPOINT_CATEGORIES",
    "FIXPOINT_FIELDS",
    "GAS_CONTEXT_HIGH_BYTE",
    "GAS_TYPES",
    "KNOWN_FIELD_IDS",
    "MFC_FIELD_NAMES",
    "MFC_FLOW_PARAM_NAMES",
    "MFC_FLOW_VALUE_FIELD",
    "MFC_RANGE_FIELD",
    "PID_FIELDS",
    "PROVENANCE_FIELDS",
    "REF_NEIGHBOR_FIELD",
    "SAMPLE_NEIGHBOR_FIELD",
    "SEGMENT_VALUES_TYPE",
    "SENSITIVITY_SUFFIX",
    "STAGE_FIELDS",
    "TEMP_CAL_CATEGORY",
    "TEMP_CAL_COEFF_FIELD",
    "TEMP_CAL_SUFFIX",
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

#: Stage scalar field ids (all f32). A stage table carries all five; stage N
#: is the Nth such table in stream order. Durations are stored in minutes and
#: exposed in seconds (x60), consistent with the public time column.
STAGE_FIELDS: Final[dict[str, int]] = {
    "stage_type": 0x083F,
    "temperature": 0x0E17,
    "heating_rate": 0x0E13,
    "acquisition_rate": 0x0E14,
    "time": 0x0E15,
}

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

#: Fixpoint tables are categorised 0x7530..0x753F (one per standard, ascending
#: temperature). The 0x7530 category is shared with the sample table, so a
#: fixpoint table is confirmed by carrying the actual- and corrected-
#: temperature fields.
FIXPOINT_CATEGORIES: Final = tuple(range(0x7530, 0x7540))
FIXPOINT_FIELDS: Final[dict[str, int]] = {
    "name": 0x0443,
    "actual_c": 0x0444,
    "measured_c": 0x0445,
    "weight": 0x0446,
    "corrected_c": 0x0447,
}

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

# -- MFC (mass flow controllers) ---------------------------------------------------

#: The f32 range record identifying an MFC range table (plausible ml/min
#: full-scale values are 0.1..1000).
MFC_RANGE_FIELD: Final = 0x1048
#: Device-parameter tables: UTF-16LE parameter name / f32 value.
MFC_FLOW_NAME_FIELD: Final = 0x1062
MFC_FLOW_VALUE_FIELD: Final = 0x1061
#: Gas-context tables have category high byte 0x1B and carry the gas name.
GAS_CONTEXT_HIGH_BYTE: Final = 0x1B

GAS_TYPES: Final = ("NITROGEN", "OXYGEN", "ARGON", "HELIUM", "CARBON_DIOXIDE")
#: Controller display names, in the ordinal order paired with range tables.
MFC_FIELD_NAMES: Final = ("Purge 1", "Purge 2", "Protective")
#: Metadata key -> the *_LastUsedFlow parameter name holding its setpoint.
MFC_FLOW_PARAM_NAMES: Final[dict[str, str]] = {
    "purge_1_mfc_flow": "Purge 1 MFC_MFC400_LastUsedFlow",
    "purge_2_mfc_flow": "Purge 2 MFC_MFC400_LastUsedFlow",
    "protective_mfc_flow": "Protective MFC_MFC400_LastUsedFlow",
}

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
    | {fid for candidates in PROVENANCE_FIELDS.values() for fid in candidates}
    | set(TIMEZONE_FIELDS.values())
    | {CORRECTION_LINK_FIELD}
    | {MFC_RANGE_FIELD, MFC_FLOW_NAME_FIELD, MFC_FLOW_VALUE_FIELD}
    | {SAMPLE_NEIGHBOR_FIELD, REF_NEIGHBOR_FIELD}
    | {field_id for field_id, _ in DATA_FIELDS}
)
