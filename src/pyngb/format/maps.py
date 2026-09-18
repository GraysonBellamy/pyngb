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
    "CAL_RECORD_PATH_FIELD",
    "CHANNEL_CONFIG_TYPE",
    "CHANNEL_DEF_NAME_FIELD",
    "CHANNEL_HEADER_TYPE",
    "CHANNEL_MAP",
    "CHANNEL_RANGE_FIELD",
    "CHANNEL_RANGE_KEYS",
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
    "EXPANSION_CURVE_FIELD",
    "EXPANSION_CURVE_TYPE",
    "EXPANSION_STANDARD_FIELDS",
    "EXPANSION_STANDARD_TYPE",
    "FIELD_MAP",
    "FIXPOINT_CATEGORIES",
    "FIXPOINT_FIELDS",
    "FORCE_DEVICE_ID",
    "GAS_DENSITY_FIELD",
    "GAS_FORMULA_FIELD",
    "GAS_GUID_FIELD",
    "GAS_NAME_FIELD",
    "GAS_RECORD_GUID_FIELD",
    "GAS_RECORD_TYPE",
    "INSTRUMENT_CODE_FIELD",
    "INSTRUMENT_MODEL_FIELD",
    "INSTRUMENT_TABLE_TYPE",
    "KNOWN_FIELD_IDS",
    "MEASUREMENT_TYPES",
    "MEASUREMENT_TYPE_FIELD",
    "MFC_DEVICE_KIND",
    "MFC_RANGE_FACTOR_FIELD",
    "MFC_RANGE_FIELD",
    "MFC_RANGE_MAX_FIELD",
    "MFC_RANGE_TYPE",
    "MFC_ROLES",
    "PID_FIELDS",
    "PROVENANCE_FIELDS",
    "REF_NEIGHBOR_FIELD",
    "SAMPLE_GEOMETRY_CATEGORY",
    "SAMPLE_GEOMETRY_FIELDS",
    "SAMPLE_GEOMETRY_TYPE",
    "SAMPLE_LENGTH_FIELD",
    "SAMPLE_NEIGHBOR_FIELD",
    "SAMPLE_TABLE_TYPE",
    "SEGMENT_VALUES_TYPE",
    "SENS_CAL_RECORD_TYPE",
    "SENS_FIXPOINT_EXCLUDES",
    "SENS_FIXPOINT_FIELDS",
    "SENS_FIXPOINT_REQUIRES",
    "STAGE_CATEGORY_BASE",
    "STAGE_FIELDS",
    "STAGE_FLOW_FIELD",
    "STAGE_FORCE_FIELD",
    "STAGE_TABLE_TYPE",
    "STAGE_TYPE_BODY",
    "STAGE_TYPE_FINAL",
    "STAGE_TYPE_INITIAL",
    "STREAM_3_CHANNEL_DEF_TYPE",
    "STREAM_3_CHANNEL_LABELS",
    "TEMP_CAL_CATEGORY",
    "TEMP_CAL_COEFF_FIELD",
    "TEMP_CAL_RECORD_TYPE",
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
#: Header categories are ``<ch> 17`` (STA channels) or ``<ch> 18`` (the
#: dilatometer's dL and force channels) in stream 2 and ``<ch> 75`` in
#: stream 3; only the low byte identifies the channel. 0x87 is a data-less
#: trailing header and intentionally unmapped, as is the dilatometer's
#: all-zero second displacement channel 0x82 (meaning unknown); unmapped ids
#: pass through as two-digit hex column names via :func:`channel_name`.
CHANNEL_MAP: Final[dict[int, str]] = {
    # stream_2 channels: STA
    0x8C: "time",
    0x8D: "sample_temperature",
    0x8E: "dsc_signal",
    0x9C: "purge_flow_1",
    0x9D: "purge_flow_2",
    0x9E: "protective_flow",
    0x90: "mass",
    # stream_2 channels: DIL (push-rod dilatometer; verified against Proteus
    # CSV exports — dL in µm as f64, force in N as f32, the setpoint constant)
    0x8F: "length_change",
    0x4E: "force",
    0x4F: "force_setpoint",
    # stream_3 channels (named in the file itself: STREAM_3_CHANNEL_LABELS)
    0x30: "furnace_temperature",
    0x31: "cooling_power",
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


#: Stream-3 channels are self-describing: stream 1 carries one definition
#: table per channel (type ref STREAM_3_CHANNEL_DEF_TYPE, category 0x7530 +
#: channel ordinal, i.e. the same category as the channel's stream-3
#: header) with Proteus's display name in CHANNEL_DEF_NAME_FIELD and two
#: class codes (0x10C4, 0x10C9) that group channels by kind: the "Cooling"
#: channel shares the furnace-power class (1, 6), the two "Furnace"
#: entries are the furnace temperature (2, 1) and the furnace power (1, 6).
#: The names are NOT unique ("Furnace" twice) and are display strings, so
#: CHANNEL_MAP stays the naming authority; the labels below are the
#: observed names, pinned by a test over every fixture as a drift tripwire.
STREAM_3_CHANNEL_DEF_TYPE: Final = 0x2BA9
CHANNEL_DEF_NAME_FIELD: Final = 0x10C3
STREAM_3_CHANNEL_LABELS: Final[dict[int, str]] = {
    0x30: "Furnace",
    0x31: "Cooling",
    0x32: "Furnace",
    0x33: "HFoilTemp",
    0x34: "uC Module",
    0x35: "Air Pressure",
    0x36: "Acc. X",
    0x37: "Acc. Y",
    0x38: "Acc. Z",
}

#: Channel configuration tables (type CHANNEL_CONFIG_TYPE), one per stream-2
#: channel, categorised by the channel's header category. CHANNEL_RANGE_FIELD
#: (f32) is the channel's measuring range in the column's units: 20000 µm
#: for the dilatometer's dL (the "M.RANGE" of Proteus exports), 35000 mg
#: for the STA mass channel, 5000 µV for DSC; extracted as the metadata keys
#: in CHANNEL_RANGE_KEYS (column name -> key). Also carried, not extracted:
#: 0x083F (the instrument-internal channel id), 0x0BB9 (1 on every channel
#: stored as f64, 0 on f32 channels).
CHANNEL_CONFIG_TYPE: Final = 0x2B06
CHANNEL_RANGE_FIELD: Final = 0x0BBC
CHANNEL_RANGE_KEYS: Final[dict[str, str]] = {
    "length_change": "length_change_range",
    "mass": "mass_range",
    "dsc_signal": "dsc_range",
}


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


#: Initial sample length L0 (mm) of a dilatometer sample descriptor table.
SAMPLE_LENGTH_FIELD: Final = 0x0C9F

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
    # Dilatometer: the initial sample length L0 (mm) sits in the id next to
    # the STA sample mass. Blank correction runs store 0.0, rejected like a
    # zero mass.
    MetaField("sample_length", 0x7530, SAMPLE_LENGTH_FIELD, _positive_float),
)

#: The sample descriptor table (type ref), for the embedded correction's
#: descriptor (see extract.embedded_correction_sample_length).
SAMPLE_TABLE_TYPE: Final = 0x2AFB

#: Dilatometer sample geometry table: diameter (mm) and cross-section
#: (mm², exactly pi*d²/4), both f64. Also carried, not extracted: 0x111E =
#: cross_section / (1000 * length) (derived) and 0x1120, a shape code (3 is
#: the only observed value; presumably cylinder). Extracted only alongside
#: a positive sample_length: blank correction runs can carry stale form
#: values (the three RO_Para corrections store d = 7.98 mm with L0 = 0).
SAMPLE_GEOMETRY_CATEGORY: Final = 0x1857
SAMPLE_GEOMETRY_TYPE: Final = 0x2BCF
SAMPLE_GEOMETRY_FIELDS: Final[dict[str, int]] = {
    "sample_diameter": 0x111B,
    "sample_cross_section": 0x111D,
}

#: Instrument identity table: the model string ("NETZSCH DIL 402 Expedis
#: Select", "NETZSCH STA 449F3"; absent from the STA-449F3A .ngb-ds3
#: fixtures) and an instrument type code (44 = STA 449, 69 = DIL 402 — the
#: same number Proteus embeds in its calibration record file names,
#: "K_44_STA449F3A-…"). The code is documented, not extracted.
INSTRUMENT_TABLE_TYPE: Final = 0x2B17
INSTRUMENT_MODEL_FIELD: Final = 0x0432
INSTRUMENT_CODE_FIELD: Final = 0x083F

#: Measurement kind, from the measurement-definition table (category
#: CORRECTION_LINK_CATEGORY): 1 = a correction run (.ngb-bs3/.ngb-cla),
#: 2 = a sample run (.ngb-ss3), 3 = "Sample + Correction" (.ngb-ds3/
#: .ngb-dla). The same table's 0x083F holds 2 for sample kinds and 4 for
#: corrections.
MEASUREMENT_TYPE_FIELD: Final = 0x103A
MEASUREMENT_TYPES: Final[dict[int, str]] = {
    1: "correction",
    2: "sample",
    3: "sample_correction",
}

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

#: External calibration records are category-0x01F5 source tables told
#: apart by TYPE ref, with the record path in CAL_RECORD_PATH_FIELD: the
#: temperature calibration (paths end in .ngb-ts3, or the identity records
#: TCALZERO.TMX / TCALZERO.TCX on dilatometer and STA-449F3A files) and the
#: DSC sensitivity calibration (.ngb-es3, or SENSZERO.EXX). Matching on the
#: path suffix — the pre-0.6 rule — missed every zero record.
TEMP_CAL_RECORD_TYPE: Final = 0x2422
SENS_CAL_RECORD_TYPE: Final = 0x23F0
CAL_RECORD_PATH_FIELD: Final = 0x07D4

#: Calibration provenance scalar field ids within the calibration-source
#: table. Candidate ids are tried in order: the ts3 table stores the
#: crucible in 0x0433, es3 in 0x044C.
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

# -- Dilatometer expansion standard ----------------------------------------------

#: The reference-material expansion record (category 0x01F5, like the other
#: calibration sources) names the standard the push-rod system is corrected
#: against and its validity range; the literature expansion curve itself is
#: the EXPANSION_CURVE_FIELD byte array of the adjacent category-0x01F7
#: table: ``u16 byte_length, u16 n, then n x (f32 T [°C], f32 dL/L0)``,
#: relative to 20 °C. The record appears in nested copies (the original
#: NETZSCH record and the instrument PC's copy, differing only in path and
#: date); every copy of the curve is identical. First match wins.
EXPANSION_STANDARD_TYPE: Final = 0x2454
EXPANSION_STANDARD_FIELDS: Final[dict[str, int]] = {
    "name": 0x0462,  # string, e.g. "FUSED SILICA"
    "source": 0x0463,  # string, e.g. "NBS 739/1971"
    "comment": 0x083D,  # string
    "temperature_min": 0x0464,  # f32, °C
    "temperature_max": 0x0465,  # f32, °C
    "record_path": 0x07D4,  # string, the .scl record
    "date": 0x083E,  # i32 Unix timestamp of the record
}
EXPANSION_CURVE_TYPE: Final = 0x2459
EXPANSION_CURVE_FIELD: Final = 0x04C0

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

#: The dilatometer's push-rod force controller is device id 58 (definition
#: category 0x1C00, kind 10, no gas or range). Its per-stage state table is
#: followed by a type-MFC_RANGE_TYPE table whose STAGE_FORCE_FIELD (f32, N)
#: is that stage's force setpoint — the same pattern as the MFC flows.
FORCE_DEVICE_ID: Final = 58
STAGE_FORCE_FIELD: Final = 0x10FA

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
    | {STAGE_FLOW_FIELD, STAGE_FORCE_FIELD}
    | {DEVICE_PARAM_NAME_FIELD, DEVICE_PARAM_VALUE_FIELD}
    | {SAMPLE_NEIGHBOR_FIELD, REF_NEIGHBOR_FIELD}
    | {field_id for field_id, _ in DATA_FIELDS}
    | {CAL_RECORD_PATH_FIELD, MEASUREMENT_TYPE_FIELD, INSTRUMENT_MODEL_FIELD}
    | {CHANNEL_RANGE_FIELD, CHANNEL_DEF_NAME_FIELD}
    | set(SAMPLE_GEOMETRY_FIELDS.values())
    | set(EXPANSION_STANDARD_FIELDS.values())
    | {EXPANSION_CURVE_FIELD}
)
