"""
Constants, enums, and configuration classes for NGB parsing.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypedDict

__all__ = [  # noqa: RUF022 - order chosen for logical grouping
    "BinaryMarkers",
    "BinaryProcessing",
    "BaseColumnMetadata",
    "BaselinableColumnMetadata",
    "DEFAULT_COLUMN_METADATA",
    "FIELD_APPLICABILITY",
    "DataType",
    "DataTypeSizes",
    "FileMetadata",
    "FileMetadataRequired",
    "PatternConfig",
    "PatternOffsets",
    "REF_CRUCIBLE_SIG_FRAGMENT",
    "SAMPLE_CRUCIBLE_SIG_FRAGMENT",
    "StreamMarkers",
    "TemperatureCalibration",
    "TemperatureFixpoint",
    "ValidationThresholds",
]


class FileMetadataRequired(TypedDict):
    """Required metadata fields always present after parsing.

    These fields are guaranteed to be populated by the parser
    and can be relied upon in downstream processing.
    """

    # No fields are strictly required - the NGB format allows for sparse metadata
    # All fields are technically optional based on file contents


class TemperatureFixpoint(TypedDict, total=False):
    """A single temperature-calibration fixpoint (phase-transition standard).

    Each fixpoint is one row of the Proteus temperature-calibration table:
    ``actual`` vs ``measured`` with a ``weight``, producing a ``corrected`` value.
    Standards vary per calibration (e.g. Biphenyl, Benzoeacid, KClO4, In, Sn) -
    the names and values are read from the file, never hard-coded.

    The relationship between the columns is exact and was verified against every
    available file (residuals ``< 1e-3``)::

        corrected_c = measured_c + correction(measured_c)
        correction(T) = 1e-3*B0 + 1e-5*B1*T + 1e-8*B2*T**2

    where ``[B0, B1, B2]`` are ``TemperatureCalibration.coefficients``. The
    remaining gap ``actual_c - corrected_c`` is the calibration residual.

    Fields:
        name: Standard name as recorded by Proteus.
        actual_c: Actual (literature) transition temperature in °C.
        measured_c: Raw measured transition temperature in °C (before correction).
        weight: Regression weight for this point (1.0 in all observed files).
        corrected_c: Measured value with the calibration polynomial applied (°C).
    """

    name: str
    actual_c: float
    measured_c: float
    weight: float
    corrected_c: float


class TemperatureCalibration(TypedDict, total=False):
    """Temperature-calibration block extracted for traceability/QA only.

    IMPORTANT: The ``sample_temperature`` channel stored in NGB files is already
    temperature-corrected by Proteus. These coefficients must NOT be applied to it
    (doing so would double-correct). They are captured for provenance only.

    The correction polynomial is the active one Proteus used to produce the
    ``corrected_c`` column of each fixpoint (verified by round-trip); it is simply
    not re-applied to the already-corrected sample channel.

    Fields:
        coefficients: Polynomial coefficients [B0, B1, B2]. The Proteus correction
            (NOT re-applied to the data by pyngb) is
            ``correction[°C] = 1e-3*B0 + 1e-5*B1*T_exp + 1e-8*B2*T_exp**2``.
        fixpoints: The phase-transition standards used for the calibration.
        record_path: Path to the external temperature-calibration record (.ngb-ts3).
    """

    coefficients: list[float]
    fixpoints: list[TemperatureFixpoint]
    record_path: str


class FileMetadata(TypedDict, total=False):
    """Type definition for file metadata dictionary.

    Mass-related fields grouped together after core identifying fields. Reference masses
    are structurally derived; crucible_mass pattern also matches reference_crucible_mass and
    is disambiguated using signature fragments (see SAMPLE_CRUCIBLE_SIG_FRAGMENT / REF_CRUCIBLE_SIG_FRAGMENT).

    Note:
        All fields are optional (total=False) as the NGB binary format does not
        guarantee the presence of any particular metadata field. Files from different
        instruments, software versions, or with different configurations may have
        different subsets of metadata available.
    """

    instrument: str
    project: str
    date_performed: str
    lab: str
    operator: str
    crucible_type: str
    comment: str
    furnace_type: str
    carrier_type: str
    sample_id: str
    sample_name: str
    # Mass group
    sample_mass: float
    crucible_mass: float
    reference_mass: float
    reference_crucible_mass: float
    # Other descriptors
    material: str
    application_version: str
    licensed_to: str
    temperature_program: dict[str, dict[str, Any]]
    calibration_constants: dict[str, float]
    # Temperature calibration (traceability/QA only - never applied to the data)
    temperature_calibration: TemperatureCalibration
    sensitivity_record_path: str
    file_hash: dict[str, str]
    # MFC (Mass Flow Controller) metadata
    purge_1_mfc_gas: str
    purge_2_mfc_gas: str
    protective_mfc_gas: str
    purge_1_mfc_range: float
    purge_2_mfc_range: float
    protective_mfc_range: float
    # Control parameters (PID settings)
    furnace_xp: float
    furnace_tn: float
    furnace_tv: float
    sample_xp: float
    sample_tn: float
    sample_tv: float


class BaseColumnMetadata(TypedDict, total=False):
    """Base column metadata structure for all thermal analysis data columns.

    This defines metadata fields that apply to all column types.
    """

    units: str  # Physical units (e.g., "mg", "°C", "mW", "mg/min")
    processing_history: list[
        str
    ]  # Processing steps applied (e.g., ["raw", "smoothed"])
    source: str  # Data origin (e.g., "measurement", "calculated", "derived")


class BaselinableColumnMetadata(BaseColumnMetadata, total=False):
    """Extended metadata for columns that support baseline correction and calibration.

    This includes the baseline_subtracted field for signals like mass and DSC
    that can be baseline-corrected, and calibration_applied for DSC signals
    that can be calibrated from µV to mW.
    """

    baseline_subtracted: bool  # True if baseline correction has been applied
    calibration_applied: bool  # True if calibration has been applied (DSC only)


# Define which metadata fields apply to which column types
FIELD_APPLICABILITY = {
    "units": "all",  # All columns have units
    "processing_history": "all",  # All columns have processing history
    "source": "all",  # All columns have a source
    "baseline_subtracted": [
        "mass",
        "dsc_signal",
    ],  # Only these can be baseline corrected
    "calibration_applied": [
        "dsc_signal",
    ],  # Only DSC signals can be calibrated
}

# Default metadata for common column types
DEFAULT_COLUMN_METADATA = {
    "time": {"units": "s", "processing_history": ["raw"], "source": "measurement"},
    "mass": {
        "units": "mg",
        "processing_history": ["raw"],
        "source": "measurement",
        "baseline_subtracted": False,
    },
    "sample_temperature": {
        "units": "°C",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "furnace_temperature": {
        "units": "°C",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "dsc_signal": {
        "units": "µV",
        "processing_history": ["raw"],
        "source": "measurement",
        "baseline_subtracted": False,
        "calibration_applied": False,
    },
    "dtg": {
        "units": "mg/min",
        "processing_history": ["calculated"],
        "source": "derived",
    },
    "purge_flow_1": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "purge_flow_2": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "protective_flow": {
        "units": "ml/min",
        "processing_history": ["raw"],
        "source": "measurement",
    },
    "furnace_power": {
        "units": "W",
        "processing_history": ["raw"],
        "source": "measurement",
    },
}


class DataType(Enum):
    """Binary data type identifiers used in NGB files.

    These constants map to the binary identifiers used in NETZSCH NGB files
    to specify the data type of values stored in the binary format.

    Examples:
        >>> DataType.FLOAT64.value
        b'\\x05'
        >>> data_type == DataType.FLOAT32.value
        True
    """

    INT32 = b"\x03"  # 32-bit signed integer (little-endian)
    FLOAT32 = b"\x04"  # 32-bit IEEE 754 float (little-endian)
    FLOAT64 = b"\x05"  # 64-bit IEEE 754 double (little-endian)
    STRING = b"\x1f"  # Enhanced string parsing: supports both standard (4-byte length + UTF-8) and NETZSCH (fffeff + char_count + UTF-16LE) formats


@dataclass(frozen=True, slots=True)
class BinaryMarkers:
    """Binary markers for parsing NGB files.

    These byte sequences mark important boundaries and structures within
    the binary NGB file format. They are used to locate data sections,
    separate tables, and identify data types.

    Attributes:
        END_FIELD: Marks the end of a data field
        TYPE_PREFIX: Precedes data type identifier
        TYPE_SEPARATOR: Separates type from value data
        END_TABLE: Marks the end of a table
        TABLE_SEPARATOR: Separates individual tables in a stream
        START_DATA: Marks the beginning of data payload
        END_DATA: Marks the end of data payload
    """

    END_FIELD: bytes = b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
    TYPE_PREFIX: bytes = b"\x17\xfc\xff\xff"
    TYPE_SEPARATOR: bytes = b"\x80\x01"
    END_TABLE: bytes = b"\x18\xfc\xff\xff\x03"
    TABLE_SEPARATOR: bytes = b"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x1a\x80\x01\x01\x80\x02\x00\x00"
    START_DATA: bytes = b"\xa0\x01"
    END_DATA: bytes = (
        b"\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x18\xfc\xff\xff\x03\x80\x01"
    )


# Constants for binary parsing - moved to BinaryProcessing dataclass


@dataclass
class PatternConfig:
    """Configuration for metadata and column patterns.

    This class defines the binary patterns used to locate and extract
    specific metadata fields, temperature program data, calibration constants,
    and data columns from NGB files.

    The patterns are defined as tuples of (category_bytes, field_bytes) that
    are used to construct regex patterns for finding specific data fields
    in the binary stream.

    Attributes:
        metadata_patterns: Maps field names to (category, field) byte patterns
        temp_prog_patterns: Patterns for temperature program extraction
        cal_constants_patterns: Patterns for calibration constant extraction
        column_map: Maps hex column IDs to human-readable column names

    Example:
        >>> config = PatternConfig()
        >>> config.column_map["8d"] = "time"
        >>> config.metadata_patterns["sample_id"] = (b"\\x30\\x75", b"\\x98\\x08")

    Note:
        Modifying these patterns may break compatibility with certain
        NGB file versions. Use caution when customizing.
    """

    metadata_patterns: dict[str, tuple[bytes, bytes]] = field(
        default_factory=lambda: {
            # Core metadata
            "instrument": (rb"\x75\x17", rb"\x59\x10"),
            "project": (rb"\x72\x17", rb"\x3c\x08"),
            "date_performed": (rb"\x72\x17", rb"\x3e\x08"),
            "lab": (rb"\x72\x17", rb"\x34\x08"),
            "operator": (rb"\x72\x17", rb"\x35\x08"),
            "crucible_type": (rb"\x7e\x17", rb"\x40\x08"),
            "comment": (rb"\x72\x17", rb"\x3d\x08"),
            "furnace_type": (rb"\x7a\x17", rb"\x40\x08"),
            "carrier_type": (rb"\x79\x17", rb"\x40\x08"),
            # Sample descriptors
            "sample_id": (rb"\x30\x75", rb"\x98\x08"),
            "sample_name": (rb"\x30\x75", rb"\x40\x08"),
            # Mass fields: crucible_mass pattern ALSO matches reference crucible mass (structural disambiguation required)
            "sample_mass": (rb"\x30\x75", rb"\x9e\x0c"),
            "crucible_mass": (rb"\x7e\x17", rb"\x9e\x0c"),
            # Additional
            "material": (rb"\x30\x75", rb"\x62\x09"),
            # Note: MFC fields are handled separately in _extract_mfc_metadata
            # to avoid conflicts with the general pattern matching
        }
    )
    temp_prog_patterns: dict[str, bytes] = field(
        default_factory=lambda: {
            "stage_type": b"\x3f\x08",
            "temperature": b"\x17\x0e",
            "heating_rate": b"\x13\x0e",
            "acquisition_rate": b"\x14\x0e",
            "time": b"\x15\x0e",
        }
    )

    # Temperature program binary structure constants
    temp_prog_type_separator: bytes = b"\x00\x00\x01\x00\x00\x00"
    temp_prog_data_type: bytes = b"\x0c"
    temp_prog_field_separator: bytes = b"\x00\x17\xfc\xff\xff"
    temp_prog_value_prefix: bytes = b"\x04\x80\x01"
    cal_constants_patterns: dict[str, bytes] = field(
        default_factory=lambda: {
            **{f"p{i}": bytes([0x4F + i, 0x04]) for i in range(5)},
            "p5": b"\xc3\x04",
        }
    )
    # Temperature-calibration fixpoint scalar field ids (within 30 75 .. 34 75 tables).
    # Columns of the Proteus calibration table: actual / measured / weight / corrected.
    temperature_cal_patterns: dict[str, bytes] = field(
        default_factory=lambda: {
            "name": b"\x43\x04",
            "actual_c": b"\x44\x04",
            "measured_c": b"\x45\x04",
            "weight": b"\x46\x04",
            "corrected_c": b"\x47\x04",
        }
    )
    # Maps a channel's true header ID (first byte of its header table, hex) to
    # its public column name. Stream_2 headers precede their data tables;
    # stream_3 works the same way. Channel 87 is a trailing header that carries
    # no data and is intentionally unmapped. Unmapped IDs pass through as
    # hex-string column names (e.g. the all-zero stream_3 channel "31").
    column_map: dict[str, str] = field(
        default_factory=lambda: {
            # stream_2 channels
            "8c": "time",
            "8d": "sample_temperature",
            "8e": "dsc_signal",
            "9c": "purge_flow_1",
            "9d": "purge_flow_2",
            "9e": "protective_flow",
            "90": "mass",
            # stream_3 channels
            "30": "furnace_temperature",
            "32": "furnace_power",
            "33": "h_foil_temperature",
            "34": "uc_module",
            "35": "environmental_pressure",
            "36": "environmental_acceleration_x",
            "37": "environmental_acceleration_y",
            "38": "environmental_acceleration_z",
        }
    )


# Structural signature fragments used to differentiate sample vs reference crucible mass
# occurrences when both share identical (category, field) byte patterns.
SAMPLE_CRUCIBLE_SIG_FRAGMENT = (
    b"\x83\x0c\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x04\x80\x01"
)
REF_CRUCIBLE_SIG_FRAGMENT = (
    b"\xc4\x10\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x02\x80\x01"
)

# Binary structure constants for metadata extraction
TEMP_PROG_TYPE_PREFIX = b"\x03\x80\x01"

# Control parameter signatures
CONTROL_SIGNATURES = {
    0x0FE7: "xp",  # proportional gain
    0x0FE8: "tn",  # integral time
    0x0FE9: "tv",  # derivative time
}

# Gas types for MFC metadata
GAS_TYPES = ["NITROGEN", "OXYGEN", "ARGON", "HELIUM", "CARBON_DIOXIDE"]

# MFC field names
MFC_FIELD_NAMES = ["Purge 1", "Purge 2", "Protective"]

# Application and license extraction constants
APP_LICENSE_CATEGORY = b"\x00\x03"
APP_LICENSE_FIELD = b"\x18\xfc"
STRING_DATA_TYPE = b"\x1f"

# Temperature-calibration extraction constants
# Coefficients live in an f7 01 table as a float32 data array anchored on field be 04:
#   be 04 | 00 00 01 00 00 00 | 0c 00 | 17 fc ff ff | 10 | a0 01 | <count u32 LE> | <data>
TEMP_CAL_COEFF_CATEGORY = b"\xf7\x01"
TEMP_CAL_COEFF_SIGNATURE = (
    b"\xbe\x04\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x10\xa0\x01"
)
# Fixpoint tables are categorised 30 75 .. 34 75 (one per standard, ascending temp)
TEMP_CAL_FIXPOINT_CATEGORIES = (
    b"\x30\x75",
    b"\x31\x75",
    b"\x32\x75",
    b"\x33\x75",
    b"\x34\x75",
)
# External calibration record path suffixes (UTF-16LE in the f5 01 tables)
TEMP_CAL_RECORD_SUFFIX = ".ngb-ts3"  # temperature calibration record
SENSITIVITY_RECORD_SUFFIX = ".ngb-es3"  # DSC sensitivity calibration record


@dataclass(frozen=True, slots=True)
class StreamMarkers:
    """Binary markers specific to NGB stream processing."""

    # Stream 2 markers
    STREAM2_HEADER: bytes = b"\x17"
    STREAM2_DATA: bytes = b"\x75"

    # Stream 3 markers
    STREAM3_HEADER: bytes = b"\x80\x22\x2b"
    STREAM3_DATA: bytes = b"\x75"  # Same as stream 2

    # Position markers
    STREAM2_HEADER_POS: int = 1  # table[1:2]
    STREAM3_HEADER_POS: int = 22  # table[22:25]
    DATA_MARKER_POS: int = 1  # table[1:2]


@dataclass(frozen=True, slots=True)
class BinaryProcessing:
    """Constants for binary data processing."""

    TABLE_SPLIT_OFFSET: int = -2
    START_DATA_HEADER_OFFSET: int = 6

    # Memory management
    DEFAULT_MEMORY_LIMIT_MB: int = 500
    LARGE_FILE_THRESHOLD_MB: int = 100


@dataclass(frozen=True, slots=True)
class DataTypeSizes:
    """Expected byte sizes for different data types."""

    INT32_BYTES: int = 4
    FLOAT32_BYTES: int = 4
    FLOAT64_BYTES: int = 8
    STRING_MIN_BYTES: int = 4  # Length prefix minimum


@dataclass(frozen=True, slots=True)
class PatternOffsets:
    """Byte offsets and window sizes for pattern matching."""

    # Crucible mass extraction
    CRUCIBLE_MASS_SEARCH_WINDOW: int = 256
    CRUCIBLE_MASS_PREVIEW_SIZE: int = 64

    # Control parameters
    CONTROL_PARAM_SEARCH_OFFSET: int = 200

    # Application license
    APP_LICENSE_SEARCH_RANGE: int = 120

    # MFC signature values
    MFC_SIGNATURE: int = 0x1048
    GAS_CONTEXT_SIGNATURE: int = 0x1B


@dataclass(frozen=True)
class ValidationThresholds:
    """Thresholds for data validation."""

    # Temperature ranges (Celsius)
    MIN_TEMPERATURE: float = -200.0
    MAX_TEMPERATURE: float = 2000.0

    # Mass ranges (mg)
    MIN_MASS: float = 0.0
    MAX_MASS: float = 10000.0

    # Flow rates (ml/min)
    MIN_FLOW_RATE: float = 0.1
    MAX_FLOW_RATE: float = 1000.0

    # Heating rates (K/min)
    MIN_HEATING_RATE: float = 0.0
    MAX_HEATING_RATE: float = 100.0
