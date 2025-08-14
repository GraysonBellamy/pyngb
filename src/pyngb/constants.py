"""
Constants, enums, and configuration classes for NGB parsing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypedDict

__all__ = [  # noqa: RUF022 - order chosen for logical grouping
    "BinaryMarkers",
    "DataType",
    "FileMetadata",
    "PatternConfig",
    "REF_CRUCIBLE_SIG_FRAGMENT",
    "SAMPLE_CRUCIBLE_SIG_FRAGMENT",
]


class FileMetadata(TypedDict, total=False):
    """Type definition for file metadata dictionary.

    Mass-related fields grouped together after core identifying fields. Reference masses
    are structurally derived; crucible_mass pattern also matches reference_crucible_mass and
    is disambiguated using signature fragments (see SAMPLE_CRUCIBLE_SIG_FRAGMENT / REF_CRUCIBLE_SIG_FRAGMENT).
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
    STRING = b"\x1f"  # UTF-8 string with 4-byte length prefix


@dataclass(frozen=True)
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


# Constants for binary parsing
START_DATA_HEADER_OFFSET = 6  # Bytes to skip after START_DATA marker to reach payload


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
            f"p{i}": bytes([0x4F + i, 0x04]) if i < 5 else b"\xc3\x04" for i in range(6)
        }
    )
    column_map: dict[str, str] = field(
        default_factory=lambda: {
            "8d": "time",
            "8e": "sample_temperature",
            "9c": "dsc_signal",
            "9d": "purge_flow_1",
            "9e": "purge_flow_2",
            "90": "protective_flow",
            "87": "mass",
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
MFC_SIGNATURE = 0x1048  # MFC range signature
GAS_CONTEXT_SIGNATURE = 0x1B

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

# Search window sizes for structural extraction
CRUCIBLE_MASS_SEARCH_WINDOW = 256
CRUCIBLE_MASS_PREVIEW_SIZE = 64
CONTROL_PARAM_SEARCH_OFFSET = 200

# Application and license extraction constants
APP_LICENSE_CATEGORY = b"\x00\x03"
APP_LICENSE_FIELD = b"\x18\xfc"
APP_LICENSE_SEARCH_RANGE = 120
STRING_DATA_TYPE = b"\x1f"
