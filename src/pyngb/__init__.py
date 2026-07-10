# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
pyngb: A Python library for parsing NETZSCH STA NGB files.
"""

from importlib.metadata import PackageNotFoundError, version

from .analysis import dtg, dtg_custom
from .api.analysis import (
    add_dtg,
    apply_dsc_calibration,
    calculate_table_dtg,
    normalize_to_initial_mass,
)
from .api.loaders import read_ngb, read_ngb_metadata
from .api.metadata import (
    get_column_units,
    set_column_units,
    mark_baseline_corrected,
    get_column_baseline_status,
    inspect_column_metadata,
)
from .baseline import BaselineSubtractor
from .batch import (
    BatchProcessor,
    BatchResult,
    NGBDataset,
    process_directory,
    process_files,
)
from .config import ParsingConfig
from .constants import (
    FileMetadata,
    BaseColumnMetadata,
    BaselinableColumnMetadata,
    SensitivityCalibration,
    SensitivityFixpoint,
    TemperatureCalibration,
    TemperatureFixpoint,
)
from .exceptions import (
    NGBCorruptedFileError,
    NGBDataTypeError,
    NGBParseError,
    NGBResourceLimitError,
    NGBStreamNotFoundError,
)
from .format import Field, NGBDocument, Table, load_document
from .validation import QualityChecker, ValidationResult, validate_sta_data

try:
    __version__ = version("pyngb")
except PackageNotFoundError:
    __version__ = "0.0.0"
__author__ = "Grayson Bellamy"
__email__ = "gbellamy@umd.edu"

__all__ = [
    "BaseColumnMetadata",
    "BaselinableColumnMetadata",
    "BaselineSubtractor",
    "BatchProcessor",
    "BatchResult",
    "Field",
    "FileMetadata",
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBDataset",
    "NGBDocument",
    "NGBParseError",
    "NGBResourceLimitError",
    "NGBStreamNotFoundError",
    "ParsingConfig",
    "QualityChecker",
    "SensitivityCalibration",
    "SensitivityFixpoint",
    "Table",
    "TemperatureCalibration",
    "TemperatureFixpoint",
    "ValidationResult",
    "__author__",
    "__email__",
    "__version__",
    "add_dtg",
    "apply_dsc_calibration",
    "calculate_table_dtg",
    "dtg",
    "dtg_custom",
    "get_column_baseline_status",
    # Metadata functions
    "get_column_units",
    "inspect_column_metadata",
    "load_document",
    "mark_baseline_corrected",
    "normalize_to_initial_mass",
    # Other functions
    "process_directory",
    "process_files",
    "read_ngb",
    "read_ngb_metadata",
    "set_column_units",
    "validate_sta_data",
]
