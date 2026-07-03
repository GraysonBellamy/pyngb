# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
pyngb: A Python library for parsing NETZSCH STA NGB files.
"""

from importlib.metadata import PackageNotFoundError, version

from .analysis import dtg, dtg_custom
from .api.analysis import add_dtg, calculate_table_dtg, normalize_to_initial_mass
from .api.loaders import read_ngb
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
    BinaryMarkers,
    DataType,
    FileMetadata,
    FileMetadataRequired,
    PatternConfig,
    BaseColumnMetadata,
    BaselinableColumnMetadata,
    TemperatureCalibration,
    TemperatureFixpoint,
)
from .core.parser import NGBParser
from .exceptions import (
    NGBBaselineError,
    NGBCorruptedFileError,
    NGBDataTypeError,
    NGBParseError,
    NGBResourceLimitError,
    NGBStreamNotFoundError,
    NGBValidationError,
)
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
    "BinaryMarkers",
    "DataType",
    "FileMetadata",
    "FileMetadataRequired",
    "NGBBaselineError",
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBDataset",
    "NGBParseError",
    "NGBParser",
    "NGBResourceLimitError",
    "NGBStreamNotFoundError",
    "NGBValidationError",
    "ParsingConfig",
    "PatternConfig",
    "QualityChecker",
    "TemperatureCalibration",
    "TemperatureFixpoint",
    "ValidationResult",
    "__author__",
    "__email__",
    "__version__",
    "add_dtg",
    "calculate_table_dtg",
    "dtg",
    "dtg_custom",
    "get_column_baseline_status",
    # Metadata functions
    "get_column_units",
    "inspect_column_metadata",
    "mark_baseline_corrected",
    "normalize_to_initial_mass",
    # Other functions
    "process_directory",
    "process_files",
    "read_ngb",
    "set_column_units",
    "validate_sta_data",
]
