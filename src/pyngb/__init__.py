# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
pyngb: A Python library for parsing NETZSCH STA NGB files.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .api.loaders import get_sta_data, load_ngb_data
from .batch import BatchProcessor, NGBDataset, process_directory, process_files
from .constants import BinaryMarkers, DataType, FileMetadata, PatternConfig
from .core.parser import NGBParser, NGBParserExtended
from .exceptions import (
    NGBCorruptedFileError,
    NGBDataTypeError,
    NGBParseError,
    NGBStreamNotFoundError,
    NGBUnsupportedVersionError,
)
from .validation import QualityChecker, ValidationResult, validate_sta_data

try:
    __version__ = version("pyngb")
except PackageNotFoundError:
    __version__ = "0.0.0"
__author__ = "Grayson Bellamy"
__email__ = "gbellamy@umd.edu"

__all__ = [
    "BatchProcessor",
    "BinaryMarkers",
    "DataType",
    "FileMetadata",
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBDataset",
    "NGBParseError",
    "NGBParser",
    "NGBParserExtended",
    "NGBStreamNotFoundError",
    "NGBUnsupportedVersionError",
    "PatternConfig",
    "QualityChecker",
    "ValidationResult",
    "__author__",
    "__email__",
    "__version__",
    "get_sta_data",
    "load_ngb_data",
    "process_directory",
    "process_files",
    "validate_sta_data",
]
