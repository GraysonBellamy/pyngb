# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
pyngb: A Python library for parsing NETZSCH STA NGB files.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .api.loaders import read_ngb
from .baseline import BaselineSubtractor, subtract_baseline
from .batch import BatchProcessor, NGBDataset, process_directory, process_files
from .constants import BinaryMarkers, DataType, FileMetadata, PatternConfig
from .core.parser import NGBParser
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
    "BaselineSubtractor",
    "BatchProcessor",
    "BinaryMarkers",
    "DataType",
    "FileMetadata",
    "NGBCorruptedFileError",
    "NGBDataTypeError",
    "NGBDataset",
    "NGBParseError",
    "NGBParser",
    "NGBStreamNotFoundError",
    "NGBUnsupportedVersionError",
    "PatternConfig",
    "QualityChecker",
    "ValidationResult",
    "__author__",
    "__email__",
    "__version__",
    "process_directory",
    "process_files",
    "read_ngb",
    "subtract_baseline",
    "validate_sta_data",
]
