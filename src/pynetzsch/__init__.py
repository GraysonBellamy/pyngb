# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
PyNetzsch: A Python library for parsing NETZSCH STA NGB files.
"""

from __future__ import annotations

from .api.loaders import get_sta_data, load_ngb_data
from .constants import BinaryMarkers, DataType, FileMetadata, PatternConfig
from .core.parser import NGBParser, NGBParserExtended
from .exceptions import (
    NGBCorruptedFileError,
    NGBDataTypeError,
    NGBParseError,
    NGBStreamNotFoundError,
    NGBUnsupportedVersionError,
)

__version__ = "0.1.0"
__author__ = "Grayson Bellamy"
__email__ = "gbellamy@umd.edu"

__all__ = [
    # Core functions
    "load_ngb_data",
    "get_sta_data",
    # Parser classes
    "NGBParser",
    "NGBParserExtended",
    # Configuration and types
    "PatternConfig",
    "DataType",
    "BinaryMarkers",
    "FileMetadata",
    # Exceptions
    "NGBParseError",
    "NGBCorruptedFileError",
    "NGBUnsupportedVersionError",
    "NGBDataTypeError",
    "NGBStreamNotFoundError",
    # Metadata
    "__version__",
    "__author__",
    "__email__",
]
