# SPDX-FileCopyrightText: 2025-present GraysonBellamy <gbellamy@umd.edu>
#
# SPDX-License-Identifier: MIT

"""
NETZSCH STA NGB File Parser

A Python library for parsing NETZSCH STA (Simultaneous Thermal Analysis)
NGB (NETZSCH Binary) files containing thermal analysis data.

Basic Usage:
    >>> from pynetzsch import load_ngb_data
    >>> table = load_ngb_data("sample.ngb-ss3")
    >>> print(f"Columns: {table.column_names}")
    >>> print(f"Rows: {table.num_rows}")

Advanced Usage:
    >>> from pynetzsch import get_sta_data, NGBParser
    >>> metadata, data = get_sta_data("sample.ngb-ss3")
    >>> parser = NGBParser()
    >>> # Custom parsing...
"""

from .api import load_ngb_data, get_sta_data, main
from .core import NGBParser, NGBParserExtended
from .exceptions import (
    NGBParseError,
    NGBCorruptedFileError,
    NGBUnsupportedVersionError,
    NGBDataTypeError,
    NGBStreamNotFoundError,
)
from .constants import PatternConfig, DataType

__all__ = [
    # Main API functions
    "load_ngb_data",
    "get_sta_data",
    "main",
    # Parser classes
    "NGBParser",
    "NGBParserExtended",
    # Configuration
    "PatternConfig",
    "DataType",
    # Exceptions
    "NGBParseError",
    "NGBCorruptedFileError",
    "NGBUnsupportedVersionError",
    "NGBDataTypeError",
    "NGBStreamNotFoundError",
]
