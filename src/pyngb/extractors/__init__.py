"""
Data extraction components for NGB parsing.
"""

from .manager import MetadataExtractor
from .streams import DataStreamProcessor

__all__ = [
    "DataStreamProcessor",
    "MetadataExtractor",
]
