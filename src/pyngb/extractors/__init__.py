"""
Data extraction components for NGB parsing.
"""

from .metadata import MetadataExtractor
from .streams import DataStreamProcessor

__all__ = ["DataStreamProcessor", "MetadataExtractor"]
