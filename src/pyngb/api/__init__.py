"""
Public API functions for loading and analyzing NGB data.
"""

from .analysis import add_dtg, calculate_table_dtg
from .loaders import main, read_ngb

__all__ = [
    "add_dtg",
    "calculate_table_dtg",
    "main",
    "read_ngb",
]
