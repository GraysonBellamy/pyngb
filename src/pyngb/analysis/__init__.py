"""
Simplified DTG (Derivative Thermogravimetry) analysis.

This module provides clean, simple tools for DTG calculation focused on
the most common use cases.
"""

from __future__ import annotations

from .dtg import dtg, dtg_custom

__all__ = [
    "dtg",
    "dtg_custom",
]
