"""Data validation and quality checking tools for STA data.

This module provides comprehensive validation of STA thermal analysis data,
organized into focused validators for different aspects of the data.
"""

import logging

from .base import ValidationResult
from .checker import QualityChecker
from .functions import (
    check_dsc_data,
    check_mass_data,
    check_temperature_profile,
    validate_sta_data,
)

__all__ = [
    "QualityChecker",
    "ValidationResult",
    "check_dsc_data",
    "check_mass_data",
    "check_temperature_profile",
    "validate_sta_data",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
