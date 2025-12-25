"""Configuration validation for pyNGB constants and thresholds."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def validate_thresholds() -> None:
    """Validate ValidationThresholds configuration at import time.

    Raises:
        NGBConfigurationError: If any threshold configuration is invalid
    """
    from pyngb.constants import ValidationThresholds
    from pyngb.exceptions import NGBConfigurationError

    errors = []

    # Temperature bounds
    if ValidationThresholds.MIN_TEMPERATURE >= ValidationThresholds.MAX_TEMPERATURE:
        errors.append(
            f"MIN_TEMPERATURE ({ValidationThresholds.MIN_TEMPERATURE}) must be "
            f"less than MAX_TEMPERATURE ({ValidationThresholds.MAX_TEMPERATURE})"
        )

    # Physical constraints
    if ValidationThresholds.MIN_TEMPERATURE < -273.15:
        errors.append(
            f"MIN_TEMPERATURE ({ValidationThresholds.MIN_TEMPERATURE}) cannot be "
            "below absolute zero (-273.15°C)"
        )

    # Mass constraints
    if ValidationThresholds.MIN_MASS < 0:
        errors.append(f"MIN_MASS ({ValidationThresholds.MIN_MASS}) cannot be negative")

    if ValidationThresholds.MAX_MASS <= ValidationThresholds.MIN_MASS:
        errors.append(
            f"MAX_MASS ({ValidationThresholds.MAX_MASS}) must be greater than "
            f"MIN_MASS ({ValidationThresholds.MIN_MASS})"
        )

    # Flow rate constraints
    if hasattr(ValidationThresholds, "MIN_FLOW_RATE"):
        if ValidationThresholds.MIN_FLOW_RATE < 0:
            errors.append(
                f"MIN_FLOW_RATE ({ValidationThresholds.MIN_FLOW_RATE}) cannot be negative"
            )

        if (
            hasattr(ValidationThresholds, "MAX_FLOW_RATE")
            and ValidationThresholds.MAX_FLOW_RATE <= ValidationThresholds.MIN_FLOW_RATE
        ):
            errors.append(
                f"MAX_FLOW_RATE ({ValidationThresholds.MAX_FLOW_RATE}) must be greater than "
                f"MIN_FLOW_RATE ({ValidationThresholds.MIN_FLOW_RATE})"
            )

    if errors:
        raise NGBConfigurationError(
            "Invalid ValidationThresholds configuration detected:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )


def validate_pattern_config(config: Any) -> None:
    """Validate PatternConfig column map hex IDs.

    Args:
        config: PatternConfig instance to validate

    Raises:
        NGBConfigurationError: If any pattern configuration is invalid
    """
    from pyngb.exceptions import NGBConfigurationError

    if not hasattr(config, "column_map"):
        return

    errors = []
    for hex_id, column_name in config.column_map.items():
        try:
            int(hex_id, 16)
        except ValueError:
            errors.append(
                f"Invalid hex column ID '{hex_id}' for column '{column_name}'"
            )

    if errors:
        raise NGBConfigurationError(
            "Invalid PatternConfig column_map:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )


def validate_all_configs() -> None:
    """Run all configuration validations.

    This function is called at module import to ensure all configurations
    are valid before any parsing operations begin.

    Raises:
        NGBConfigurationError: If any configuration validation fails
    """
    validate_thresholds()

    # Validate default PatternConfig instance
    from pyngb.constants import PatternConfig

    validate_pattern_config(PatternConfig())

    logger.debug("All configuration validations passed")


# Run validation at import time
validate_all_configs()
