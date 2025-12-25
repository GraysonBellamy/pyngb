"""Centralized configuration for pyNGB.

This module provides configuration classes for various aspects of the library,
including parsing, validation, and batch processing settings.
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ParsingConfig:
    """Configuration for binary parsing operations.

    Attributes:
        max_file_size_mb: Maximum file size in MB to parse (default: 1000)
        max_tables_per_stream: Maximum number of tables per stream (default: 100)
        max_array_size_mb: Maximum array size in MB (default: 500)
        encoding_fallback: Fallback encoding for strings (default: "utf-8")
    """

    max_file_size_mb: int = 1000
    max_tables_per_stream: int = 100
    max_array_size_mb: int = 500
    encoding_fallback: str = "utf-8"

    def __post_init__(self) -> None:
        """Validate parsing configuration."""
        if self.max_file_size_mb <= 0:
            raise ValueError("max_file_size_mb must be positive")
        if self.max_tables_per_stream <= 0:
            raise ValueError("max_tables_per_stream must be positive")
        if self.max_array_size_mb <= 0:
            raise ValueError("max_array_size_mb must be positive")


@dataclass(frozen=True, slots=True)
class ValidationConfig:
    """Configuration for data validation.

    Attributes:
        min_temperature: Minimum valid temperature in °C (default: -273.15, absolute zero)
        max_temperature: Maximum valid temperature in °C (default: 2000.0)
        warn_min_temperature: Warn if temperature below this value (default: -50.0)
        warn_max_temperature: Warn if temperature above this value (default: 2000.0)
        min_temperature_range: Minimum temperature range for valid experiment (default: 10.0)
        max_mass_mg: Maximum valid mass in mg (default: 10000.0)
        min_time_interval: Minimum time interval between points in seconds (default: 0.0)
        max_time_interval: Maximum time interval between points in seconds (default: 3600.0)
    """

    min_temperature: float = -273.15  # Absolute zero
    max_temperature: float = 3000.0  # Maximum instrument capability
    warn_min_temperature: float = -50.0
    warn_max_temperature: float = 2000.0
    min_temperature_range: float = 10.0
    max_mass_mg: float = 10000.0
    min_time_interval: float = 0.0
    max_time_interval: float = 3600.0  # 1 hour

    def __post_init__(self) -> None:
        """Validate validation configuration."""
        if self.min_temperature >= self.max_temperature:
            raise ValueError("min_temperature must be less than max_temperature")
        if self.warn_min_temperature < self.min_temperature:
            raise ValueError("warn_min_temperature cannot be less than min_temperature")
        if self.warn_max_temperature > self.max_temperature:
            raise ValueError("warn_max_temperature cannot exceed max_temperature")
        if self.min_temperature_range <= 0:
            raise ValueError("min_temperature_range must be positive")
        if self.max_mass_mg <= 0:
            raise ValueError("max_mass_mg must be positive")
        if self.min_time_interval < 0:
            raise ValueError("min_time_interval cannot be negative")
        if self.max_time_interval <= self.min_time_interval:
            raise ValueError("max_time_interval must be greater than min_time_interval")


@dataclass(frozen=True, slots=True)
class BatchConfig:
    """Configuration for batch processing.

    Attributes:
        max_workers: Maximum number of worker processes (default: None = CPU count)
        chunk_size: Number of files per processing chunk (default: 10)
        max_memory_gb: Maximum memory usage in GB (default: 4.0)
        skip_errors: Whether to skip files with errors (default: False)
        show_progress: Whether to show progress indicators (default: True)
    """

    max_workers: int | None = None  # None = cpu_count
    chunk_size: int = 10
    max_memory_gb: float = 4.0
    skip_errors: bool = False
    show_progress: bool = True

    def __post_init__(self) -> None:
        """Validate batch configuration."""
        if self.max_workers is not None and self.max_workers <= 0:
            raise ValueError("max_workers must be positive or None")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.max_memory_gb <= 0:
            raise ValueError("max_memory_gb must be positive")


@dataclass
class PyNGBConfig:
    """Main configuration container for pyNGB.

    This class aggregates all configuration sections and provides validation.

    Attributes:
        parsing: Configuration for binary parsing operations
        validation: Configuration for data validation
        batch: Configuration for batch processing

    Examples:
        >>> # Use default configuration
        >>> config = PyNGBConfig()
        >>>
        >>> # Customize configuration
        >>> config = PyNGBConfig(
        ...     parsing=ParsingConfig(max_file_size_mb=2000),
        ...     validation=ValidationConfig(max_temperature=2500.0),
        ...     batch=BatchConfig(max_workers=4)
        ... )
        >>>
        >>> # Access configuration values
        >>> max_size = config.parsing.max_file_size_mb
        >>> max_temp = config.validation.max_temperature
    """

    parsing: ParsingConfig = field(default_factory=ParsingConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Individual configs are validated in their own __post_init__
        # This can be extended for cross-config validation if needed

    @classmethod
    def from_env(cls) -> "PyNGBConfig":
        """Create configuration from environment variables.

        Supported environment variables:
        - PYNGB_MAX_FILE_SIZE_MB: Maximum file size in MB
        - PYNGB_MAX_WORKERS: Maximum number of worker processes
        - PYNGB_SKIP_ERRORS: Whether to skip files with errors (true/false)

        Returns:
            Configuration instance with values from environment
        """
        parsing_config = ParsingConfig(
            max_file_size_mb=int(os.getenv("PYNGB_MAX_FILE_SIZE_MB", "1000"))
        )

        validation_config = ValidationConfig()

        batch_config = BatchConfig(
            max_workers=int(os.getenv("PYNGB_MAX_WORKERS", 0)) or None,
            skip_errors=os.getenv("PYNGB_SKIP_ERRORS", "false").lower() == "true",
        )

        return cls(
            parsing=parsing_config,
            validation=validation_config,
            batch=batch_config,
        )


# Global default configuration
DEFAULT_CONFIG = PyNGBConfig()


__all__ = [
    "DEFAULT_CONFIG",
    "BatchConfig",
    "ParsingConfig",
    "PyNGBConfig",
    "ValidationConfig",
]
