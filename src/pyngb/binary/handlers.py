"""
Data type handlers and registry for binary data parsing.
"""

from __future__ import annotations

import logging
from typing import Protocol

import numpy as np

from ..constants import DataType
from ..exceptions import NGBDataTypeError

__all__ = [
    "DataTypeHandler",
    "DataTypeRegistry",
    "Float32Handler",
    "Float64Handler",
    "Int32Handler",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataTypeHandler(Protocol):
    """Protocol for data type handlers."""

    def can_handle(self, data_type: bytes) -> bool:
        """Check if this handler can process the given data type."""
        ...

    def parse_data(self, data: bytes | memoryview) -> list[float]:
        """Parse binary data and return list of floats."""
        ...


class Float64Handler:
    """Handler for 64-bit IEEE 754 double precision floating point data.

    This handler processes binary data containing arrays of 64-bit doubles
    stored in little-endian format. Uses NumPy's frombuffer for optimal
    performance.

    Example:
        >>> handler = Float64Handler()
        >>> handler.can_handle(b'\\x05')  # DataType.FLOAT64.value
        True
        >>> data = b'\\x00\\x00\\x00\\x00\\x00\\x00\\xf0\\x3f'  # 1.0 as double
        >>> handler.parse_data(data)
        [1.0]
    """

    def can_handle(self, data_type: bytes) -> bool:
        return data_type == DataType.FLOAT64.value

    def parse_data(self, data: bytes | memoryview) -> list[float]:
        arr = np.frombuffer(data, dtype="<f8")
        return [float(x) for x in arr]


class Float32Handler:
    """Handler for 32-bit IEEE 754 single precision floating point data.

    This handler processes binary data containing arrays of 32-bit floats
    stored in little-endian format. Uses NumPy's frombuffer for optimal
    performance.

    Example:
        >>> handler = Float32Handler()
        >>> handler.can_handle(b'\\x04')  # DataType.FLOAT32.value
        True
        >>> data = b'\\x00\\x00\\x80\\x3f'  # 1.0 as float
        >>> handler.parse_data(data)
        [1.0]
    """

    def can_handle(self, data_type: bytes) -> bool:
        return data_type == DataType.FLOAT32.value

    def parse_data(self, data: bytes | memoryview) -> list[float]:
        arr = np.frombuffer(data, dtype="<f4")
        return [float(x) for x in arr]


class Int32Handler:
    """Handler for 32-bit signed integer data.

    This handler processes binary data containing arrays of 32-bit integers
    stored in little-endian format. Uses NumPy's frombuffer for optimal
    performance.

    Example:
        >>> handler = Int32Handler()
        >>> handler.can_handle(b'\\x03')  # DataType.INT32.value
        True
        >>> data = b'\\x2a\\x00\\x00\\x00'  # 42 as little-endian int32
        >>> handler.parse_data(data)
        [42.0]
    """

    def can_handle(self, data_type: bytes) -> bool:
        return data_type == DataType.INT32.value

    def parse_data(self, data: bytes | memoryview) -> list[float]:
        arr = np.frombuffer(data, dtype="<i4")
        return [float(x) for x in arr]


class DataTypeRegistry:
    """Registry for data type handlers with pluggable architecture.

    This registry manages a collection of data type handlers that can
    process different binary data formats found in NGB files. New handlers
    can be registered to extend support for additional data types.

    The registry uses a chain-of-responsibility pattern to find the
    appropriate handler for each data type.

    Example:
        >>> registry = DataTypeRegistry()
        >>> registry.parse_data(b'\\x05', binary_data)  # Uses Float64Handler
        [1.0, 2.0, 3.0]

        >>> # Add custom handler
        >>> class CustomHandler:
        ...     def can_handle(self, data_type): return data_type == b'\\x06'
        ...     def parse_data(self, data): return [42.0]
        >>> registry.register(CustomHandler())

    Attributes:
        _handlers: List of registered data type handlers

    Note:
        Handlers are checked in registration order. Register more specific
        handlers before more general ones.
    """

    def __init__(self) -> None:
        self._handlers: list[DataTypeHandler] = []
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default data type handlers."""
        self.register(Int32Handler())
        self.register(Float64Handler())
        self.register(Float32Handler())

    def register(self, handler: DataTypeHandler) -> None:
        """Register a new data type handler."""
        self._handlers.append(handler)

    def parse_data(self, data_type: bytes, data: bytes | memoryview) -> list[float]:
        """Parse data using appropriate handler.

        Args:
            data_type: Binary data type identifier
            data: Binary data to parse

        Returns:
            List of parsed float values

        Raises:
            NGBDataTypeError: If no handler is found for the data type
        """
        for handler in self._handlers:
            if handler.can_handle(data_type):
                try:
                    return handler.parse_data(data)
                except Exception as e:
                    logger.warning(
                        f"Handler {handler.__class__.__name__} failed to parse data: {e}"
                    )
                    # Continue to next handler if available
                    continue

        # If we get here, no handler could process the data
        raise NGBDataTypeError(f"No handler found for data type: {data_type.hex()}")
