"""
Unit tests for pyngb binary parsing handlers.
"""

import struct

import numpy as np
import pytest

from pyngb.binary.handlers import (
    DataTypeRegistry,
    Float32Handler,
    Float64Handler,
    Int32Handler,
)
from pyngb.constants import DataType
from pyngb.exceptions import NGBDataTypeError


def _assert_f64_array(result: np.ndarray, expected: list[float]) -> None:
    """Every handler returns a float64 array with exactly these values."""
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float64
    np.testing.assert_array_equal(result, np.array(expected, dtype=np.float64))


class TestFloat64Handler:
    """Test Float64Handler class."""

    def test_can_handle_float64(self) -> None:
        handler = Float64Handler()
        assert handler.can_handle(DataType.FLOAT64.value)
        assert not handler.can_handle(DataType.FLOAT32.value)
        assert not handler.can_handle(DataType.INT32.value)
        assert not handler.can_handle(b"\x99")

    def test_parse_single_float64(self) -> None:
        handler = Float64Handler()
        _assert_f64_array(handler.parse_data(struct.pack("<d", 1.0)), [1.0])

    def test_parse_multiple_float64(self) -> None:
        handler = Float64Handler()
        data = struct.pack("<3d", 1.0, -2.5, 3.75)
        _assert_f64_array(handler.parse_data(data), [1.0, -2.5, 3.75])

    def test_parse_empty_data(self) -> None:
        handler = Float64Handler()
        _assert_f64_array(handler.parse_data(b""), [])

    def test_parse_misaligned_data_raises(self) -> None:
        handler = Float64Handler()
        with pytest.raises(ValueError, match="multiple of element size"):
            handler.parse_data(b"\x00\x00\x00")  # not a multiple of 8


class TestFloat32Handler:
    """Test Float32Handler class."""

    def test_can_handle_float32(self) -> None:
        handler = Float32Handler()
        assert handler.can_handle(DataType.FLOAT32.value)
        assert not handler.can_handle(DataType.FLOAT64.value)
        assert not handler.can_handle(DataType.INT32.value)
        assert not handler.can_handle(b"\x99")

    def test_parse_single_float32(self) -> None:
        handler = Float32Handler()
        _assert_f64_array(handler.parse_data(struct.pack("<f", 1.0)), [1.0])

    def test_parse_multiple_float32(self) -> None:
        handler = Float32Handler()
        data = struct.pack("<2f", 1.0, -2.5)
        _assert_f64_array(handler.parse_data(data), [1.0, -2.5])

    def test_widening_preserves_float32_value_exactly(self) -> None:
        """The f64 result equals the f32 value bit-exactly, not the decimal it rounds."""
        handler = Float32Handler()
        result = handler.parse_data(struct.pack("<f", 0.1))
        _assert_f64_array(result, [np.float32(0.1)])
        assert result[0] != 0.1  # 0.1f32 widened, not 0.1f64

    def test_parse_empty_data(self) -> None:
        handler = Float32Handler()
        _assert_f64_array(handler.parse_data(b""), [])


class TestInt32Handler:
    """Test Int32Handler class."""

    def test_can_handle_int32(self) -> None:
        handler = Int32Handler()
        assert handler.can_handle(DataType.INT32.value)
        assert not handler.can_handle(DataType.FLOAT64.value)
        assert not handler.can_handle(b"\x99")

    def test_parse_int32_values(self) -> None:
        handler = Int32Handler()
        data = struct.pack("<3i", 42, -7, 2**31 - 1)
        _assert_f64_array(handler.parse_data(data), [42.0, -7.0, 2147483647.0])

    def test_parse_empty_data(self) -> None:
        handler = Int32Handler()
        _assert_f64_array(handler.parse_data(b""), [])


class TestDataTypeRegistry:
    """Test DataTypeRegistry class."""

    def test_default_handlers_registered(self) -> None:
        registry = DataTypeRegistry()

        _assert_f64_array(
            registry.parse_data(DataType.FLOAT64.value, struct.pack("<d", 1.0)), [1.0]
        )
        _assert_f64_array(
            registry.parse_data(DataType.FLOAT32.value, struct.pack("<f", 1.0)), [1.0]
        )
        _assert_f64_array(
            registry.parse_data(DataType.INT32.value, struct.pack("<i", 42)), [42.0]
        )

    def test_itemsize(self) -> None:
        registry = DataTypeRegistry()
        assert registry.itemsize(DataType.FLOAT64.value) == 8
        assert registry.itemsize(DataType.FLOAT32.value) == 4
        assert registry.itemsize(DataType.INT32.value) == 4
        assert registry.itemsize(b"\x99") is None

    def test_register_custom_handler(self) -> None:
        registry = DataTypeRegistry()

        class CustomHandler:
            itemsize = 1

            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> np.ndarray:
                return np.array([42.0])

        registry.register(CustomHandler())  # type: ignore[arg-type]

        _assert_f64_array(registry.parse_data(b"\x99", b"any_data"), [42.0])

    def test_unknown_data_type_error(self) -> None:
        registry = DataTypeRegistry()

        with pytest.raises(NGBDataTypeError) as exc_info:
            registry.parse_data(b"\x99", b"some_data")

        assert "No handler found for data type: 99" in str(exc_info.value)

    def test_handler_precedence(self) -> None:
        """Handlers are checked in registration order."""
        registry = DataTypeRegistry()

        class FirstHandler:
            itemsize = 1

            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> np.ndarray:
                return np.array([1.0])

        class SecondHandler:
            itemsize = 1

            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> np.ndarray:
                return np.array([2.0])

        registry.register(FirstHandler())  # type: ignore[arg-type]
        registry.register(SecondHandler())  # type: ignore[arg-type]

        _assert_f64_array(registry.parse_data(b"\x99", b"data"), [1.0])

    def test_empty_registry(self) -> None:
        registry = DataTypeRegistry()
        registry._handlers.clear()

        with pytest.raises(NGBDataTypeError):
            registry.parse_data(DataType.FLOAT64.value, b"data")
