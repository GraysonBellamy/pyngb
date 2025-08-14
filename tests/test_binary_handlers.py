"""
Unit tests for pyngb binary parsing handlers.
"""

import pytest

from pyngb.binary.handlers import DataTypeRegistry, Float32Handler, Float64Handler
from pyngb.constants import DataType
from pyngb.exceptions import NGBDataTypeError


class TestFloat64Handler:
    """Test Float64Handler class."""

    def test_can_handle_float64(self):
        """Test that Float64Handler recognizes FLOAT64 data type."""
        handler = Float64Handler()
        assert handler.can_handle(DataType.FLOAT64.value)
        assert not handler.can_handle(DataType.FLOAT32.value)
        assert not handler.can_handle(DataType.INT32.value)
        assert not handler.can_handle(b"\x99")

    def test_parse_single_float64(self):
        """Test parsing a single 64-bit float."""
        handler = Float64Handler()

        # 1.0 as 64-bit float in little-endian
        data = b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
        result = handler.parse_data(data)

        assert len(result) == 1
        assert abs(result[0] - 1.0) < 1e-15

    def test_parse_multiple_float64(self):
        """Test parsing multiple 64-bit floats."""
        handler = Float64Handler()

        # Array of [1.0, 2.0, 3.0] as 64-bit floats
        data = (
            b"\x00\x00\x00\x00\x00\x00\xf0\x3f"  # 1.0
            b"\x00\x00\x00\x00\x00\x00\x00\x40"  # 2.0
            b"\x00\x00\x00\x00\x00\x00\x08\x40"  # 3.0
        )
        result = handler.parse_data(data)

        assert len(result) == 3
        assert abs(result[0] - 1.0) < 1e-15
        assert abs(result[1] - 2.0) < 1e-15
        assert abs(result[2] - 3.0) < 1e-15

    def test_parse_empty_data(self):
        """Test parsing empty data."""
        handler = Float64Handler()
        result = handler.parse_data(b"")
        assert result == []


class TestFloat32Handler:
    """Test Float32Handler class."""

    def test_can_handle_float32(self):
        """Test that Float32Handler recognizes FLOAT32 data type."""
        handler = Float32Handler()
        assert handler.can_handle(DataType.FLOAT32.value)
        assert not handler.can_handle(DataType.FLOAT64.value)
        assert not handler.can_handle(DataType.INT32.value)
        assert not handler.can_handle(b"\x99")

    def test_parse_single_float32(self):
        """Test parsing a single 32-bit float."""
        handler = Float32Handler()

        # 1.0 as 32-bit float in little-endian
        data = b"\x00\x00\x80\x3f"
        result = handler.parse_data(data)

        assert len(result) == 1
        assert abs(result[0] - 1.0) < 1e-6

    def test_parse_multiple_float32(self):
        """Test parsing multiple 32-bit floats."""
        handler = Float32Handler()

        # Array of [1.0, 2.0] as 32-bit floats
        data = (
            b"\x00\x00\x80\x3f"  # 1.0
            b"\x00\x00\x00\x40"  # 2.0
        )
        result = handler.parse_data(data)

        assert len(result) == 2
        assert abs(result[0] - 1.0) < 1e-6
        assert abs(result[1] - 2.0) < 1e-6

    def test_parse_empty_data(self):
        """Test parsing empty data."""
        handler = Float32Handler()
        result = handler.parse_data(b"")
        assert result == []


class TestDataTypeRegistry:
    """Test DataTypeRegistry class."""

    def test_default_handlers_registered(self):
        """Test that default handlers are registered."""
        registry = DataTypeRegistry()

        # Should handle float64 and float32
        data_f64 = b"\x00\x00\x00\x00\x00\x00\xf0\x3f"  # 1.0 as float64
        result = registry.parse_data(DataType.FLOAT64.value, data_f64)
        assert len(result) == 1
        assert abs(result[0] - 1.0) < 1e-15

        data_f32 = b"\x00\x00\x80\x3f"  # 1.0 as float32
        result = registry.parse_data(DataType.FLOAT32.value, data_f32)
        assert len(result) == 1
        assert abs(result[0] - 1.0) < 1e-6

    def test_register_custom_handler(self):
        """Test registering a custom handler."""
        registry = DataTypeRegistry()

        class CustomHandler:
            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> list:
                return [42.0]  # Always return 42.0

        custom_handler = CustomHandler()
        registry.register(custom_handler)

        # Should use custom handler
        result = registry.parse_data(b"\x99", b"any_data")
        assert result == [42.0]

    def test_unknown_data_type_error(self):
        """Test that unknown data type raises NGBDataTypeError."""
        registry = DataTypeRegistry()

        with pytest.raises(NGBDataTypeError) as exc_info:
            registry.parse_data(b"\x99", b"some_data")

        assert "No handler found for data type: 99" in str(exc_info.value)

    def test_handler_precedence(self):
        """Test that handlers are checked in registration order."""
        registry = DataTypeRegistry()

        class FirstHandler:
            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> list:
                return [1.0]

        class SecondHandler:
            def can_handle(self, data_type: bytes) -> bool:
                return data_type == b"\x99"

            def parse_data(self, data: bytes) -> list:
                return [2.0]

        # Register in order
        registry.register(FirstHandler())
        registry.register(SecondHandler())

        # Should use first handler (registered first)
        result = registry.parse_data(b"\x99", b"data")
        assert result == [1.0]

    def test_empty_registry(self):
        """Test registry with no handlers."""
        # Create registry without default handlers
        registry = DataTypeRegistry()
        registry._handlers.clear()  # Remove default handlers

        with pytest.raises(NGBDataTypeError):
            registry.parse_data(DataType.FLOAT64.value, b"data")


class TestDataTypeHandlerProtocol:
    """Test DataTypeHandler protocol compliance."""

    def test_float64_handler_implements_protocol(self):
        """Test that Float64Handler implements DataTypeHandler protocol."""
        handler = Float64Handler()

        # Should have required methods
        assert hasattr(handler, "can_handle")
        assert hasattr(handler, "parse_data")
        assert callable(handler.can_handle)
        assert callable(handler.parse_data)

        # Methods should work correctly
        assert isinstance(handler.can_handle(b"\x05"), bool)
        assert isinstance(handler.parse_data(b""), list)

    def test_float32_handler_implements_protocol(self):
        """Test that Float32Handler implements DataTypeHandler protocol."""
        handler = Float32Handler()

        # Should have required methods
        assert hasattr(handler, "can_handle")
        assert hasattr(handler, "parse_data")
        assert callable(handler.can_handle)
        assert callable(handler.parse_data)

        # Methods should work correctly
        assert isinstance(handler.can_handle(b"\x04"), bool)
        assert isinstance(handler.parse_data(b""), list)
