"""
Simple test runner for pynetzsch tests.
Run this file to execute all tests without pytest installation.
"""

import sys
import traceback
from pathlib import Path

# Add src to path so we can import pynetzsch
sys.path.insert(0, str(Path(__file__).parent / "src"))


def run_basic_tests():
    """Run basic tests without pytest."""
    passed = 0
    failed = 0

    print("🧪 Running PyNetzsch Basic Tests")
    print("=" * 50)

    tests = [
        test_imports,
        test_exceptions,
        test_constants,
        test_binary_handlers,
        test_binary_parser_basic,
    ]

    for test_func in tests:
        try:
            print(f"Running {test_func.__name__}...", end=" ")
            test_func()
            print("✅ PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {e}")
            traceback.print_exc()
            failed += 1

    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)

    return failed == 0


def test_imports():
    """Test that all modules can be imported."""
    # Test main API
    from pynetzsch import load_ngb_data, get_sta_data, NGBParser

    # Test submodules
    from pynetzsch.binary import BinaryParser, DataTypeRegistry
    from pynetzsch.extractors import MetadataExtractor, DataStreamProcessor
    from pynetzsch.constants import DataType, PatternConfig
    from pynetzsch.exceptions import NGBParseError

    assert callable(load_ngb_data)
    assert callable(NGBParser)


def test_exceptions():
    """Test exception classes."""
    from pynetzsch.exceptions import (
        NGBParseError,
        NGBCorruptedFileError,
        NGBDataTypeError,
    )

    # Test inheritance
    error = NGBCorruptedFileError("test")
    assert isinstance(error, NGBParseError)
    assert isinstance(error, Exception)
    assert str(error) == "test"


def test_constants():
    """Test constants and configurations."""
    from pynetzsch.constants import DataType, PatternConfig, BinaryMarkers

    # Test DataType enum
    assert DataType.FLOAT64.value == b"\x05"
    assert DataType.INT32.value == b"\x03"

    # Test PatternConfig
    config = PatternConfig()
    assert "8d" in config.column_map
    assert config.column_map["8d"] == "time"

    # Test BinaryMarkers
    markers = BinaryMarkers()
    assert markers.START_DATA == b"\xa0\x01"

    # Test that markers are immutable
    try:
        markers.START_DATA = b"changed"
        assert False, "Should not be able to modify markers"
    except AttributeError:
        pass  # Expected


def test_binary_handlers():
    """Test binary data handlers."""
    from pynetzsch.binary.handlers import Float64Handler, DataTypeRegistry
    from pynetzsch.constants import DataType
    import struct

    # Test Float64Handler
    handler = Float64Handler()
    assert handler.can_handle(DataType.FLOAT64.value)

    # Test parsing 1.0 as float64
    data = struct.pack("<d", 1.0)
    result = handler.parse_data(data)
    assert len(result) == 1
    assert abs(result[0] - 1.0) < 1e-15

    # Test DataTypeRegistry
    registry = DataTypeRegistry()
    result = registry.parse_data(DataType.FLOAT64.value, data)
    assert len(result) == 1
    assert abs(result[0] - 1.0) < 1e-15


def test_binary_parser_basic():
    """Test basic binary parser functionality."""
    from pynetzsch.binary.parser import BinaryParser
    from pynetzsch.constants import DataType
    import struct

    parser = BinaryParser()

    # Test parse_value
    result = parser.parse_value(DataType.INT32.value, struct.pack("<i", 42))
    assert result == 42

    result = parser.parse_value(DataType.FLOAT32.value, struct.pack("<f", 1.5))
    assert abs(result - 1.5) < 1e-6

    # Test string parsing
    string_data = b"\x05\x00\x00\x00Hello"
    result = parser.parse_value(DataType.STRING.value, string_data)
    assert result == "Hello"

    # Test split_tables with no separator
    data = b"single_table"
    result = parser.split_tables(data)
    assert len(result) == 1
    assert result[0] == data


if __name__ == "__main__":
    success = run_basic_tests()
    sys.exit(0 if success else 1)
