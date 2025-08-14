"""
Comprehensive test runner for pyngb tests.
Run this file to execute all tests without pytest installation.
Includes both core functionality and new features testing.
"""

import sys
import traceback
from pathlib import Path

import polars as pl

# Add src to path so we can import pyngb
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def run_basic_tests():
    """Run comprehensive tests without pytest."""
    passed = 0
    failed = 0

    print("🧪 Running pyngb Comprehensive Tests")
    print("=" * 50)

    core_tests = [
        test_imports,
        test_exceptions,
        test_constants,
        test_binary_handlers,
        test_binary_parser_basic,
    ]

    new_feature_tests = [
        test_validation_features,
        test_batch_processing_features,
        test_integration,
    ]

    print("📋 Core functionality tests:")
    for test_func in core_tests:
        try:
            print(f"  Running {test_func.__name__}...", end=" ")
            test_func()
            print("✅ PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {e}")
            traceback.print_exc()
            failed += 1

    print("\n🚀 New features tests:")
    for test_func in new_feature_tests:
        try:
            print(f"  Running {test_func.__name__}...", end=" ")
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
    from pyngb import NGBParser, read_ngb

    # Test submodules

    assert callable(read_ngb)
    assert callable(NGBParser)


def test_exceptions():
    """Test exception classes."""
    from pyngb.exceptions import NGBCorruptedFileError, NGBParseError

    # Test inheritance
    error = NGBCorruptedFileError("test")
    assert isinstance(error, NGBParseError)
    assert isinstance(error, Exception)
    assert str(error) == "test"


def test_constants():
    """Test constants and configurations."""
    from pyngb.constants import BinaryMarkers, DataType, PatternConfig

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
    import struct

    from pyngb.binary.handlers import DataTypeRegistry, Float64Handler
    from pyngb.constants import DataType

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
    import struct

    from pyngb.binary.parser import BinaryParser
    from pyngb.constants import DataType

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


def test_validation_features():
    """Test validation features."""
    # Import validation modules
    from pyngb.validation import QualityChecker, ValidationResult, validate_sta_data

    # Create sample data with correct column names
    sample_data = pl.DataFrame(
        {
            "time": [0.0, 100.0, 200.0, 300.0],
            "temperature": [25.0, 100.0, 150.0, 200.0],
            "dta_uv": [10.0, 20.0, 30.0, 40.0],
            "sample_mass_mg": [10.5, 10.2, 9.8, 9.5],
        }
    )

    # Test quick validation function
    issues = validate_sta_data(sample_data)
    assert isinstance(issues, list)

    # Test comprehensive quality checker
    checker = QualityChecker(sample_data)
    result = checker.full_validation()
    assert isinstance(result, ValidationResult)
    assert result.is_valid

    # Test ValidationResult functionality
    test_result = ValidationResult()
    test_result.add_error("Test error")
    test_result.add_warning("Test warning")
    assert not test_result.is_valid
    assert test_result.has_warnings


def test_batch_processing_features():
    """Test batch processing features."""
    from pyngb.batch import BatchProcessor, NGBDataset

    # Test BatchProcessor initialization
    processor = BatchProcessor(max_workers=2, verbose=False)
    assert processor.max_workers == 2
    assert not processor.verbose

    # Test empty file list processing
    results = processor.process_files([])
    assert len(results) == 0

    # Test NGBDataset functionality
    dataset = NGBDataset([])
    assert len(dataset) == 0


def test_integration():
    """Test integration between modules."""
    # Test that all imports work
    from pyngb.batch import BatchProcessor
    from pyngb.validation import validate_sta_data

    # Create test data with correct column names
    test_data = pl.DataFrame(
        {
            "time": [0.0, 100.0],
            "temperature": [25.0, 100.0],
            "dta_uv": [10.0, 20.0],
        }
    )

    # Test validation finds no issues with good data
    issues = validate_sta_data(test_data)
    assert len(issues) == 0

    # Test BatchProcessor can be created
    processor = BatchProcessor(max_workers=1)
    assert processor is not None


if __name__ == "__main__":
    success = run_basic_tests()
    sys.exit(0 if success else 1)
