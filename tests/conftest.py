"""
Test configuration and fixtures for pyngb tests.
"""

import tempfile
import zipfile
from pathlib import Path

import pytest

from pyngb.constants import BinaryMarkers, PatternConfig


@pytest.fixture()
def sample_binary_data():
    """Create sample binary data for testing."""
    # Create a simple binary sequence with markers
    markers = BinaryMarkers()
    data = (
        b"header_data"
        + markers.START_DATA
        + b"\x05"
        + b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
        + markers.END_DATA
        + b"footer"
    )
    return data


@pytest.fixture()
def sample_metadata_patterns():
    """Create sample metadata patterns for testing."""
    return {
        "test_field": (b"\x75\x17", b"\x59\x10"),
        "sample_id": (b"\x30\x75", b"\x98\x08"),
    }


@pytest.fixture()
def sample_pattern_config():
    """Create a sample PatternConfig for testing."""
    config = PatternConfig()
    # Override with minimal test patterns
    config.metadata_patterns = {
        "instrument": (b"\x75\x17", b"\x59\x10"),
        "sample_name": (b"\x30\x75", b"\x40\x08"),
    }
    config.column_map = {
        "8d": "time",
        "8e": "sample_temperature",
    }
    return config


@pytest.fixture()
def sample_ngb_file():
    """Create a sample NGB file for integration tests."""
    with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
        with zipfile.ZipFile(temp_file.name, "w") as z:
            # Create minimal stream data
            markers = BinaryMarkers()

            # Stream 1 - metadata
            stream1_data = (
                b"\x75\x17"
                + b"padding"
                + b"\x59\x10"
                + b"more_padding"
                + markers.TYPE_PREFIX
                + b"\x1f"
                + markers.TYPE_SEPARATOR
                + b"\x0c\x00\x00\x00Test Instrument\x00"
                + markers.END_FIELD
            )
            z.writestr("Streams/stream_1.table", stream1_data)

            # Stream 2 - data
            stream2_data = (
                b"\x8d\x17"
                + b"padding"
                + markers.TABLE_SEPARATOR
                + b"\x8d\x75"
                + markers.START_DATA
                + b"\x05"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00"
                + b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
                + markers.END_DATA
            )
            z.writestr("Streams/stream_2.table", stream2_data)

        return temp_file.name


@pytest.fixture()
def sample_metadata():
    """Create sample metadata dictionary."""
    return {
        "instrument": "Test Instrument",
        "sample_name": "Test Sample",
        "sample_mass": 15.5,
        "operator": "Test User",
        "date_performed": "2025-01-01T10:00:00+00:00",
    }


@pytest.fixture()
def cleanup_temp_files():
    """Fixture to clean up temporary files after tests."""
    temp_files = []

    def _add_temp_file(filepath):
        temp_files.append(filepath)
        return filepath

    yield _add_temp_file

    # Cleanup
    for temp_file in temp_files:
        try:
            Path(temp_file).unlink(missing_ok=True)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def cleanup_generated_files():
    """Automatically clean up generated tmp*.parquet files after each test."""
    yield

    # Clean up any tmp*.parquet files generated during the test
    root_dir = Path(__file__).parent.parent  # Project root
    for tmp_file in root_dir.glob("tmp*.parquet"):
        try:
            tmp_file.unlink()
        except Exception:
            pass


@pytest.fixture()
def real_test_files():
    """Provide real test files if available, otherwise skip."""
    test_files_dir = Path(__file__).parent / "test_files"
    if not test_files_dir.exists():
        return []

    real_files = list(test_files_dir.glob("*.ngb-ss3"))
    return real_files
