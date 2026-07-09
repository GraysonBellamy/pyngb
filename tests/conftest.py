"""
Test configuration and fixtures for pyngb tests.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from support.ngb_builder import minimal_ngb


@pytest.fixture()
def sample_ngb_file(tmp_path: Path) -> str:
    """Create a sample NGB file for integration tests.

    A strict-grammar synthetic archive (streams 1 and 2, every dtype, a
    time channel with two segments) that parses through the full public
    API. See :func:`support.ngb_builder.minimal_ngb`.
    """
    return str(minimal_ngb(tmp_path / "sample.ngb-ss3"))


@pytest.fixture()
def sample_metadata() -> dict[str, Any]:
    """Create sample metadata dictionary."""
    return {
        "instrument": "Test Instrument",
        "sample_name": "Test Sample",
        "sample_mass": 15.5,
        "operator": "Test User",
        "date_performed": "2025-01-01T10:00:00+00:00",
    }


@pytest.fixture()
def cleanup_temp_files() -> Iterator[Any]:
    """Fixture to clean up temporary files after tests."""
    temp_files: list[str] = []

    def _add_temp_file(filepath: str) -> str:
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
def cleanup_generated_files() -> Iterator[None]:
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
def real_test_files() -> list[Path]:
    """Provide real test files, skipping the test when none are available."""
    test_files_dir = Path(__file__).parent / "test_files"
    real_files = (
        list(test_files_dir.glob("*.ngb-ss3")) if test_files_dir.exists() else []
    )
    if not real_files:
        pytest.skip("No real test files available")
    return real_files
