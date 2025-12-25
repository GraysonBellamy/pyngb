"""Performance regression tests.

These tests ensure that performance doesn't regress over time.
Uses pytest-benchmark for accurate performance measurements.
"""

from __future__ import annotations
from typing import Any

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pyngb import read_ngb
from pyngb.analysis import dtg
from pyngb.batch import BatchProcessor, process_files
from pyngb.validation import QualityChecker


@pytest.mark.benchmark
class TestParsingPerformance:
    """Performance tests for file parsing."""

    def test_read_ngb_performance(self, benchmark: Any, real_ngb_file: Any) -> None:
        """Reading an NGB file should complete within acceptable time.

        Baseline: < 1 second for typical files (~500KB)
        """
        result = benchmark(read_ngb, str(real_ngb_file))

        # Verify result is valid
        assert result is not None
        assert result.num_rows > 0

        # Performance assertion - should complete in under 2 seconds
        assert benchmark.stats["mean"] < 2.0, "Parsing took too long"

    def test_read_ngb_with_metadata_performance(self, benchmark: Any, real_ngb_file: Any) -> None:
        """Reading NGB with metadata extraction should be fast.

        Baseline: < 1.5 seconds for typical files
        """
        result = benchmark(read_ngb, str(real_ngb_file), return_metadata=True)

        # Verify result is valid
        table, metadata = result
        assert table.num_rows > 0
        assert isinstance(metadata, dict)

        # Performance assertion
        assert benchmark.stats["mean"] < 2.5, "Parsing with metadata took too long"


@pytest.mark.benchmark
class TestAnalysisPerformance:
    """Performance tests for analysis operations."""

    def test_dtg_calculation_performance(self, benchmark: Any) -> None:
        """DTG calculation should be fast for typical datasets.

        Baseline: < 0.1 seconds for 10,000 points
        """
        n_points = 10000
        time_arr = np.linspace(0, 1000, n_points)
        mass_arr = 100 - 0.5 * time_arr + np.random.normal(0, 0.1, n_points)

        result = benchmark(dtg, time_arr, mass_arr, smooth="medium")

        assert len(result) == n_points
        assert benchmark.stats["mean"] < 0.2, "DTG calculation took too long"

    def test_dtg_large_dataset_performance(self, benchmark: Any) -> None:
        """DTG should handle large datasets efficiently.

        Baseline: < 0.5 seconds for 100,000 points
        """
        n_points = 100000
        time_arr = np.linspace(0, 10000, n_points)
        mass_arr = 100 - 0.5 * time_arr + np.random.normal(0, 0.01, n_points)

        result = benchmark(dtg, time_arr, mass_arr, smooth="loose")

        assert len(result) == n_points
        assert benchmark.stats["mean"] < 1.0, "Large DTG calculation took too long"


@pytest.mark.benchmark
class TestValidationPerformance:
    """Performance tests for validation operations."""

    def test_quick_check_performance(self, benchmark: Any, sample_sta_data: Any) -> None:
        """Quick validation should be very fast.

        Baseline: < 0.01 seconds for typical datasets
        """
        checker = QualityChecker(sample_sta_data)

        result = benchmark(checker.quick_check)

        assert isinstance(result, list)
        assert benchmark.stats["mean"] < 0.05, "Quick check took too long"

    def test_full_validation_performance(self, benchmark: Any, sample_sta_data: Any) -> None:
        """Full validation should complete in reasonable time.

        Baseline: < 0.1 seconds for typical datasets
        """
        checker = QualityChecker(sample_sta_data)

        result = benchmark(checker.full_validation)

        assert result is not None
        assert benchmark.stats["mean"] < 0.2, "Full validation took too long"


@pytest.mark.benchmark
class TestBatchProcessingPerformance:
    """Performance tests for batch processing."""

    def test_batch_processor_throughput(self, benchmark: Any, sample_ngb_file: Any) -> None:
        """Batch processing should maintain good throughput.

        Baseline: > 5 files/second for small files
        """
        # Use the same file multiple times (simulating batch processing)
        n_files = 10
        files = [sample_ngb_file] * n_files

        processor = BatchProcessor(max_workers=2)

        def process_batch() -> Any:
            results = []
            for result in processor.process_files(files):
                results.append(result)
            return results

        results = benchmark(process_batch)

        assert len(results) == n_files
        # Calculate throughput
        files_per_second = n_files / benchmark.stats["mean"]
        assert files_per_second > 2, (
            f"Throughput too low: {files_per_second:.2f} files/s"
        )

    def test_process_files_function_performance(self, benchmark: Any, sample_ngb_file: Any) -> None:
        """process_files convenience function should be efficient.

        Baseline: Similar to BatchProcessor performance
        """
        n_files = 5
        files = [sample_ngb_file] * n_files

        def process() -> Any:
            results = list(process_files(files, max_workers=2))
            return results

        results = benchmark(process)

        assert len(results) == n_files
        assert benchmark.stats["mean"] < 5.0, "process_files took too long"


@pytest.mark.benchmark
class TestDataFrameOperationsPerformance:
    """Performance tests for DataFrame operations."""

    def test_large_dataframe_creation_performance(self, benchmark: Any) -> None:
        """Creating large DataFrames should be efficient."""
        n_rows = 100000
        n_cols = 10

        def create_dataframe() -> Any:
            data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
            return pl.DataFrame(data)

        df = benchmark(create_dataframe)

        assert df.shape == (n_rows, n_cols)
        assert benchmark.stats["mean"] < 0.5, "DataFrame creation took too long"

    def test_dataframe_filtering_performance(self, benchmark: Any) -> None:
        """Filtering large DataFrames should be fast."""
        n_rows = 100000
        df = pl.DataFrame(
            {
                "time": np.linspace(0, 1000, n_rows),
                "temperature": np.random.randn(n_rows) * 10 + 500,
                "mass": np.random.randn(n_rows) * 5 + 100,
            }
        )

        def filter_dataframe() -> Any:
            return df.filter(
                (pl.col("temperature") > 400) & (pl.col("temperature") < 600)
            )

        result = benchmark(filter_dataframe)

        assert result.shape[0] > 0
        assert benchmark.stats["mean"] < 0.1, "DataFrame filtering took too long"


# Fixtures for performance tests
@pytest.fixture
def sample_sta_data() -> Any:
    """Create sample STA data for validation tests."""
    n_points = 1000
    return pl.DataFrame(
        {
            "time": np.linspace(0, 1000, n_points),
            "sample_temperature": np.linspace(25, 1000, n_points),
            "mass": 100 - np.linspace(0, 20, n_points),
            "dsc_signal": np.sin(np.linspace(0, 10, n_points)) * 10,
        }
    )


@pytest.fixture
def real_ngb_file() -> Any:
    """Get a real NGB file for performance testing."""
    test_files_dir = Path(__file__).parent.parent / "test_files"
    ngb_files = list(test_files_dir.glob("*.ngb-ss3"))

    if not ngb_files:
        pytest.skip("No real NGB files available for performance testing")

    # Use the first available file
    return ngb_files[0]
