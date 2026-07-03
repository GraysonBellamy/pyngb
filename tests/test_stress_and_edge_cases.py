"""
Stress tests and edge case integration tests for pyngb package.

These tests push the package to its limits and test unusual scenarios
to ensure robustness for production use.
"""

import gc
import tempfile
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pyngb import BatchProcessor, read_ngb
from pyngb.exceptions import NGBStreamNotFoundError
from pyngb.validation import QualityChecker


@pytest.mark.integration
class TestStressConditions:
    """Test package behavior under stress conditions."""

    @pytest.fixture
    def real_test_files(self) -> Any:
        """Get available real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    @pytest.mark.slow
    def test_concurrent_file_access(self, real_test_files: Any) -> None:
        """Test concurrent access to the same files."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]

        def parse_file() -> int:
            return read_ngb(str(test_file)).num_rows

        # Concurrent access to the same file must succeed on every thread and
        # every thread must see the same data.
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(parse_file) for _ in range(10)]
            row_counts = [future.result() for future in futures]

        assert len(row_counts) == 10
        assert len(set(row_counts)) == 1, (
            f"Inconsistent results in concurrent access: {set(row_counts)}"
        )
        assert row_counts[0] > 0

    @pytest.mark.slow
    def test_memory_stress_repeated_parsing(self, real_test_files: Any) -> None:
        """Test memory usage with repeated parsing."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]

        import os

        import psutil

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Parse the same real file many times; every parse must succeed.
        for i in range(20):
            table = read_ngb(str(test_file))
            assert table.num_rows > 0
            del table

            # Force garbage collection every 5 iterations
            if i % 5 == 0:
                gc.collect()

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 200MB)
        memory_mb = memory_increase / 1024 / 1024
        assert memory_mb < 200, f"Memory increased by {memory_mb:.1f} MB (too much)"

    def test_large_batch_processing(self, real_test_files: Any) -> None:
        """Test processing a large number of files."""
        if not real_test_files:
            pytest.skip("No test files available")

        # Create a large list by repeating available files
        extended_file_list = real_test_files * 10  # 10x the files

        with tempfile.TemporaryDirectory() as temp_dir:
            processor = BatchProcessor(max_workers=2, verbose=False)

            results = processor.process_files(
                [str(f) for f in extended_file_list],
                output_dir=temp_dir,
                skip_errors=True,
            )

            # Every entry is a real fixture, so every one must succeed.
            assert len(results) == len(extended_file_list)
            assert all(r["status"] == "success" for r in results)

    def test_rapid_successive_operations(self, real_test_files: Any) -> None:
        """Test rapid successive operations on the same data."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]

        # Rapid successive operations on a real file must all succeed; the
        # fixtures parse reliably, so anything less is a regression.
        from pyngb.core.parser import NGBParser

        for i in range(30):
            if i % 3 == 0:
                table = read_ngb(str(test_file))
                assert table.num_rows > 0
            elif i % 3 == 1:
                metadata, data = read_ngb(str(test_file), return_metadata=True)
                assert metadata
                assert data.num_rows > 0
            else:
                parser = NGBParser()
                metadata, table = parser.parse(str(test_file))
                assert metadata
                assert table.num_rows > 0


class TestEdgeCaseFiles:
    """Test with edge case file scenarios."""

    def create_edge_case_file(self, scenario: str) -> Any:
        """Create files with specific edge case scenarios."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            if scenario == "minimal_zip":
                # Create a ZIP file that's missing required NGB structure
                with zipfile.ZipFile(temp_file.name, "w") as z:
                    z.writestr("random_file.txt", b"minimal")

            elif scenario == "large_metadata":
                # Create a ZIP file with invalid structure
                with zipfile.ZipFile(temp_file.name, "w") as z:
                    # Create large metadata in wrong location
                    large_data = b"x" * 10000  # 10KB of data
                    z.writestr("wrong_path/stream_1.table", large_data)

            elif scenario == "many_streams":
                # Create a ZIP file with many files but wrong structure
                with zipfile.ZipFile(temp_file.name, "w") as z:
                    # Create many stream files in wrong location
                    for i in range(20):
                        z.writestr(
                            f"wrong_folder/stream_{i}.table", f"data_{i}".encode()
                        )

            elif scenario == "empty_streams":
                # Create a ZIP file with empty streams in wrong location
                with zipfile.ZipFile(temp_file.name, "w") as z:
                    z.writestr("wrong_path/stream_1.table", b"")
                    z.writestr("wrong_path/stream_2.table", b"")

            elif scenario == "corrupted_zip":
                # Write partial ZIP header
                temp_file.write(b"PK\x03\x04\x14\x00")

            name = temp_file.name

        return name

    def test_minimal_file_handling(self) -> None:
        """Test handling of minimal valid files."""
        test_file = self.create_edge_case_file("minimal_zip")

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_large_metadata_handling(self) -> None:
        """Test handling of files with very large metadata."""
        test_file = self.create_edge_case_file("large_metadata")

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_many_streams_handling(self) -> None:
        """Test handling of files with many stream files."""
        test_file = self.create_edge_case_file("many_streams")

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_empty_streams_handling(self) -> None:
        """Test handling of files with empty streams."""
        test_file = self.create_edge_case_file("empty_streams")

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_corrupted_file_handling(self) -> None:
        """Test handling of corrupted files."""
        test_file = self.create_edge_case_file("corrupted_zip")

        try:
            with pytest.raises(zipfile.BadZipFile):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)


class TestExtremeDataScenarios:
    """Test with extreme data scenarios."""

    def test_extreme_validation_scenarios(self) -> None:
        """Test validation with extreme data scenarios."""

        # Scenario 1: All NaN data
        nan_data = pl.DataFrame(
            {
                "time": [float("nan")] * 100,
                "sample_temperature": [float("nan")] * 100,
                "mass": [float("nan")] * 100,
            }
        )

        # Should handle gracefully
        checker = QualityChecker(nan_data)
        result = checker.full_validation()
        assert not result.is_valid

        # Scenario 2: Infinite values
        inf_data = pl.DataFrame(
            {
                "time": [float("inf"), -float("inf")] * 50,
                "sample_temperature": [float("inf")] * 100,
                "mass": [1.0] * 100,
            }
        )

        checker = QualityChecker(inf_data)
        result = checker.full_validation()
        assert not result.is_valid

        # Scenario 3: Extremely large values
        large_data = pl.DataFrame(
            {
                "time": list(range(100)),
                "sample_temperature": [1e10] * 100,  # 10 billion degrees
                "mass": [1e-20] * 100,  # Extremely small mass
            }
        )

        checker = QualityChecker(large_data)
        issues = checker.quick_check()
        # May or may not be valid depending on validation rules
        assert isinstance(issues, list)

        print("✓ Extreme data validation scenarios completed")

    def test_edge_case_batch_scenarios(self) -> None:
        """Test batch processing with edge case scenarios."""

        # Create various problematic files
        problematic_files = []

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # File 1: Empty file
            empty_file = temp_path / "empty.ngb-ss3"
            empty_file.touch()
            problematic_files.append(str(empty_file))

            # File 2: Text file with wrong extension
            text_file = temp_path / "text.ngb-ss3"
            text_file.write_text("This is not an NGB file")
            problematic_files.append(str(text_file))

            # File 3: Valid ZIP but wrong structure
            wrong_zip = temp_path / "wrong.ngb-ss3"
            with zipfile.ZipFile(wrong_zip, "w") as z:
                z.writestr("not_streams/data.txt", "wrong structure")
            problematic_files.append(str(wrong_zip))

            # Test batch processing
            processor = BatchProcessor(max_workers=1, verbose=False)
            results = processor.process_files(
                problematic_files,  # type: ignore[arg-type]
                skip_errors=True,
                output_dir=temp_path,
            )

            # Should handle all problematic files gracefully
            assert len(results) == len(problematic_files)

            # All should fail, but processing should complete
            failed_count = sum(1 for r in results if r["status"] == "error")
            assert failed_count == len(problematic_files), (
                "All problematic files should fail"
            )

            print(
                f"✓ Edge case batch processing: {failed_count} files failed as expected"
            )


@pytest.mark.integration
class TestConcurrencyEdgeCases:
    """Test edge cases related to concurrency."""

    @pytest.fixture
    def real_test_files(self) -> Any:
        """Get available real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    def test_thread_safety_batch_processing(self, real_test_files: Any) -> None:
        """Test thread safety of batch processing components."""
        if not real_test_files:
            pytest.skip("No test files available")

        results = []
        errors = []

        def batch_process() -> Any:
            try:
                processor = BatchProcessor(max_workers=1, verbose=False)
                with tempfile.TemporaryDirectory() as temp_dir:
                    result = processor.process_files(
                        [str(real_test_files[0])], output_dir=temp_dir, skip_errors=True
                    )
                    results.append(len(result))
                    return True
            except Exception as e:
                errors.append(str(e))
                return False

        # Run batch processing from multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=batch_process)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Every concurrent batch run must succeed and process its one file.
        assert len(results) == 5, f"Batch processes failed. Errors: {errors[:3]}"
        assert all(count == 1 for count in results)

    def test_parser_state_isolation(self, real_test_files: Any) -> None:
        """Test that parser instances don't share state inappropriately."""
        if not real_test_files:
            pytest.skip("No test files available")

        from pyngb.core.parser import NGBParser

        # Create multiple parser instances
        parsers = [NGBParser() for _ in range(3)]

        # Test that they have independent state
        for i, parser in enumerate(parsers):
            # Modify configuration
            parser.config.column_map[f"test_{i}"] = f"test_column_{i}"

        # Verify configurations are independent
        for i, parser in enumerate(parsers):
            assert f"test_{i}" in parser.config.column_map
            assert parser.config.column_map[f"test_{i}"] == f"test_column_{i}"

            # Other parsers shouldn't have this key
            for j in range(3):
                if i != j:
                    assert (
                        f"test_{j}" not in parser.config.column_map
                        or parser.config.column_map[f"test_{j}"] != f"test_column_{j}"
                    )

        print("✓ Parser state isolation verified")

    def test_concurrent_validation(self) -> None:
        """Test concurrent validation operations."""

        # Create test data
        test_data = pl.DataFrame(
            {
                "time": list(range(1000)),
                "sample_temperature": [25 + i * 0.1 for i in range(1000)],
                "mass": [10 - i * 0.001 for i in range(1000)],
            }
        )

        validation_results = []

        def validate_data() -> Any:
            try:
                checker = QualityChecker(test_data)
                result = checker.full_validation()
                validation_results.append(result.is_valid)
                return True
            except Exception:
                return False

        # Run concurrent validations
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(validate_data) for _ in range(10)]
            completed = [future.result() for future in futures]

        # Every concurrent validation must succeed and agree.
        assert sum(completed) == 10, "Concurrent validations failed"
        assert len(set(validation_results)) == 1, (
            f"Inconsistent validation results: {set(validation_results)}"
        )


class TestBoundaryConditions:
    """Test boundary conditions and limits."""

    def test_empty_data_handling(self) -> None:
        """Test handling of completely empty data."""

        # Empty DataFrame
        empty_df = pl.DataFrame({})

        # Should handle gracefully
        from pyngb.validation import validate_sta_data

        issues = validate_sta_data(empty_df)
        assert len(issues) > 0  # Should detect that data is empty

        # Empty DataFrame with expected columns but no rows
        empty_with_cols = pl.DataFrame(
            {
                "time": [],
                "sample_temperature": [],
                "mass": [],
            }
        )

        issues = validate_sta_data(empty_with_cols)
        assert len(issues) > 0  # Should detect empty data

        print("✓ Empty data handling verified")

    def test_single_point_data(self) -> None:
        """Test handling of single data point scenarios."""

        single_point = pl.DataFrame(
            {
                "time": [1.0],
                "sample_temperature": [25.0],
                "mass": [10.0],
            }
        )

        # A single-row frame must be reported as an issue, not crash the
        # checker: one point cannot show heating or cooling.
        checker = QualityChecker(single_point)
        issues = checker.quick_check()

        assert issues == ["Temperature is constant (no heating/cooling)"]

    def test_extreme_file_sizes(self) -> None:
        """Test handling of extremely small and large file scenarios."""

        # Test with extremely small ZIP
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                z.writestr("tiny.txt", b"x")  # Single byte

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

        print("✓ Extreme file size handling verified")

    def test_unicode_edge_cases(self) -> None:
        """Test handling of various Unicode scenarios."""

        # Test with Unicode in mock metadata
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                # Include Unicode content in wrong location
                unicode_content = "测试样品 🧪 test sample".encode()
                z.writestr("wrong_path/unicode_stream.table", unicode_content)

        try:
            with pytest.raises(NGBStreamNotFoundError):
                read_ngb(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

        print("✓ Unicode edge case handling verified")
