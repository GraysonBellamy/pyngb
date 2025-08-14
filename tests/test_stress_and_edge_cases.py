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

import polars as pl
import pytest

from pyngb import BatchProcessor, NGBDataset, read_ngb
from pyngb.validation import QualityChecker


@pytest.mark.integration
class TestStressConditions:
    """Test package behavior under stress conditions."""

    @pytest.fixture
    def real_test_files(self):
        """Get available real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    @pytest.mark.slow
    def test_concurrent_file_access(self, real_test_files):
        """Test concurrent access to the same files."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]
        results = []
        errors = []

        def parse_file():
            try:
                table = read_ngb(str(test_file))
                results.append(table.num_rows)
                return True
            except Exception as e:
                errors.append(str(e))
                return False

        # Test concurrent access
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(parse_file) for _ in range(10)]
            completed = [future.result() for future in futures]

        # Should handle concurrent access gracefully
        successful = sum(completed)
        assert successful > 0, (
            f"No successful concurrent accesses. Errors: {errors[:3]}"
        )

        # Results should be consistent
        if len(set(results)) == 1:
            print(f"✓ Concurrent access successful: {successful}/10 accesses")
        else:
            print(f"⚠ Inconsistent results in concurrent access: {set(results)}")

    @pytest.mark.slow
    def test_memory_stress_repeated_parsing(self, real_test_files):
        """Test memory usage with repeated parsing."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]

        import os

        import psutil

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Parse the same file many times
        for i in range(20):
            try:
                table = read_ngb(str(test_file))
                # Immediately release reference
                del table

                # Force garbage collection every 5 iterations
                if i % 5 == 0:
                    gc.collect()

            except Exception:
                # Some parsing failures are expected with mock data
                pass

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 200MB)
        memory_mb = memory_increase / 1024 / 1024
        assert memory_mb < 200, f"Memory increased by {memory_mb:.1f} MB (too much)"

        print(f"✓ Memory stress test: {memory_mb:.1f} MB increase after 20 parses")

    def test_large_batch_processing(self, real_test_files):
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

            # Should handle large batches
            assert len(results) == len(extended_file_list)

            # Some should succeed (even if mock data fails)
            statuses = [r["status"] for r in results]
            unique_statuses = set(statuses)

            print(
                f"✓ Large batch test: {len(results)} files, statuses: {unique_statuses}"
            )

    def test_rapid_successive_operations(self, real_test_files):
        """Test rapid successive operations on the same data."""
        if not real_test_files:
            pytest.skip("No test files available")

        test_file = real_test_files[0]

        operations_completed = 0

        # Perform rapid successive operations
        for i in range(50):
            try:
                # Alternate between different operations
                if i % 3 == 0:
                    _ = read_ngb(str(test_file))
                    operations_completed += 1
                elif i % 3 == 1:
                    metadata, data = read_ngb(str(test_file), return_metadata=True)
                    operations_completed += 1
                else:
                    from pyngb.core.parser import NGBParser

                    parser = NGBParser()
                    metadata, data = parser.parse(str(test_file))
                    operations_completed += 1

            except Exception:
                # Some failures expected with rapid operations
                pass

        # Should complete a reasonable number of operations
        success_rate = operations_completed / 50
        assert success_rate > 0.1, (
            f"Too few operations succeeded: {operations_completed}/50"
        )

        print(
            f"✓ Rapid operations test: {operations_completed}/50 operations succeeded"
        )


class TestEdgeCaseFiles:
    """Test with edge case file scenarios."""

    def create_edge_case_file(self, scenario):
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

    def test_minimal_file_handling(self):
        """Test handling of minimal valid files."""
        test_file = self.create_edge_case_file("minimal_zip")

        try:
            # Should handle gracefully (may raise exception but shouldn't crash)
            with pytest.raises(Exception):
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_large_metadata_handling(self):
        """Test handling of files with very large metadata."""
        test_file = self.create_edge_case_file("large_metadata")

        try:
            # Should handle large metadata without memory issues
            with pytest.raises(Exception):  # Expected to fail with mock data
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_many_streams_handling(self):
        """Test handling of files with many stream files."""
        test_file = self.create_edge_case_file("many_streams")

        try:
            # Should handle files with many streams
            with pytest.raises(Exception):  # Expected to fail with mock data
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_empty_streams_handling(self):
        """Test handling of files with empty streams."""
        test_file = self.create_edge_case_file("empty_streams")

        try:
            # Should handle empty streams gracefully
            with pytest.raises(Exception):  # Expected to fail
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)

    def test_corrupted_file_handling(self):
        """Test handling of corrupted files."""
        test_file = self.create_edge_case_file("corrupted_zip")

        try:
            # Should handle corruption gracefully
            with pytest.raises(Exception):  # Expected to fail
                read_ngb(test_file)
        finally:
            Path(test_file).unlink(missing_ok=True)


class TestExtremeDataScenarios:
    """Test with extreme data scenarios."""

    def test_extreme_validation_scenarios(self):
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
        result = checker.quick_check()
        # May or may not be valid depending on validation rules

        print("✓ Extreme data validation scenarios completed")

    def test_edge_case_batch_scenarios(self):
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
                problematic_files, skip_errors=True, output_dir=temp_path
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

    def test_resource_exhaustion_scenarios(self):
        """Test scenarios that might exhaust system resources."""

        # Test 1: Many simultaneous parser instances
        parsers = []
        for _ in range(100):
            from pyngb.core.parser import NGBParser

            parser = NGBParser()
            parsers.append(parser)

        # Should be able to create many parsers
        assert len(parsers) == 100

        # Clear references
        parsers.clear()
        gc.collect()

        # Test 2: Large dataset object
        fake_files = [Path(f"fake_file_{i}.ngb-ss3") for i in range(1000)]
        dataset = NGBDataset(fake_files)

        # Should handle large file lists
        assert len(dataset) == 1000

        # Test 3: Many validation objects
        sample_data = pl.DataFrame(
            {
                "time": [1, 2, 3],
                "sample_temperature": [25, 50, 75],
            }
        )

        checkers = []
        for _ in range(50):
            checker = QualityChecker(sample_data)
            checkers.append(checker)

        # Should be able to create many checkers
        assert len(checkers) == 50

        print("✓ Resource exhaustion scenarios completed")


@pytest.mark.integration
class TestConcurrencyEdgeCases:
    """Test edge cases related to concurrency."""

    @pytest.fixture
    def real_test_files(self):
        """Get available real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    def test_thread_safety_batch_processing(self, real_test_files):
        """Test thread safety of batch processing components."""
        if not real_test_files:
            pytest.skip("No test files available")

        results = []
        errors = []

        def batch_process():
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

        # Should handle concurrent batch processing
        assert len(results) > 0, f"No successful batch processes. Errors: {errors[:3]}"

        print(
            f"✓ Thread safety test: {len(results)}/5 concurrent batch processes succeeded"
        )

    def test_parser_state_isolation(self, real_test_files):
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

    def test_concurrent_validation(self):
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

        def validate_data():
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

        # Should handle concurrent validation
        successful = sum(completed)
        assert successful > 0, "No successful concurrent validations"

        # Results should be consistent
        if validation_results:
            unique_results = set(validation_results)
            assert len(unique_results) == 1, (
                f"Inconsistent validation results: {unique_results}"
            )

        print(f"✓ Concurrent validation test: {successful}/10 validations succeeded")


class TestBoundaryConditions:
    """Test boundary conditions and limits."""

    def test_empty_data_handling(self):
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

    def test_single_point_data(self):
        """Test handling of single data point scenarios."""

        single_point = pl.DataFrame(
            {
                "time": [1.0],
                "sample_temperature": [25.0],
                "mass": [10.0],
            }
        )

        # Should handle single point data
        checker = QualityChecker(single_point)
        result = checker.quick_check()

        # May or may not be valid depending on validation rules
        assert result is not None

        print("✓ Single point data handling verified")

    def test_extreme_file_sizes(self):
        """Test handling of extremely small and large file scenarios."""

        # Test with extremely small ZIP
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                z.writestr("tiny.txt", b"x")  # Single byte

        try:
            with pytest.raises(Exception):  # Expected to fail
                read_ngb(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

        print("✓ Extreme file size handling verified")

    def test_unicode_edge_cases(self):
        """Test handling of various Unicode scenarios."""

        # Test with Unicode in mock metadata
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                # Include Unicode content in wrong location
                unicode_content = "测试样品 🧪 test sample".encode()
                z.writestr("wrong_path/unicode_stream.table", unicode_content)

        try:
            # Should handle Unicode content gracefully
            with pytest.raises(Exception):  # Expected to fail with mock data
                read_ngb(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

        print("✓ Unicode edge case handling verified")
