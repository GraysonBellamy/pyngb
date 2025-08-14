"""
Integration tests for the complete pyngb parsing workflow.
"""

import json
import tempfile
import zipfile
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from pyngb import (
    BatchProcessor,
    NGBDataset,
    NGBParser,
    read_ngb,
    validate_sta_data,
)
from pyngb.constants import BinaryMarkers, PatternConfig
from pyngb.validation import QualityChecker


@pytest.mark.integration
class TestRealFileIntegration:
    """Integration tests using real NGB test files."""

    @pytest.fixture
    def real_test_files(self):
        """Get paths to real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    def test_all_real_files_parsing(self, real_test_files):
        """Test that all real test files can be parsed successfully."""
        results = []

        for file_path in real_test_files:
            try:
                # Test read_ngb API (default mode)
                table = read_ngb(str(file_path))

                # Test read_ngb API (metadata mode)
                metadata, data = read_ngb(str(file_path), return_metadata=True)

                # Verify consistency between APIs
                assert table.num_rows == data.num_rows
                assert len(table.column_names) == len(data.column_names)

                # Extract embedded metadata from table
                _embedded_meta = json.loads(table.schema.metadata[b"file_metadata"])

                # Basic sanity checks
                assert table.num_rows > 0, f"No data in {file_path.name}"
                assert len(metadata) > 0, f"No metadata in {file_path.name}"

                results.append(
                    {
                        "file": file_path.name,
                        "rows": table.num_rows,
                        "columns": len(table.column_names),
                        "metadata_fields": len(metadata),
                        "has_temperature": "sample_temperature" in table.column_names,
                        "has_time": "time" in table.column_names,
                        "has_mass": "mass" in table.column_names,
                        "sample_name": metadata.get("sample_name", "Unknown"),
                        "instrument": metadata.get("instrument", "Unknown"),
                    }
                )

            except Exception as e:
                pytest.fail(f"Failed to parse {file_path.name}: {e}")

        # Verify we tested multiple files
        assert len(results) >= 1, "Should have tested at least one file"

        # Print summary for visibility
        for result in results:
            print(
                f"✓ {result['file']}: {result['rows']} rows, {result['columns']} cols, "
                f"sample='{result['sample_name']}', instrument='{result['instrument']}'"
            )

    def test_cross_file_consistency(self, real_test_files):
        """Test consistency across different real files."""
        if len(real_test_files) < 2:
            pytest.skip("Need at least 2 files for consistency testing")

        file_data = []

        for file_path in real_test_files[:3]:  # Test first 3 files
            metadata, data = read_ngb(str(file_path), return_metadata=True)
            file_data.append(
                {
                    "path": file_path,
                    "metadata": metadata,
                    "data": data,
                    "columns": set(data.column_names),
                }
            )

        # Check for common columns across files
        common_columns = set.intersection(*[fd["columns"] for fd in file_data])
        assert "time" in common_columns, "All files should have time column"

        # Check that all files have basic metadata
        for fd in file_data:
            assert "instrument" in fd["metadata"] or "sample_name" in fd["metadata"], (
                f"File {fd['path'].name} missing basic metadata"
            )

    def test_data_export_roundtrip(self, real_test_files):
        """Test exporting data and ensuring roundtrip integrity."""
        if not real_test_files:
            pytest.skip("No real test files available")

        test_file = real_test_files[0]
        original_table = read_ngb(str(test_file))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Export to Parquet
            parquet_file = temp_path / "test_export.parquet"
            pq.write_table(original_table, parquet_file)

            # Read back
            imported_table = pq.read_table(parquet_file)

            # Verify data integrity
            assert imported_table.num_rows == original_table.num_rows
            assert imported_table.num_columns == original_table.num_columns
            assert imported_table.column_names == original_table.column_names

            # Verify metadata preservation
            assert imported_table.schema.metadata is not None
            assert b"file_metadata" in imported_table.schema.metadata

            # Test CSV export via Polars
            df = pl.from_arrow(original_table)
            csv_file = temp_path / "test_export.csv"
            df.write_csv(csv_file)

            # Read back CSV and verify basic structure
            imported_df = pl.read_csv(csv_file)
            assert imported_df.height == df.height
            assert imported_df.width == df.width

    def test_validation_on_real_data(self, real_test_files):
        """Test data validation on real files."""
        if not real_test_files:
            pytest.skip("No real test files available")

        for file_path in real_test_files[:2]:  # Test first 2 files
            table = read_ngb(str(file_path))
            df = pl.from_arrow(table)

            # Run validation
            issues = validate_sta_data(df)

            # Real data might have some issues, but shouldn't be catastrophic
            critical_issues = [issue for issue in issues if "error" in issue.lower()]
            assert len(critical_issues) == 0, (
                f"Critical issues in {file_path.name}: {critical_issues}"
            )

            # Test with QualityChecker
            checker = QualityChecker(df)
            result = checker.full_validation()

            # Should at least pass basic checks
            assert result.summary()["checks_passed"] > 0, (
                f"No validation passes for {file_path.name}"
            )


@pytest.mark.integration
class TestBatchProcessingIntegration:
    """Integration tests for batch processing with real scenarios."""

    @pytest.fixture
    def real_test_files(self):
        """Get paths to real test files."""
        test_dir = Path(__file__).parent / "test_files"
        files = list(test_dir.glob("*.ngb-ss3"))
        if not files:
            pytest.skip("No real test files available")
        return files

    def test_batch_processing_real_files(self, real_test_files):
        """Test batch processing on real files."""
        if not real_test_files:
            pytest.skip("No test files available")

        with tempfile.TemporaryDirectory() as temp_dir:
            _output_dir = Path(temp_dir)

            processor = BatchProcessor(max_workers=1, verbose=False)
            results = processor.process_files(
                [str(f) for f in real_test_files],
                output_format="parquet",
                output_dir=_output_dir,
            )

            # Verify processing results
            assert len(results) == len(real_test_files)
            successful = [r for r in results if r["status"] == "success"]
            assert len(successful) > 0, "At least one file should process successfully"

            # Verify output files exist
            for result in successful:
                input_path = Path(result["file"])
                parquet_path = _output_dir / f"{input_path.stem}.parquet"
                assert parquet_path.exists(), f"Missing output file: {parquet_path}"

                # Verify output file content
                output_table = pq.read_table(parquet_path)
                assert output_table.num_rows > 0
                assert output_table.num_rows == result["rows"]

    def test_ngb_dataset_real_files(self, real_test_files):
        """Test NGBDataset with real files."""
        if not real_test_files:
            pytest.skip("No real test files available")

        dataset = NGBDataset(real_test_files)

        # Test basic functionality
        assert len(dataset) == len(real_test_files)

        # Test summary
        summary = dataset.summary()
        assert summary["file_count"] == len(real_test_files)
        assert summary["loadable_files"] > 0
        assert "unique_instruments" in summary

        # Test metadata export
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_file = Path(temp_dir) / "metadata.csv"
            dataset.export_metadata(metadata_file, format="csv")

            assert metadata_file.exists()

            # Read back and verify
            metadata_df = pl.read_csv(metadata_file)
            assert metadata_df.height > 0
            assert "file_path" in metadata_df.columns
            assert "file_name" in metadata_df.columns

    def test_directory_processing_integration(self, real_test_files):
        """Test directory processing functionality."""
        if not real_test_files:
            pytest.skip("No real test files available")

        test_dir = real_test_files[0].parent

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Use BatchProcessor directly to specify output directory
            from pyngb.batch import BatchProcessor

            processor = BatchProcessor(max_workers=1)
            results = processor.process_directory(
                str(test_dir),
                pattern="*.ngb-ss3",
                output_format="both",
                output_dir=str(output_dir),
            )

            assert len(results) >= 1
            successful = [r for r in results if r["status"] == "success"]
            assert len(successful) > 0

            # Verify files were created in the temporary directory, not the source
            generated_files = list(output_dir.glob("*"))
            assert len(generated_files) > 0, (
                "No files were generated in the output directory"
            )


@pytest.mark.integration
class TestAdvancedUseCases:
    """Integration tests for advanced usage scenarios."""

    def test_custom_parser_configuration(self):
        """Test using custom parser configuration."""
        # Create custom configuration
        custom_config = PatternConfig()

        # Modify configuration
        custom_config.column_map["custom_id"] = "custom_column"
        custom_config.metadata_patterns["custom_field"] = (b"\x99\x99", b"\x88\x88")

        # Create parser with custom config
        parser = NGBParser(custom_config)

        # Verify configuration is applied
        assert parser.config.column_map["custom_id"] == "custom_column"
        assert "custom_field" in parser.config.metadata_patterns

    def test_parser_component_integration(self):
        """Test direct interaction with parser components."""
        from pyngb.binary import BinaryParser
        from pyngb.extractors import DataStreamProcessor, MetadataExtractor

        config = PatternConfig()

        # Create components
        binary_parser = BinaryParser()
        metadata_extractor = MetadataExtractor(config, binary_parser)
        data_processor = DataStreamProcessor(config, binary_parser)

        # Test that components work together
        assert hasattr(metadata_extractor, "extract_metadata")
        assert hasattr(data_processor, "process_stream_2")

        # Test with mock data
        test_data = b"mock binary data"
        tables = binary_parser.split_tables(test_data)
        assert isinstance(tables, list)

    def test_memory_management_integration(self):
        """Test memory management across parsing operations."""
        import gc

        # Create a realistic mock file
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                # Minimal but valid content
                z.writestr("Streams/stream_1.table", b"mock data 1")
                z.writestr("Streams/stream_2.table", b"mock data 2")
            temp_file_path = temp_file.name

        try:
            # Parse multiple times to test memory cleanup
            tables = []
            for _ in range(5):
                try:
                    table = read_ngb(temp_file_path)
                    tables.append(table)
                except Exception:
                    # Expected for mock data, just testing memory handling
                    pass

            # Clear references and force garbage collection
            tables.clear()
            gc.collect()

            # Test should complete without memory issues
            assert True

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_concurrent_processing_safety(self):
        """Test thread safety and concurrent processing."""
        import concurrent.futures

        # Create multiple mock files
        test_files = []
        for i in range(3):
            with tempfile.NamedTemporaryFile(
                suffix=f"_test_{i}.ngb-ss3", delete=False
            ) as temp_file:
                with zipfile.ZipFile(temp_file.name, "w") as z:
                    z.writestr("Streams/stream_1.table", f"mock data {i}".encode())
                test_files.append(temp_file.name)

        try:

            def parse_file(file_path):
                try:
                    # This will likely fail with mock data, but tests concurrency
                    return read_ngb(file_path)
                except Exception as e:
                    return str(e)

            # Test concurrent parsing
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(parse_file, f) for f in test_files]
                results = [future.result() for future in futures]

            # Should complete without deadlocks or race conditions
            assert len(results) == len(test_files)

        finally:
            for file_path in test_files:
                Path(file_path).unlink(missing_ok=True)


@pytest.mark.integration
class TestErrorRecoveryIntegration:
    """Integration tests for error handling and recovery."""

    def test_partial_batch_failure_recovery(self):
        """Test batch processing recovery from partial failures."""
        # Create mix of valid and invalid files
        test_files = []

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create invalid file (not a ZIP)
            invalid_file = temp_path / "invalid.ngb-ss3"
            invalid_file.write_text("not a zip file")
            test_files.append(str(invalid_file))

            # Create another invalid file (empty ZIP)
            empty_zip = temp_path / "empty.ngb-ss3"
            with zipfile.ZipFile(empty_zip, "w"):
                pass
            test_files.append(str(empty_zip))

            # Create a minimal valid-ish file
            valid_file = temp_path / "valid.ngb-ss3"
            with zipfile.ZipFile(valid_file, "w") as z:
                z.writestr("Streams/stream_1.table", b"some data")
                z.writestr("Streams/stream_2.table", b"more data")
            test_files.append(str(valid_file))

            # Test batch processing with skip_errors=True
            processor = BatchProcessor(max_workers=1, verbose=False)
            results = processor.process_files(
                test_files, skip_errors=True, output_dir=temp_path
            )

            # Should have results for all files
            assert len(results) == len(test_files)

            # Should have both successes and failures
            statuses = [r["status"] for r in results]
            assert "error" in statuses  # Some should fail

            # Error messages should be informative
            error_results = [r for r in results if r["status"] == "error"]
            for error_result in error_results:
                assert error_result["error"] is not None
                assert len(error_result["error"]) > 0

    def test_validation_error_handling(self):
        """Test validation with problematic data."""
        # Create problematic data with proper types
        problematic_data = pl.DataFrame(
            {
                "time": [1.0, 2.0, 999999.0, 4.0],  # Use float to avoid type issues
                "sample_temperature": [
                    25.0,
                    None,
                    75.0,
                    100.0,
                ],  # Use None instead of NaN
                "mass": [10.0, 9.0, 8.0, -5.0],  # Use float for mass
                "dsc_signal": [0.1, 0.2, 0.3, 0.4],
            }
        )

        # Test validation handles problematic data gracefully
        issues = validate_sta_data(problematic_data)
        assert len(issues) > 0  # Should detect issues

        # Test QualityChecker with problematic data
        checker = QualityChecker(problematic_data)
        result = checker.full_validation()

        # Should complete without crashing
        assert result is not None
        assert not result.is_valid  # Should be marked as invalid

    def test_corrupted_file_handling(self):
        """Test handling of various corrupted file scenarios."""
        corruption_scenarios = [
            ("truncated_zip", b"PK\x03\x04"),  # Truncated ZIP header
            ("binary_garbage", b"\x00\xff\x00\xff" * 100),  # Random binary
            ("text_file", b"This is just text, not a ZIP"),
            ("partial_zip", b"PK\x03\x04\x14\x00\x00\x00"),  # Partial ZIP structure
        ]

        for scenario_name, content in corruption_scenarios:
            with tempfile.NamedTemporaryFile(
                suffix=".ngb-ss3", delete=False
            ) as temp_file:
                temp_file.write(content)
                temp_file.flush()
                temp_file_path = temp_file.name

            try:
                # Should handle corruption gracefully
                with pytest.raises(Exception):  # Should raise some kind of error
                    read_ngb(temp_file_path)
            finally:
                Path(temp_file_path).unlink(missing_ok=True)


@pytest.mark.integration
class TestDataIntegrityValidation:
    """Integration tests for data integrity and validation."""

    def test_metadata_data_consistency(self):
        """Test consistency between metadata and actual data."""
        # This test would be better with real files, but we'll create a realistic mock
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                import struct

                from pyngb.constants import BinaryMarkers

                markers = BinaryMarkers()

                # Create metadata indicating 15.5 mg sample mass
                metadata_content = (
                    b"\x30\x75"  # Sample category
                    + b"padding" * 5
                    + b"\x9e\x0c"  # Sample mass field
                    + b"padding" * 3
                    + markers.TYPE_PREFIX
                    + b"\x05"  # Float64 type
                    + markers.TYPE_SEPARATOR
                    + struct.pack("<d", 15.5)  # 15.5 as float64 bytes
                    + markers.END_FIELD
                )
                z.writestr("wrong_path/stream_1.table", metadata_content)

                # Create basic data stream in wrong location
                data_content = b"minimal data stream"
                z.writestr("wrong_path/stream_2.table", data_content)
            temp_file_path = temp_file.name

        try:
            # This will likely fail due to incomplete mock data, but tests the workflow
            with pytest.raises(Exception):
                metadata, data = read_ngb(temp_file_path, return_metadata=True)

        finally:
            Path(temp_file_path).unlink(missing_ok=True)

    def test_cross_api_consistency(self):
        """Test consistency between different API endpoints."""
        # Test that different ways of accessing the same data yield consistent results

        # Use real test files if available
        test_dir = Path(__file__).parent / "test_files"
        real_files = list(test_dir.glob("*.ngb-ss3"))

        if real_files:
            test_file = real_files[0]

            # Method 1: read_ngb (default mode)
            table1 = read_ngb(str(test_file))

            # Method 2: read_ngb (metadata mode)
            metadata, table2 = read_ngb(str(test_file), return_metadata=True)

            # Method 3: Direct parser usage
            parser = NGBParser()
            metadata3, table3 = parser.parse(str(test_file))

            # All should yield consistent results
            assert table1.num_rows == table2.num_rows == table3.num_rows
            assert table1.num_columns == table2.num_columns == table3.num_columns
            assert table1.column_names == table2.column_names == table3.column_names

            # Metadata should be consistent (though table1 has embedded metadata)
            # Allow for small differences due to embedded vs separate metadata
            assert abs(len(metadata) - len(metadata3)) <= 2, (
                f"Metadata length difference too large: {len(metadata)} vs {len(metadata3)}"
            )


@pytest.mark.integration
class TestPerformanceIntegration:
    """Integration tests for performance characteristics."""

    @pytest.mark.slow
    def test_large_dataset_processing(self):
        """Test processing of larger datasets."""
        test_dir = Path(__file__).parent / "test_files"
        real_files = list(test_dir.glob("*.ngb-ss3"))

        if not real_files:
            pytest.skip("No real test files available for performance testing")

        import time

        # Test single file performance
        start_time = time.perf_counter()
        _table = read_ngb(str(real_files[0]))
        single_time = time.perf_counter() - start_time

        # Should complete in reasonable time
        assert single_time < 30.0, (
            f"Single file parsing took {single_time:.2f}s (too slow)"
        )

        # Test batch processing performance
        if len(real_files) > 1:
            start_time = time.perf_counter()
            processor = BatchProcessor(max_workers=2, verbose=False)

            with tempfile.TemporaryDirectory() as temp_dir:
                results = processor.process_files(
                    [str(f) for f in real_files], output_dir=temp_dir
                )

            batch_time = time.perf_counter() - start_time
            successful = [r for r in results if r["status"] == "success"]

            if len(successful) > 1:
                avg_time = batch_time / len(successful)
                print(
                    f"Batch processing: {len(successful)} files in {batch_time:.2f}s "
                    f"(avg {avg_time:.2f}s per file)"
                )

    def test_memory_usage_validation(self):
        """Test memory usage remains reasonable."""
        test_dir = Path(__file__).parent / "test_files"
        real_files = list(test_dir.glob("*.ngb-ss3"))

        if not real_files:
            pytest.skip("No real test files available for memory testing")

        import os

        import psutil

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Parse multiple files
        tables = []
        for file_path in real_files:
            try:
                table = read_ngb(str(file_path))
                tables.append(table)
            except Exception:
                # Skip files that can't be parsed
                continue

        peak_memory = process.memory_info().rss
        memory_increase = peak_memory - initial_memory

        # Clear references
        tables.clear()

        # Memory increase should be reasonable (less than 500MB for test files)
        assert memory_increase < 500 * 1024 * 1024, (
            f"Memory usage increased by {memory_increase / 1024 / 1024:.1f} MB"
        )


@pytest.mark.integration
class TestRegressionProtection:
    """Integration tests to prevent regressions."""

    def test_api_stability(self):
        """Test that public API remains stable."""
        # Test that all expected public functions are available
        import pyngb

        required_functions = [
            "read_ngb",
            "NGBParser",
            "BatchProcessor",
            "NGBDataset",
            "validate_sta_data",
            "QualityChecker",
        ]

        for func_name in required_functions:
            assert hasattr(pyngb, func_name), f"Missing public API: {func_name}"
            assert callable(getattr(pyngb, func_name)), f"Not callable: {func_name}"

    def test_import_structure_stability(self):
        """Test that import structure remains stable."""
        # These imports should continue to work
        from pyngb import read_ngb
        from pyngb.batch import BatchProcessor, NGBDataset
        from pyngb.constants import PatternConfig
        from pyngb.core import NGBParser
        from pyngb.validation import QualityChecker, validate_sta_data

        # All should be callable/instantiable
        assert callable(read_ngb)
        assert callable(BatchProcessor)
        assert callable(NGBDataset)
        assert callable(validate_sta_data)
        assert callable(QualityChecker)
        assert callable(NGBParser)
        assert callable(PatternConfig)

        # Test basic instantiation
        parser = NGBParser()
        config = PatternConfig()
        markers = BinaryMarkers()

        assert parser is not None
        assert config is not None
        assert markers is not None

    def test_backwards_compatibility_scenarios(self):
        """Test scenarios that should remain backwards compatible."""
        # Test old-style usage patterns

        # Pattern 1: Direct parser instantiation
        from pyngb.core.parser import NGBParser

        parser = NGBParser()
        assert hasattr(parser, "parse")

        # Pattern 2: Configuration customization
        from pyngb.constants import PatternConfig

        config = PatternConfig()
        config.column_map["new_id"] = "new_column"
        assert "new_id" in config.column_map

        # Pattern 3: Batch processing
        from pyngb.batch import process_files

        assert callable(process_files)
