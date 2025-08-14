"""
Test temperature program extraction functionality.

This module tests the critical temperature program extraction feature that
extracts complete heating program stages from NGB files. This addresses
a recurring issue where only partial temperature programs were extracted.
"""

import json
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pyngb.batch import BatchProcessor
from pyngb.binary.parser import BinaryParser
from pyngb.constants import PatternConfig
from pyngb.core.parser import NGBParser
from pyngb.extractors.metadata import MetadataExtractor


class TestTemperatureProgramExtraction:
    """Test temperature program extraction in various scenarios."""

    @pytest.fixture
    def test_files(self):
        """Get available test files."""
        test_dir = Path(__file__).parent / "test_files"
        return list(test_dir.glob("*.ngb-ss3"))

    @pytest.fixture
    def metadata_extractor(self):
        """Create metadata extractor for testing."""
        config = PatternConfig()
        parser = BinaryParser()
        return MetadataExtractor(config, parser)

    def test_direct_temperature_program_extraction(
        self, metadata_extractor, test_files
    ):
        """Test direct temperature program extraction on stream data."""
        if not test_files:
            pytest.skip("No test files available")

        for test_file in test_files:
            with zipfile.ZipFile(test_file, "r") as z:
                if "Streams/stream_1.table" not in z.namelist():
                    continue

                with z.open("Streams/stream_1.table") as stream:
                    stream_data = stream.read()

            # Direct extraction on full stream data
            metadata = {}
            metadata_extractor._extract_temperature_program(stream_data, metadata)

            # Verify temperature program was extracted
            assert "temperature_program" in metadata, (
                f"No temperature program in {test_file.name}"
            )
            temp_prog = metadata["temperature_program"]

            # Should have multiple stages for complete programs
            assert isinstance(temp_prog, dict), (
                f"Temperature program not a dict in {test_file.name}"
            )
            assert len(temp_prog) > 0, f"Empty temperature program in {test_file.name}"

            # Each stage should have expected fields
            for stage_key, stage in temp_prog.items():
                assert stage_key.startswith("stage_"), (
                    f"Invalid stage key {stage_key} in {test_file.name}"
                )
                assert isinstance(stage, dict), (
                    f"Stage {stage_key} not a dict in {test_file.name}"
                )

                # Check required fields exist
                expected_fields = [
                    "temperature",
                    "heating_rate",
                    "time",
                    "acquisition_rate",
                ]
                for field in expected_fields:
                    if field in stage:
                        assert isinstance(stage[field], (int, float)), (
                            f"Stage {stage_key} field {field} not numeric in {test_file.name}"
                        )

    def test_extract_metadata_method_completeness(self, metadata_extractor, test_files):
        """Test that extract_metadata method returns complete temperature programs."""
        if not test_files:
            pytest.skip("No test files available")

        for test_file in test_files:
            with zipfile.ZipFile(test_file, "r") as z:
                if "Streams/stream_1.table" not in z.namelist():
                    continue

                with z.open("Streams/stream_1.table") as stream:
                    stream_data = stream.read()

            # Method 1: Direct extraction (baseline)
            direct_metadata = {}
            metadata_extractor._extract_temperature_program(
                stream_data, direct_metadata
            )

            # Method 2: Full extract_metadata method
            parser = BinaryParser()
            tables = parser.split_tables(stream_data)
            full_metadata = metadata_extractor.extract_metadata(tables)

            # Compare results
            if "temperature_program" in direct_metadata:
                direct_stages = len(direct_metadata["temperature_program"])

                assert "temperature_program" in full_metadata, (
                    f"extract_metadata missing temperature_program in {test_file.name}"
                )

                full_stages = len(full_metadata["temperature_program"])

                # CRITICAL: extract_metadata should return same number of stages as direct extraction
                assert full_stages == direct_stages, (
                    f"Stage count mismatch in {test_file.name}: extract_metadata({full_stages}) != direct({direct_stages})"
                )

                # Verify stage data consistency
                for stage_key in direct_metadata["temperature_program"]:
                    assert stage_key in full_metadata["temperature_program"], (
                        f"Missing stage {stage_key} in extract_metadata for {test_file.name}"
                    )

    def test_ngb_parser_temperature_program(self, test_files):
        """Test that NGBParser returns complete temperature programs."""
        if not test_files:
            pytest.skip("No test files available")

        parser = NGBParser()

        for test_file in test_files:
            try:
                metadata, data = parser.parse(str(test_file))

                if "temperature_program" in metadata:
                    temp_prog = metadata["temperature_program"]

                    # Should have meaningful number of stages
                    assert len(temp_prog) >= 1, (
                        f"No temperature program stages in {test_file.name}"
                    )

                    # Verify data structure
                    for stage_key, stage in temp_prog.items():
                        assert isinstance(stage, dict), (
                            f"Stage {stage_key} not dict in {test_file.name}"
                        )

                        # At least temperature should be present
                        if "temperature" in stage:
                            temp_val = stage["temperature"]
                            assert isinstance(temp_val, (int, float)), (
                                f"Temperature not numeric in {stage_key} of {test_file.name}"
                            )
                            assert temp_val >= -50 and temp_val <= 2000, (
                                f"Temperature {temp_val} out of realistic range in {test_file.name}"
                            )

            except Exception as e:
                pytest.fail(f"NGBParser failed on {test_file.name}: {e}")

    def test_batch_processing_temperature_program(self, test_files, tmp_path):
        """Test that batch processing preserves complete temperature programs in parquet files."""
        if not test_files:
            pytest.skip("No test files available")

        processor = BatchProcessor(max_workers=1, verbose=False)

        # Process files
        results = processor.process_files(
            [str(f) for f in test_files],
            output_dir=tmp_path,
            output_format="parquet",
            skip_errors=False,
        )

        # Verify all files processed successfully
        assert all(r["status"] == "success" for r in results), (
            f"Some files failed: {[r for r in results if r['status'] != 'success']}"
        )

        # Check parquet files contain complete temperature programs
        for result in results:
            test_file_name = Path(result["file"]).stem
            parquet_file = tmp_path / f"{test_file_name}.parquet"

            assert parquet_file.exists(), (
                f"Parquet file not created for {test_file_name}"
            )

            # Read embedded metadata
            parquet_table = pq.read_table(parquet_file)
            schema_metadata = parquet_table.schema.metadata

            assert b"file_metadata" in schema_metadata, (
                f"No metadata in {parquet_file.name}"
            )

            metadata_json = schema_metadata[b"file_metadata"].decode("utf-8")
            metadata = json.loads(metadata_json)

            # Verify temperature program completeness
            if "temperature_program" in metadata:
                temp_prog = metadata["temperature_program"]

                # Should have multiple stages for meaningful programs
                assert len(temp_prog) >= 1, (
                    f"Empty temperature program in {parquet_file.name}"
                )

                # Verify stage structure
                for stage_key, stage in temp_prog.items():
                    assert stage_key.startswith("stage_"), (
                        f"Invalid stage key in {parquet_file.name}"
                    )
                    assert isinstance(stage, dict), (
                        f"Stage not dict in {parquet_file.name}"
                    )

    def test_temperature_program_pattern_matching(self, metadata_extractor, test_files):
        """Test that temperature program patterns find the expected number of matches."""
        if not test_files:
            pytest.skip("No test files available")

        for test_file in test_files:
            with zipfile.ZipFile(test_file, "r") as z:
                if "Streams/stream_1.table" not in z.namelist():
                    continue

                with z.open("Streams/stream_1.table") as stream:
                    stream_data = stream.read()

            # Test individual pattern matching
            pattern_matches = {}
            for field_name, pattern in metadata_extractor._compiled_temp_prog.items():
                matches = pattern.findall(stream_data)
                pattern_matches[field_name] = len(matches)

            # If any temperature program patterns match, they should be consistent
            temp_patterns = ["temperature", "heating_rate", "time", "acquisition_rate"]
            found_patterns = [p for p in temp_patterns if pattern_matches.get(p, 0) > 0]

            if found_patterns:
                # All temperature program patterns should find same number of matches
                match_counts = [pattern_matches[p] for p in found_patterns]
                max_matches = max(match_counts)
                min_matches = min(match_counts)

                assert max_matches == min_matches, (
                    f"Inconsistent pattern matches in {test_file.name}: {pattern_matches}"
                )

                # Should find reasonable number of stages
                assert max_matches >= 1, (
                    f"No temperature program patterns found in {test_file.name}"
                )

    def test_temperature_program_regression(self, test_files):
        """Regression test for the specific issue of extract_metadata returning fewer stages than direct extraction."""
        if not test_files:
            pytest.skip("No test files available")

        # This test specifically checks the bug that was fixed:
        # extract_metadata was only returning 1 stage while direct extraction returned 5

        config = PatternConfig()
        parser = BinaryParser()
        extractor = MetadataExtractor(config, parser)

        for test_file in test_files:
            with zipfile.ZipFile(test_file, "r") as z:
                if "Streams/stream_1.table" not in z.namelist():
                    continue

                with z.open("Streams/stream_1.table") as stream:
                    stream_data = stream.read()

            # Direct extraction (should work correctly)
            direct_metadata = {}
            extractor._extract_temperature_program(stream_data, direct_metadata)

            # extract_metadata method (was broken, now fixed)
            tables = parser.split_tables(stream_data)
            full_metadata = extractor.extract_metadata(tables)

            # Both should return same results
            direct_has_temp = "temperature_program" in direct_metadata
            full_has_temp = "temperature_program" in full_metadata

            assert direct_has_temp == full_has_temp, (
                f"Presence mismatch in {test_file.name}: direct({direct_has_temp}) != full({full_has_temp})"
            )

            if direct_has_temp and full_has_temp:
                direct_count = len(direct_metadata["temperature_program"])
                full_count = len(full_metadata["temperature_program"])

                # CRITICAL REGRESSION TEST: Should be equal, not 5 vs 1
                assert direct_count == full_count, (
                    f"REGRESSION: {test_file.name} stage count mismatch - direct: {direct_count}, extract_metadata: {full_count}"
                )


class TestTemperatureProgramSpecificFiles:
    """Test temperature program extraction on specific known files."""

    @pytest.mark.parametrize(
        ("file_pattern", "expected_min_stages"),
        [
            ("Red_Oak_STA_10K_250731_R7.ngb-ss3", 3),
            ("DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3", 3),
            ("RO_FILED_STA_N2_10K_250129_R29.ngb-ss3", 3),
        ],
    )
    def test_specific_file_temperature_programs(
        self, file_pattern, expected_min_stages
    ):
        """Test temperature program extraction on specific files with known expected results."""
        test_dir = Path(__file__).parent / "test_files"
        test_files = list(test_dir.glob(file_pattern))

        if not test_files:
            pytest.skip(f"Test file {file_pattern} not available")

        test_file = test_files[0]

        # Use NGBParser (full pipeline test)
        parser = NGBParser()
        metadata, data = parser.parse(str(test_file))

        # Should have temperature program
        assert "temperature_program" in metadata, (
            f"No temperature program in {test_file.name}"
        )

        temp_prog = metadata["temperature_program"]
        stage_count = len(temp_prog)

        # Should have at least the expected number of stages
        assert stage_count >= expected_min_stages, (
            f"{test_file.name} has {stage_count} stages, expected at least {expected_min_stages}"
        )

        # Verify stage structure and realistic values
        for stage_key, stage in temp_prog.items():
            if "temperature" in stage:
                temp = stage["temperature"]
                assert isinstance(temp, (int, float)), (
                    f"Non-numeric temperature in {stage_key}"
                )
                assert -50 <= temp <= 2000, (
                    f"Unrealistic temperature {temp}°C in {stage_key}"
                )

            if "heating_rate" in stage:
                rate = stage["heating_rate"]
                assert isinstance(rate, (int, float)), (
                    f"Non-numeric heating rate in {stage_key}"
                )
                assert 0 <= rate <= 100, (
                    f"Unrealistic heating rate {rate}°C/min in {stage_key}"
                )

            if "time" in stage:
                time_val = stage["time"]
                assert isinstance(time_val, (int, float)), (
                    f"Non-numeric time in {stage_key}"
                )
                assert 0 <= time_val <= 10000, (
                    f"Unrealistic time {time_val}min in {stage_key}"
                )


if __name__ == "__main__":
    pytest.main([__file__])
