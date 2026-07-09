"""
Test temperature program extraction functionality.

This module tests the critical temperature program extraction feature that
extracts complete heating program stages from NGB files. This addresses
a recurring issue where only partial temperature programs were extracted.
Synthetic stage-table units live in test_extract.py; these tests pin the
behavior end-to-end on the real fixtures through the public API.
"""

import json
import logging
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from pyngb import read_ngb_metadata
from pyngb.batch import BatchProcessor

logger = logging.getLogger(__name__)


class TestTemperatureProgramExtraction:
    """Test temperature program extraction in various scenarios."""

    @pytest.fixture
    def test_files(self) -> list[Path]:
        """Get available test files."""
        test_dir = Path(__file__).parent / "test_files"
        return list(test_dir.glob("*.ngb-ss3"))

    def test_temperature_program_structure(self, test_files: Any) -> None:
        """Every fixture yields a well-formed, complete temperature program.

        Regression guard: extract_metadata once returned 1 stage while the
        stream actually recorded 5, so the count is pinned exactly — every
        shipped fixture records a 5-stage program, and a missing program is
        itself a failure.
        """
        if not test_files:
            pytest.skip("No test files available")

        for test_file in test_files:
            metadata = read_ngb_metadata(str(test_file))

            assert "temperature_program" in metadata, (
                f"No temperature program in {test_file.name}"
            )
            temp_prog = metadata["temperature_program"]
            assert isinstance(temp_prog, dict), (
                f"Temperature program not a dict in {test_file.name}"
            )

            stage_keys = [k for k in temp_prog if k.startswith("stage_")]
            assert len(stage_keys) == len(temp_prog), (
                f"Invalid stage key in {test_file.name}"
            )
            assert len(stage_keys) == 5, (
                f"Expected 5 temperature program stages in {test_file.name}, "
                f"got {len(stage_keys)}"
            )

            for stage_key, stage in temp_prog.items():
                assert isinstance(stage, dict), (
                    f"Stage {stage_key} not a dict in {test_file.name}"
                )
                for field in (
                    "temperature",
                    "heating_rate",
                    "time",
                    "acquisition_rate",
                ):
                    if field in stage:
                        assert isinstance(stage[field], (int, float)), (
                            f"Stage {stage_key} field {field} not numeric "
                            f"in {test_file.name}"
                        )
                if "temperature" in stage:
                    assert -50 <= stage["temperature"] <= 2000, (
                        f"Temperature {stage['temperature']} out of realistic "
                        f"range in {test_file.name}"
                    )

    def test_batch_processing_temperature_program(
        self, test_files: Any, tmp_path: Any
    ) -> None:
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
            test_file_name = Path(str(result["file"])).stem
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
        self, file_pattern: str, expected_min_stages: int
    ) -> None:
        """Test temperature program extraction on specific files with known expected results."""
        test_dir = Path(__file__).parent / "test_files"
        test_files = list(test_dir.glob(file_pattern))

        if not test_files:
            pytest.skip(f"Test file {file_pattern} not available")

        test_file = test_files[0]

        metadata = read_ngb_metadata(str(test_file))

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
                    f"Unrealistic time {time_val}s in {stage_key}"
                )


if __name__ == "__main__":
    pytest.main([__file__])
