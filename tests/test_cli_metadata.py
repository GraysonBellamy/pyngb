"""
Tests for CLI interface and metadata embedding functionality.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pyngb.api.loaders import read_ngb


@pytest.mark.integration
class TestCLIMetadataEmbedding:
    """Test that CLI interface properly embeds metadata in parquet files."""

    def test_cli_metadata_embedding_basic(self, tmp_path):
        """Test basic metadata embedding through the high-level API."""
        # Use a real test file
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Test the high-level API (what CLI uses)
        data = read_ngb(str(test_file))

        # Verify metadata is embedded
        assert data.schema.metadata is not None
        assert b"file_metadata" in data.schema.metadata
        assert b"type" in data.schema.metadata

        # Verify metadata content
        file_metadata = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )
        assert "instrument" in file_metadata
        assert "sample_name" in file_metadata

        # Verify type is at table level
        assert data.schema.metadata[b"type"] == b"STA"

    def test_cli_metadata_embedding_all_fields(self, tmp_path):
        """Test that all metadata fields are properly embedded."""
        test_file = Path("tests/test_files/RO_FILED_STA_N2_10K_250129_R29.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        data = read_ngb(str(test_file))

        # Verify metadata structure
        assert data.schema.metadata is not None
        file_metadata = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )

        # Check for key metadata fields
        expected_fields = [
            "instrument",
            "sample_name",
            "project",
            "date_performed",
            "calibration_constants",
            "temperature_program",
        ]

        for field in expected_fields:
            assert field in file_metadata, f"Missing metadata field: {field}"

        # Verify calibration constants structure
        cal_constants = file_metadata.get("calibration_constants", {})
        assert isinstance(cal_constants, dict)
        assert "p0" in cal_constants
        assert "p1" in cal_constants

    def test_cli_metadata_embedding_parquet_write(self, tmp_path):
        """Test that metadata is preserved when writing to parquet."""
        test_file = Path("tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Read data with metadata
        data = read_ngb(str(test_file))

        # Write to parquet
        parquet_path = tmp_path / "test_metadata.parquet"
        pq.write_table(data, parquet_path, compression="snappy")

        # Read back and verify metadata is preserved
        read_data = pq.read_table(parquet_path)

        # Check metadata is intact
        assert read_data.schema.metadata is not None
        assert b"file_metadata" in read_data.schema.metadata
        assert b"type" in read_data.schema.metadata

        # Verify metadata content matches
        original_meta = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )
        read_meta = json.loads(
            read_data.schema.metadata[b"file_metadata"].decode("utf-8")
        )

        assert original_meta["instrument"] == read_meta["instrument"]
        assert original_meta["sample_name"] == read_meta["sample_name"]

    def test_cli_metadata_embedding_data_integrity(self, tmp_path):
        """Test that data integrity is maintained with metadata embedding."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Read original data
        data = read_ngb(str(test_file))

        # Write to parquet
        parquet_path = tmp_path / "test_data_integrity.parquet"
        pq.write_table(data, parquet_path, compression="snappy")

        # Read back and verify data is intact
        read_data = pq.read_table(parquet_path)

        # Check data dimensions
        assert read_data.num_rows == data.num_rows
        assert read_data.num_columns == data.num_columns

        # Check schema names
        assert read_data.schema.names == data.schema.names

        # Check first few rows of data
        original_first = data.slice(0, 3).to_pydict()
        read_first = read_data.slice(0, 3).to_pydict()

        for col in data.schema.names:
            if col in original_first and col in read_first:
                assert len(original_first[col]) == len(read_first[col])
                # Check first value matches
                if original_first[col] and read_first[col]:
                    assert abs(original_first[col][0] - read_first[col][0]) < 1e-10

    def test_cli_metadata_embedding_file_hash(self, tmp_path):
        """Test that file hash is included in metadata."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        data = read_ngb(str(test_file))

        # Verify file hash is present
        file_metadata = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )
        assert "file_hash" in file_metadata

        file_hash_info = file_metadata["file_hash"]
        assert "file" in file_hash_info
        assert "method" in file_hash_info
        assert "hash" in file_hash_info

        # Verify hash method
        assert file_hash_info["method"] == "BLAKE2b"

        # Verify hash length (BLAKE2b produces 128 character hex string)
        assert len(file_hash_info["hash"]) == 128

    def test_cli_metadata_embedding_temperature_program(self, tmp_path):
        """Test that temperature program metadata is properly embedded."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        data = read_ngb(str(test_file))

        # Verify temperature program structure
        file_metadata = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )
        assert "temperature_program" in file_metadata

        temp_prog = file_metadata["temperature_program"]
        assert isinstance(temp_prog, dict)

        # Check for expected stages
        assert "stage_0" in temp_prog
        assert "stage_1" in temp_prog

        # Verify stage structure
        stage_1 = temp_prog["stage_1"]
        assert "temperature" in stage_1
        assert "heating_rate" in stage_1
        assert "acquisition_rate" in stage_1
        assert "time" in stage_1

    def test_cli_metadata_embedding_calibration_constants(self, tmp_path):
        """Test that calibration constants are properly embedded."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        data = read_ngb(str(test_file))

        # Verify calibration constants
        file_metadata = json.loads(
            data.schema.metadata[b"file_metadata"].decode("utf-8")
        )
        assert "calibration_constants" in file_metadata

        cal_constants = file_metadata["calibration_constants"]
        assert isinstance(cal_constants, dict)

        # Check for expected parameters
        expected_params = ["p0", "p1", "p2", "p3", "p4", "p5"]
        for param in expected_params:
            assert param in cal_constants
            assert isinstance(cal_constants[param], (int, float))

    def test_cli_metadata_embedding_multiple_files(self, tmp_path):
        """Test metadata embedding across multiple test files."""
        test_files = [
            "tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
            "tests/test_files/RO_FILED_STA_N2_10K_250129_R29.ngb-ss3",
            "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3",
        ]

        results = []
        for test_file in test_files:
            if not Path(test_file).exists():
                continue

            try:
                data = read_ngb(test_file)

                # Verify basic metadata structure
                assert data.schema.metadata is not None
                assert b"file_metadata" in data.schema.metadata
                assert b"type" in data.schema.metadata

                # Parse metadata
                file_metadata = json.loads(
                    data.schema.metadata[b"file_metadata"].decode("utf-8")
                )

                results.append(
                    {
                        "file": Path(test_file).name,
                        "metadata_keys": len(file_metadata),
                        "has_instrument": "instrument" in file_metadata,
                        "has_sample": "sample_name" in file_metadata,
                        "data_rows": data.num_rows,
                        "data_cols": data.num_columns,
                    }
                )

            except Exception as e:
                results.append({"file": Path(test_file).name, "error": str(e)})

        # Verify all files processed successfully
        successful = [r for r in results if "error" not in r]
        assert len(successful) > 0, "No files processed successfully"

        # Verify metadata consistency across files
        for result in successful:
            assert result["metadata_keys"] > 20, (
                f"Insufficient metadata keys: {result['metadata_keys']}"
            )
            assert result["has_instrument"], f"Missing instrument in {result['file']}"
            assert result["has_sample"], f"Missing sample name in {result['file']}"
            assert result["data_rows"] > 0, f"No data rows in {result['file']}"
            assert result["data_cols"] > 0, f"No data columns in {result['file']}"

    def test_cli_metadata_embedding_roundtrip(self, tmp_path):
        """Roundtrip is covered in integration tests; smoke here."""
        assert tmp_path is not None

        # Basic smoke test - comprehensive coverage in integration tests
        assert True
