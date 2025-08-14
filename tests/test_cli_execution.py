"""
Tests for CLI command execution and file generation.
"""

import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest


@pytest.mark.integration
class TestCLIExecution:
    """Test actual CLI command execution and file generation."""

    def test_cli_command_execution_basic(self, tmp_path):
        """Test basic CLI command execution."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Run CLI command
        output_dir = tmp_path / "cli_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(output_dir),
            "-f",
            "parquet",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify output file was created
        expected_file = output_dir / "Red_Oak_STA_10K_250731_R7.parquet"
        assert expected_file.exists(), f"Output file not created: {expected_file}"

        # Verify file contains data and metadata
        data = pq.read_table(expected_file)
        assert data.num_rows > 0
        assert data.num_columns > 0
        assert data.schema.metadata is not None
        assert b"file_metadata" in data.schema.metadata

    def test_cli_command_execution_verbose(self, tmp_path):
        """Test CLI command execution with verbose output."""
        test_file = Path("tests/test_files/RO_FILED_STA_N2_10K_250129_R29.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Run CLI command with verbose flag
        output_dir = tmp_path / "cli_verbose_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(output_dir),
            "-f",
            "parquet",
            "-v",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify verbose output contains expected messages (check stderr for debug/info)
        assert ("Successfully parsed" in result.stderr) or ("Parsed" in result.stderr)
        assert (
            ("DEBUG" in result.stderr)
            or ("INFO" in result.stderr)
            or ("VERBOSE" in result.stderr)
        )

        # Verify output file was created
        expected_file = output_dir / "RO_FILED_STA_N2_10K_250129_R29.parquet"
        assert expected_file.exists()

    def test_cli_command_execution_csv_format(self, tmp_path):
        """Test CLI command execution with CSV output format."""
        test_file = Path("tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Run CLI command with CSV format
        output_dir = tmp_path / "cli_csv_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(output_dir),
            "-f",
            "csv",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify CSV file was created
        expected_file = output_dir / "DF_FILED_STA_21O2_10K_220222_R1.csv"
        assert expected_file.exists()

        # Verify CSV file has content
        csv_content = expected_file.read_text()
        assert len(csv_content) > 0
        assert "time," in csv_content  # Should have header

    def test_cli_command_execution_both_formats(self, tmp_path):
        """Test CLI command execution with both CSV and Parquet output."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Run CLI command with both formats
        output_dir = tmp_path / "cli_both_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(output_dir),
            "-f",
            "all",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify both files were created
        parquet_file = output_dir / "Red_Oak_STA_10K_250731_R7.parquet"
        csv_file = output_dir / "Red_Oak_STA_10K_250731_R7.csv"

        assert parquet_file.exists(), "Parquet file not created"
        assert csv_file.exists(), "CSV file not created"

        # Verify parquet file has metadata
        data = pq.read_table(parquet_file)
        assert data.schema.metadata is not None
        assert b"file_metadata" in data.schema.metadata

    def test_cli_command_execution_invalid_file(self, tmp_path):
        """Test CLI command execution with invalid input file."""
        # Run CLI command with non-existent file
        output_dir = tmp_path / "cli_error_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            "nonexistent_file.ngb-ss3",
            "-o",
            str(output_dir),
            "-f",
            "parquet",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command failed as expected
        assert result.returncode != 0, "CLI should fail with invalid file"
        assert "does not exist" in result.stderr or "FileNotFoundError" in result.stderr

    def test_cli_command_execution_invalid_output_dir(self, tmp_path):
        """Test CLI command execution with invalid output directory."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Try to write to a non-existent parent directory
        invalid_output = tmp_path / "nonexistent" / "subdir"

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(invalid_output),
            "-f",
            "parquet",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Command should succeed (creates directories automatically)
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify output file was created
        expected_file = invalid_output / "Red_Oak_STA_10K_250731_R7.parquet"
        assert expected_file.exists()

    def test_cli_command_execution_help(self):
        """Test CLI help command."""
        cmd = [sys.executable, "-m", "pyngb", "--help"]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify help command succeeded
        assert result.returncode == 0, f"Help command failed: {result.stderr}"

        # Verify help output contains expected information
        help_text = result.stdout
        assert "Parse NETZSCH STA NGB files" in help_text
        assert "positional arguments:" in help_text
        assert "options:" in help_text
        assert "parquet" in help_text
        assert "csv" in help_text

    def test_cli_command_execution_file_extension_validation(self, tmp_path):
        """Test CLI command execution with different file extensions."""
        test_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Create a copy with different extension
        test_file_copy = tmp_path / "test_file.ngb"
        test_file_copy.write_bytes(test_file.read_bytes())

        # Run CLI command with .ngb extension
        output_dir = tmp_path / "cli_ext_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file_copy),
            "-o",
            str(output_dir),
            "-f",
            "parquet",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded (should handle different extensions)
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify output file was created
        expected_file = output_dir / "test_file.parquet"
        assert expected_file.exists()

    def test_cli_command_execution_large_file_handling(self, tmp_path):
        """Test CLI command execution with larger files."""
        test_file = Path("tests/test_files/RO_FILED_STA_N2_10K_250129_R29.ngb-ss3")
        if not test_file.exists():
            pytest.skip("Test file not available")

        # Run CLI command with larger file
        output_dir = tmp_path / "cli_large_output"
        output_dir.mkdir()

        cmd = [
            sys.executable,
            "-m",
            "pyngb",
            str(test_file),
            "-o",
            str(output_dir),
            "-f",
            "parquet",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Verify command succeeded
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify output file was created and has expected size
        expected_file = output_dir / "RO_FILED_STA_N2_10K_250129_R29.parquet"
        assert expected_file.exists()

        # Verify file size is reasonable (should be larger than input due to metadata)
        input_size = test_file.stat().st_size
        output_size = expected_file.stat().st_size
        assert output_size > input_size * 0.5, (
            f"Output file too small: {output_size} vs {input_size}"
        )

        # Verify metadata is present
        data = pq.read_table(expected_file)
        assert data.schema.metadata is not None
        assert b"file_metadata" in data.schema.metadata
