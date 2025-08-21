"""Tests for CLI baseline subtraction functionality."""

import subprocess
import tempfile
from pathlib import Path


class TestCLIBaselineSubtraction:
    """Test CLI baseline subtraction functionality."""

    @property
    def python_exe(self):
        """Get the Python executable path for the current virtual environment."""
        return "/home/gbellamy/git/pynetzsch/.venv/bin/python"

    def test_cli_baseline_subtraction_help(self):
        """Test that CLI help includes baseline options."""
        result = subprocess.run(
            [self.python_exe, "-m", "pyngb", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )

        assert result.returncode == 0
        assert "--baseline" in result.stdout
        assert "--dynamic-axis" in result.stdout
        assert "sample_temperature" in result.stdout

    def test_cli_baseline_subtraction_basic(self):
        """Test basic CLI baseline subtraction."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
        baseline_file = (
            "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    self.python_exe,
                    "-m",
                    "pyngb",
                    sample_file,
                    "-b",
                    baseline_file,
                    "-o",
                    tmpdir,
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 0
            assert "baseline subtraction" in result.stderr

            # Check output file exists with correct name
            output_file = (
                Path(tmpdir)
                / "Douglas_Fir_STA_10K_250730_R13_baseline_subtracted.parquet"
            )
            assert output_file.exists()
            assert output_file.stat().st_size > 0

    def test_cli_baseline_subtraction_dynamic_axis(self):
        """Test CLI baseline subtraction with custom dynamic axis."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
        baseline_file = (
            "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    self.python_exe,
                    "-m",
                    "pyngb",
                    sample_file,
                    "-b",
                    baseline_file,
                    "--dynamic-axis",
                    "time",
                    "-o",
                    tmpdir,
                    "-v",
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 0
            assert "dynamic_axis=time" in result.stderr

            # Check output file exists
            output_file = (
                Path(tmpdir)
                / "Douglas_Fir_STA_10K_250730_R13_baseline_subtracted.parquet"
            )
            assert output_file.exists()

    def test_cli_baseline_subtraction_all_formats(self):
        """Test CLI baseline subtraction with all output formats."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
        baseline_file = (
            "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    self.python_exe,
                    "-m",
                    "pyngb",
                    sample_file,
                    "-b",
                    baseline_file,
                    "-f",
                    "all",
                    "-o",
                    tmpdir,
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 0

            # Check both output files exist
            base_name = "Douglas_Fir_STA_10K_250730_R13_baseline_subtracted"
            parquet_file = Path(tmpdir) / f"{base_name}.parquet"
            csv_file = Path(tmpdir) / f"{base_name}.csv"

            assert parquet_file.exists()
            assert csv_file.exists()
            assert parquet_file.stat().st_size > 0
            assert csv_file.stat().st_size > 0

    def test_cli_baseline_file_not_found(self):
        """Test CLI behavior when baseline file doesn't exist."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
        baseline_file = "nonexistent_baseline.ngb-bs3"

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    self.python_exe,
                    "-m",
                    "pyngb",
                    sample_file,
                    "-b",
                    baseline_file,
                    "-o",
                    tmpdir,
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 1
            assert "does not exist" in result.stderr

    def test_cli_without_baseline_normal_behavior(self):
        """Test that CLI without baseline works normally."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [self.python_exe, "-m", "pyngb", sample_file, "-o", tmpdir],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode == 0
            assert "baseline subtraction" not in result.stderr

            # Check normal output file exists (without baseline suffix)
            output_file = Path(tmpdir) / "Douglas_Fir_STA_10K_250730_R13.parquet"
            assert output_file.exists()
            assert output_file.stat().st_size > 0

    def test_cli_dynamic_axis_validation(self):
        """Test CLI dynamic axis validation."""
        sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
        baseline_file = (
            "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test invalid dynamic axis
            result = subprocess.run(
                [
                    self.python_exe,
                    "-m",
                    "pyngb",
                    sample_file,
                    "-b",
                    baseline_file,
                    "--dynamic-axis",
                    "invalid_axis",
                    "-o",
                    tmpdir,
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )

            assert result.returncode != 0
            assert "invalid choice" in result.stderr.lower()
