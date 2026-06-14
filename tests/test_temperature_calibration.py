"""Tests for temperature-calibration metadata extraction.

The temperature calibration is extracted for traceability/QA only. The
``sample_temperature`` channel in NGB files is already temperature-corrected by
Proteus, so these coefficients are never applied to the data by pyngb.
"""

import json
import math
from pathlib import Path

import pytest

from pyngb import read_ngb

TEST_DIR = Path(__file__).parent / "test_files"

# Primary regression fixture - a stable, descriptively named file.
REG_FILE = TEST_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3"

# Expected coefficients [B0, B1, B2] (float32, so compare with tolerance).
EXPECTED_COEFFICIENTS = [-43.89777374267578, -811.7273559570312, 247.13131713867188]

# Expected fixpoint standards (name, actual_c, measured_c, corrected_c).
EXPECTED_FIXPOINTS = [
    ("Biphenyl", 69.2, 69.80000305175781, 69.2015609741211),
    ("Benzoeacid", 122.4, 123.5, 122.4913101196289),
    ("RbNO3(trig>kub)", 164.9, 165.39999389648438, 164.0811004638672),
    ("RbNO3 (III)", 283.9, 287.0, 284.83001708984375),
    ("KClO4", 299.7, 303.0, 300.72344970703125),
]


def _correction(temp: float, coeffs: list[float]) -> float:
    """Proteus temperature correction polynomial (NOT applied to sample data)."""
    b0, b1, b2 = coeffs
    return 1e-3 * b0 + 1e-5 * b1 * temp + 1e-8 * b2 * temp**2


def _metadata(path: Path) -> dict:
    table = read_ngb(str(path))
    return json.loads(table.schema.metadata[b"file_metadata"])


@pytest.mark.skipif(not REG_FILE.exists(), reason="regression file missing")
class TestTemperatureCalibrationRegression:
    """Pin the extracted temperature calibration for a known file."""

    @pytest.fixture(scope="class")
    def md(self) -> dict:
        return _metadata(REG_FILE)

    def test_block_present(self, md: dict) -> None:
        assert "temperature_calibration" in md
        assert "sensitivity_record_path" in md

    def test_coefficients(self, md: dict) -> None:
        coeffs = md["temperature_calibration"]["coefficients"]
        assert len(coeffs) == 3
        for got, want in zip(coeffs, EXPECTED_COEFFICIENTS):
            assert math.isclose(got, want, rel_tol=1e-5)

    def test_fixpoints(self, md: dict) -> None:
        fixpoints = md["temperature_calibration"]["fixpoints"]
        assert len(fixpoints) == 5
        for fp, (name, actual, measured, corrected) in zip(
            fixpoints, EXPECTED_FIXPOINTS
        ):
            assert fp["name"] == name
            assert math.isclose(fp["actual_c"], actual, abs_tol=0.1)
            assert math.isclose(fp["measured_c"], measured, rel_tol=1e-5)
            assert math.isclose(fp["corrected_c"], corrected, rel_tol=1e-5)
            assert fp["weight"] == 1.0

    def test_record_paths(self, md: dict) -> None:
        record = md["temperature_calibration"]["record_path"]
        assert record.endswith(".ngb-ts3")
        assert "Calibrations" in record
        sensitivity = md["sensitivity_record_path"]
        assert sensitivity.endswith(".ngb-es3")
        assert "Calibrations" in sensitivity


@pytest.mark.skipif(not list(TEST_DIR.glob("*.ngb-ss3")), reason="no real test files")
class TestTemperatureCalibrationStability:
    """The block should extract consistently across all available files."""

    @pytest.mark.parametrize("path", sorted(TEST_DIR.glob("*.ngb-ss3")))
    def test_well_formed(self, path: Path) -> None:
        md = _metadata(path)
        cal = md.get("temperature_calibration")
        assert cal is not None, f"no temperature_calibration in {path.name}"

        # Exactly three coefficients.
        assert len(cal["coefficients"]) == 3

        # Exactly five fixpoints, all sane, in ascending actual temperature.
        fixpoints = cal["fixpoints"]
        assert len(fixpoints) == 5
        actuals = [fp["actual_c"] for fp in fixpoints]
        assert actuals == sorted(actuals)
        for fp in fixpoints:
            assert fp["name"]
            assert 0.0 < fp["actual_c"] < 2000.0
            # Corrected value should land near the actual (literature) value.
            assert abs(fp["corrected_c"] - fp["actual_c"]) < 10.0

        # Record paths present and correctly typed.
        assert cal["record_path"].endswith(".ngb-ts3")
        assert md["sensitivity_record_path"].endswith(".ngb-es3")

    @pytest.mark.parametrize("path", sorted(TEST_DIR.glob("*.ngb-ss3")))
    def test_corrected_is_measured_plus_polynomial(self, path: Path) -> None:
        """corrected_c == measured_c + correction(measured_c) for every fixpoint.

        This pins the meaning of the columns and the coefficient formula: the
        ``corrected`` value is the ``measured`` value passed through the
        ``be 04`` calibration polynomial.
        """
        cal = _metadata(path)["temperature_calibration"]
        coeffs = cal["coefficients"]
        for fp in cal["fixpoints"]:
            predicted = fp["measured_c"] + _correction(fp["measured_c"], coeffs)
            assert math.isclose(predicted, fp["corrected_c"], abs_tol=1e-2)

    def test_coefficients_not_applied_to_temperature(self) -> None:
        """Sample temperature must remain the raw (already-corrected) channel.

        Guards against a regression where someone wires the coefficients into the
        data pipeline. The first sample-temperature reading is near ambient and
        must not be shifted by the calibration polynomial.
        """
        table = read_ngb(str(REG_FILE))
        if "sample_temperature" not in table.column_names:
            pytest.skip("no sample_temperature column")
        first = table.column("sample_temperature")[0].as_py()
        # Near room temperature; a double-correction would move this by >>1 °C.
        assert 15.0 < first < 35.0
