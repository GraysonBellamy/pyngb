"""Tests for DSC sensitivity-fixpoint metadata extraction.

The sensitivity fixpoints are the enthalpy standards behind the
``calibration_constants`` p0-p5 curve, extracted for traceability/QA only.
They live in the same ``30 75``+ categories as the temperature fixpoints but
form a distinct table family (carrying the transition-temperature field
``04 54`` and no actual-temperature field ``04 44``).

Two identities pin the column semantics and are asserted for every fixture:

    measured_sensitivity = peak_area / enthalpy
    fitted_sensitivity   = (P2 + P3*z + P4*z**2 + P5*z**3) * exp(-z**2)
    z = (temperature_c - P0) / P1
"""

import json
import math
from pathlib import Path

import pytest

from pyngb import read_ngb

TEST_DIR = Path(__file__).parent / "test_files"

# Primary regression fixture - a stable, descriptively named file.
REG_FILE = TEST_DIR / "Douglas_Fir_STA_10K_250730_R13.ngb-ss3"

# Expected standards (name, temperature_c, enthalpy, peak_area,
# measured_sensitivity, fitted_sensitivity), in category (ascending
# temperature) order. Values are float32; compare with tolerance.
EXPECTED_SENS_FIXPOINTS = [
    ("Biphenyl", 69.2, -120.5, -853.85, 7.0858917, 7.0531845),
    ("Benzoeacid", 122.4, -147.4, -999.95, 6.7839217, 6.7310123),
    ("RbNO3(trig>kub)", 164.2, -25.97, -164.58, 6.3373127, 6.4702630),
    ("KClO4", 300.0, -104.8, -590.53, 5.6348286, 5.5959873),
    ("Ag2SO4", 426.2, -51.9, -246.66, 4.7526011, 4.7571077),
    ("CsCl", 476.0, -17.2, -76.872, 4.4693022, 4.4196649),
    ("K2CrO4", 669.0, -37.08, -112.28, 3.0280473, 3.1017020),
    ("BaCO3", 807.0, -98.5, -221.46, 2.2483249, 2.2112181),
]


def _curve(temp: float, constants: dict[str, float]) -> float:
    """The Proteus DSC sensitivity curve defined by calibration_constants."""
    z = (temp - constants["p0"]) / constants["p1"]
    poly = (
        constants["p2"]
        + constants["p3"] * z
        + constants["p4"] * z**2
        + constants["p5"] * z**3
    )
    return poly * math.exp(-(z**2))


def _metadata(path: Path) -> dict:
    table = read_ngb(str(path))
    return json.loads(table.schema.metadata[b"file_metadata"])


@pytest.mark.skipif(not REG_FILE.exists(), reason="regression file missing")
class TestSensitivityFixpointRegression:
    """Pin the extracted sensitivity fixpoints for a known file."""

    @pytest.fixture(scope="class")
    def md(self) -> dict:
        return _metadata(REG_FILE)

    def test_fixpoints(self, md: dict) -> None:
        fixpoints = md["sensitivity_calibration"]["fixpoints"]
        assert len(fixpoints) == len(EXPECTED_SENS_FIXPOINTS)
        for fp, (name, temp, enthalpy, area, measured, fitted) in zip(
            fixpoints, EXPECTED_SENS_FIXPOINTS
        ):
            assert fp["name"] == name
            assert math.isclose(fp["temperature_c"], temp, abs_tol=0.1)
            assert math.isclose(fp["enthalpy"], enthalpy, rel_tol=1e-5)
            assert math.isclose(fp["peak_area"], area, rel_tol=1e-5)
            assert math.isclose(fp["measured_sensitivity"], measured, rel_tol=1e-5)
            assert math.isclose(fp["fitted_sensitivity"], fitted, rel_tol=1e-5)
            assert fp["weight"] == 1.0

    def test_temperature_fixpoints_unaffected(self, md: dict) -> None:
        """The two families share categories and field ids; extracting one
        must not bleed into the other."""
        temp_fixpoints = md["temperature_calibration"]["fixpoints"]
        assert all("enthalpy" not in fp for fp in temp_fixpoints)
        assert all("actual_c" in fp for fp in temp_fixpoints)
        sens_fixpoints = md["sensitivity_calibration"]["fixpoints"]
        assert all("actual_c" not in fp for fp in sens_fixpoints)


@pytest.mark.skipif(not list(TEST_DIR.glob("*.ngb-*")), reason="no real test files")
class TestSensitivityFixpointStability:
    """The block should extract consistently across all available files."""

    @pytest.mark.parametrize("path", sorted(TEST_DIR.glob("*.ngb-*")))
    def test_well_formed(self, path: Path) -> None:
        md = _metadata(path)
        sens = md.get("sensitivity_calibration")
        assert sens is not None, f"no sensitivity_calibration in {path.name}"

        # Real calibrations carry 6-8 standards, in ascending temperature.
        fixpoints = sens["fixpoints"]
        assert 6 <= len(fixpoints) <= 16
        temps = [fp["temperature_c"] for fp in fixpoints]
        assert temps == sorted(temps)
        for fp in fixpoints:
            assert fp["name"]
            assert 0.0 < fp["temperature_c"] < 2000.0
            # Endothermic-negative sign convention, as stored by Proteus.
            assert fp["enthalpy"] < 0.0
            assert fp["peak_area"] < 0.0
            assert fp["measured_sensitivity"] > 0.0
            assert fp["fitted_sensitivity"] > 0.0

    @pytest.mark.parametrize("path", sorted(TEST_DIR.glob("*.ngb-*")))
    def test_measured_is_area_over_enthalpy(self, path: Path) -> None:
        """measured_sensitivity == peak_area / enthalpy for every standard.

        This pins the meaning of the columns: the measured sensitivity point
        is the DSC peak area normalized by the literature enthalpy.
        """
        sens = _metadata(path)["sensitivity_calibration"]
        for fp in sens["fixpoints"]:
            assert math.isclose(
                fp["peak_area"] / fp["enthalpy"],
                fp["measured_sensitivity"],
                rel_tol=1e-5,
            )

    @pytest.mark.parametrize("path", sorted(TEST_DIR.glob("*.ngb-*")))
    def test_fitted_is_calibration_curve(self, path: Path) -> None:
        """fitted_sensitivity == calibration_constants curve at temperature_c.

        This pins both the fixpoints and the p0-p5 semantics: the standards
        are the regression behind the curve that apply_dsc_calibration uses.
        """
        md = _metadata(path)
        constants = md["calibration_constants"]
        for fp in md["sensitivity_calibration"]["fixpoints"]:
            assert math.isclose(
                _curve(fp["temperature_c"], constants),
                fp["fitted_sensitivity"],
                rel_tol=1e-4,
            )
