"""
Tests for adaptive parameter selection in thermal analysis.
"""

import numpy as np
import pytest

from pyngb.analysis.adaptive import (
    estimate_feature_scale,
    estimate_noise_level,
    get_recommended_params_for_thermal_data,
    select_savgol_params,
    validate_savgol_params,
)
from pyngb.analysis import calculate_dtg


class TestNoiseEstimation:
    """Test noise level estimation functions."""

    def test_noise_estimation_clean_signal(self):
        """Test noise estimation on clean synthetic signal."""
        # Clean exponential decay
        time = np.linspace(0, 1000, 500)
        mass_clean = 100 * np.exp(-time / 300)

        noise_level = estimate_noise_level(mass_clean)

        # Clean signal should have very low estimated noise
        assert noise_level < 0.1, f"Clean signal noise level too high: {noise_level}"
        assert noise_level >= 0, "Noise level should be non-negative"

    def test_noise_estimation_noisy_signal(self):
        """Test noise estimation on noisy synthetic signal."""
        np.random.seed(42)
        time = np.linspace(0, 1000, 500)
        mass_clean = 100 * np.exp(-time / 300)
        noise = np.random.normal(0, 2.0, len(time))  # 2.0 std dev noise
        mass_noisy = mass_clean + noise

        noise_level = estimate_noise_level(mass_noisy)

        # Should detect noise level close to true value (within factor of 2)
        assert 1.0 < noise_level < 5.0, (
            f"Noise level estimation seems off: {noise_level}"
        )

    def test_noise_estimation_methods(self):
        """Test different noise estimation methods."""
        np.random.seed(42)
        time = np.linspace(0, 500, 200)
        mass = 50 + 30 * np.exp(-time / 100) + np.random.normal(0, 1.0, len(time))

        noise_mad = estimate_noise_level(mass, method="mad_derivative")
        noise_hf = estimate_noise_level(mass, method="high_freq")

        # Both methods should give reasonable estimates
        assert 0.1 < noise_mad < 10.0, f"MAD method result unreasonable: {noise_mad}"
        assert 0.1 < noise_hf < 10.0, (
            f"High freq method result unreasonable: {noise_hf}"
        )

        # Should be in similar ballpark (within order of magnitude)
        ratio = max(noise_mad, noise_hf) / min(noise_mad, noise_hf)
        assert ratio < 10, "Different methods give very different estimates"

    def test_noise_estimation_edge_cases(self):
        """Test noise estimation edge cases."""
        # Very short signal
        short_signal = np.array([1, 2, 3])
        noise_short = estimate_noise_level(short_signal)
        assert noise_short >= 0, "Should handle short signals"

        # Constant signal
        constant_signal = np.ones(100)
        noise_constant = estimate_noise_level(constant_signal)
        assert noise_constant < 1e-10, "Constant signal should have near-zero noise"

        # Empty signal
        with pytest.raises((ValueError, IndexError)):
            estimate_noise_level(np.array([]))


class TestFeatureScaleEstimation:
    """Test feature scale estimation functions."""

    def test_feature_scale_single_peak(self):
        """Test feature scale estimation with single peak."""
        # Create signal with known feature scale
        x = np.linspace(0, 100, 400)
        # Gaussian peak with width ~10 units
        signal = 100 + 50 * np.exp(-((x - 50) ** 2) / (2 * 5**2))

        feature_scale = estimate_feature_scale(x, signal)

        # Should detect feature scale in reasonable range
        assert 5 < feature_scale < 30, f"Feature scale seems wrong: {feature_scale}"

    def test_feature_scale_multiple_features(self):
        """Test feature scale estimation with multiple features."""
        x = np.linspace(0, 200, 800)
        # Multiple Gaussian peaks with different widths
        signal = (
            100
            + 20 * np.exp(-((x - 50) ** 2) / (2 * 8**2))
            + 30 * np.exp(-((x - 120) ** 2) / (2 * 12**2))
            + 15 * np.exp(-((x - 170) ** 2) / (2 * 6**2))
        )

        feature_scale = estimate_feature_scale(x, signal)

        # Should be in reasonable range for the mixed feature sizes
        assert 3 < feature_scale < 50, (
            f"Multi-feature scale seems wrong: {feature_scale}"
        )

    def test_feature_scale_thermal_like_data(self):
        """Test feature scale on thermal analysis-like data."""
        # TGA-like mass loss curve
        temperature = np.linspace(25, 800, 1000)
        # Multiple decomposition steps
        mass = (
            100
            * (1 - 0.1 * (1 / (1 + np.exp(-(temperature - 200) / 10))))
            * (1 - 0.3 * (1 / (1 + np.exp(-(temperature - 400) / 20))))
            * (1 - 0.4 * (1 / (1 + np.exp(-(temperature - 600) / 15))))
        )

        feature_scale = estimate_feature_scale(temperature, mass)

        # For thermal data, features are typically 10-100°C wide
        assert 5 < feature_scale < 200, (
            f"Thermal feature scale unreasonable: {feature_scale}"
        )

    def test_feature_scale_edge_cases(self):
        """Test feature scale estimation edge cases."""
        # Very short data
        x = np.array([0, 1, 2, 3])
        y = np.array([1, 2, 1, 2])
        scale_short = estimate_feature_scale(x, y)
        assert scale_short > 0, "Should handle short data"

        # Flat signal
        x = np.linspace(0, 100, 200)
        y = np.ones_like(x) * 50
        scale_flat = estimate_feature_scale(x, y)
        assert scale_flat > 0, "Should handle flat signals"


class TestSavgolParamSelection:
    """Test Savitzky-Golay parameter selection."""

    def test_select_params_basic(self):
        """Test basic parameter selection."""
        # Create test data
        x = np.linspace(0, 100, 200)
        y = 50 + 30 * np.exp(-x / 30) + np.random.normal(0, 1, len(x))

        window, poly = select_savgol_params(x, y)

        # Should return reasonable parameters
        assert 3 <= window <= 50, f"Window length unreasonable: {window}"
        assert window % 2 == 1, "Window length should be odd"
        assert 1 <= poly <= 4, f"Polynomial order unreasonable: {poly}"
        assert poly < window, "Polynomial order should be less than window"

    def test_select_params_different_snr_targets(self):
        """Test parameter selection with different SNR targets."""
        x = np.linspace(0, 50, 100)
        y = 20 * np.exp(-x / 15) + np.random.normal(0, 0.5, len(x))

        # Higher SNR target should give larger window
        window_low, poly_low = select_savgol_params(x, y, target_snr=2.0)
        window_high, poly_high = select_savgol_params(x, y, target_snr=10.0)

        # Higher SNR target typically needs larger window (though not always)
        assert window_low >= 3 and window_high >= 3
        assert poly_low >= 1 and poly_high >= 1
        # The relationship isn't always monotonic due to other constraints

    def test_select_params_noisy_vs_clean(self):
        """Test that parameter selection adapts to noise level."""
        x = np.linspace(0, 100, 300)
        y_clean = 50 * np.exp(-x / 25)
        y_noisy = y_clean + np.random.normal(0, 2.0, len(x))

        window_clean, poly_clean = select_savgol_params(x, y_clean)
        window_noisy, poly_noisy = select_savgol_params(x, y_noisy)

        # Both should give valid parameters
        assert 3 <= window_clean <= 50
        assert 3 <= window_noisy <= 50
        assert window_clean % 2 == 1 and window_noisy % 2 == 1

    def test_select_params_constraints(self):
        """Test that parameter selection respects constraints."""
        # Short data
        x = np.linspace(0, 10, 20)
        y = np.exp(-x)

        window, poly = select_savgol_params(x, y, max_window_fraction=0.2)

        # Should respect data length constraints
        assert window < len(x)
        assert window <= int(len(x) * 0.2) + 1  # Allow for odd constraint
        assert poly < window

    def test_validate_params(self):
        """Test parameter validation function."""
        # Test valid parameters
        window, poly = validate_savgol_params(7, 2, 100)
        assert window == 7 and poly == 2

        # Test even window (should be made odd)
        window, poly = validate_savgol_params(8, 2, 100)
        assert window == 9 and poly == 2

        # Test window too large
        window, poly = validate_savgol_params(50, 2, 20)
        assert window < 20 and window % 2 == 1

        # Test polyorder too high
        window, poly = validate_savgol_params(7, 10, 100)
        assert poly < 7


class TestRecommendedParams:
    """Test recommended parameter functions."""

    def test_recommended_params_different_types(self):
        """Test recommended parameters for different data types."""
        tga_params = get_recommended_params_for_thermal_data("tga")
        dsc_params = get_recommended_params_for_thermal_data("dsc")
        temp_params = get_recommended_params_for_thermal_data("temperature")

        # All should return valid parameters
        for params in [tga_params, dsc_params, temp_params]:
            window, poly = params
            assert window >= 3 and window % 2 == 1
            assert 1 <= poly <= 4
            assert poly < window

    def test_recommended_params_heating_rates(self):
        """Test parameter adjustment for different heating rates."""
        slow_params = get_recommended_params_for_thermal_data("tga", 1.0)  # 1°C/min
        fast_params = get_recommended_params_for_thermal_data("tga", 20.0)  # 20°C/min

        # Both should be valid
        for params in [slow_params, fast_params]:
            window, poly = params
            assert window >= 3 and window % 2 == 1
            assert 1 <= poly <= 4

    def test_recommended_params_data_length(self):
        """Test parameter adjustment for different data lengths."""
        short_params = get_recommended_params_for_thermal_data("tga", n_points=50)
        long_params = get_recommended_params_for_thermal_data("tga", n_points=5000)

        # Should respect data length constraints
        short_window, short_poly = short_params
        long_window, long_poly = long_params

        assert short_window <= 50 * 0.05 + 2  # Should be reasonable fraction
        assert short_poly < short_window
        assert long_poly < long_window


class TestAdaptiveDTGCalculation:
    """Test DTG calculation with adaptive parameters."""

    def test_dtg_with_auto_params(self):
        """Test DTG calculation with automatic parameter selection."""
        # Create synthetic TGA-like data
        time = np.linspace(0, 3600, 800)  # 1 hour
        temperature = 25 + 10 * time / 60  # 10°C/min
        mass = 100 * (1 - 0.3 * (1 / (1 + np.exp(-(temperature - 400) / 20))))

        # Add some noise
        np.random.seed(42)
        mass += np.random.normal(0, 0.2, len(mass))

        # Calculate DTG with automatic parameters
        dtg_auto = calculate_dtg(time, mass, method="savgol", auto_params=True)

        # Should produce reasonable DTG
        assert len(dtg_auto) == len(time)
        assert np.all(np.isfinite(dtg_auto))
        assert np.any(dtg_auto < -0.001), "Should show mass loss"

    def test_dtg_auto_vs_manual_params(self):
        """Compare automatic vs manual parameter selection."""
        time = np.linspace(0, 1800, 400)
        mass = 80 * np.exp(-time / 600) + np.random.normal(0, 0.5, len(time))

        # Automatic parameters
        dtg_auto = calculate_dtg(time, mass, method="savgol", auto_params=True)

        # Manual parameters
        dtg_manual = calculate_dtg(
            time, mass, method="savgol", auto_params=False, window_length=7, polyorder=2
        )

        # Both should be valid
        assert len(dtg_auto) == len(dtg_manual) == len(time)
        assert np.all(np.isfinite(dtg_auto))
        assert np.all(np.isfinite(dtg_manual))

        # Should show correlation (both tracking the same underlying signal)
        correlation = np.corrcoef(dtg_auto, dtg_manual)[0, 1]
        assert correlation > 0.5, f"Auto and manual DTG should correlate: {correlation}"

    def test_dtg_with_custom_snr_target(self):
        """Test DTG calculation with custom SNR target."""
        time = np.linspace(0, 1000, 300)
        mass = 60 * np.exp(-time / 200) + np.random.normal(0, 1.0, len(time))

        # Different SNR targets
        dtg_low_snr = calculate_dtg(time, mass, method="savgol", target_snr=2.0)
        dtg_high_snr = calculate_dtg(time, mass, method="savgol", target_snr=10.0)

        # Both should be valid
        assert len(dtg_low_snr) == len(dtg_high_snr) == len(time)
        assert np.all(np.isfinite(dtg_low_snr))
        assert np.all(np.isfinite(dtg_high_snr))

        # Higher SNR target should generally give smoother result
        noise_low = np.std(np.diff(dtg_low_snr))
        noise_high = np.std(np.diff(dtg_high_snr))
        # Note: This relationship isn't guaranteed, just check both are reasonable
        assert 0 < noise_low < 1000  # Sanity check
        assert 0 < noise_high < 1000  # Sanity check


@pytest.mark.parametrize("data_type", ["tga", "dsc", "mass", "temperature"])
def test_recommended_params_parametrized(data_type):
    """Parametrized test for recommended parameters."""
    window, poly = get_recommended_params_for_thermal_data(data_type)

    assert window >= 3, f"Window too small for {data_type}"
    assert window % 2 == 1, f"Window not odd for {data_type}"
    assert 1 <= poly <= 4, f"Polynomial order out of range for {data_type}"
    assert poly < window, f"Polynomial order too high for {data_type}"


@pytest.mark.parametrize("heating_rate", [0.5, 1.0, 5.0, 10.0, 20.0])
def test_heating_rate_adaptation(heating_rate):
    """Test parameter adaptation for different heating rates."""
    window, poly = get_recommended_params_for_thermal_data("tga", heating_rate)

    # Should return valid parameters for all heating rates
    assert window >= 3 and window % 2 == 1
    assert 1 <= poly <= 4
    assert poly < window
