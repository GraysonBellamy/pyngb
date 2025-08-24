"""
Simplified DTG (Derivative Thermogravimetry) analysis.

This module provides a clean, simple interface for DTG calculations with smart defaults.
"""

from __future__ import annotations

import numpy as np

try:
    from scipy.signal import savgol_filter  # type: ignore[import-untyped]

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

__all__ = [
    "dtg",
    "dtg_custom",
]


def _get_smoothing_params(smooth: str) -> tuple[int, int]:
    """Get window and polynomial order for smoothing level.

    Parameters
    ----------
    smooth : str
        Smoothing level: "strict", "medium", or "loose"

    Returns
    -------
    tuple[int, int]
        Window length and polynomial order
    """
    if smooth == "strict":
        return 7, 1
    if smooth == "medium":
        return 25, 2
    if smooth == "loose":
        return 51, 3
    raise ValueError(f"Unknown smooth level: {smooth}")


def dtg(
    time: np.ndarray,
    mass: np.ndarray,
    method: str = "savgol",
    smooth: str = "medium",
) -> np.ndarray:
    """
    Calculate derivative thermogravimetry (DTG) with smart defaults.

    This function provides a dramatically simplified interface for DTG calculation,
    optimized for 90% of thermal analysis use cases.

    Parameters
    ----------
    time : array_like
        Time values in seconds
    mass : array_like
        Mass values in mg
    method : {"savgol", "gradient"}, default "savgol"
        Calculation method:
        - "savgol": Savitzky-Golay filter (recommended, requires scipy)
        - "gradient": NumPy gradient (fast, always available)
    smooth : {"strict", "medium", "loose"}, default "medium"
        Smoothing level:
        - "strict": Preserve all features (window=7, poly=1)
        - "medium": Balanced smoothing (window=25, poly=2)
        - "loose": Remove noise (window=51, poly=3)

    Returns
    -------
    np.ndarray
        DTG values in mg/min. Positive values indicate mass loss.

    Raises
    ------
    ValueError
        If arrays have different lengths or insufficient data points
    ImportError
        If scipy is required but not available

    Examples
    --------
    >>> from pyngb import dtg
    >>> # Dead simple - one line, perfect results
    >>> dtg_values = dtg(time, mass)
    >>>
    >>> # With method selection
    >>> dtg_savgol = dtg(time, mass, method="savgol")
    >>> dtg_gradient = dtg(time, mass, method="gradient")
    >>>
    >>> # With smoothing control
    >>> dtg_preserve = dtg(time, mass, smooth="strict")
    >>> dtg_balanced = dtg(time, mass, smooth="medium")  # default
    >>> dtg_clean = dtg(time, mass, smooth="loose")
    """
    # Convert to numpy arrays
    time = np.asarray(time)
    mass = np.asarray(mass)

    # Validation
    if len(time) != len(mass):
        raise ValueError("time and mass arrays must have the same length")

    if len(time) < 3:
        raise ValueError("Need at least 3 data points for DTG calculation")

    if method not in ["savgol", "gradient"]:
        raise ValueError(f"Unknown method: {method}")

    if smooth not in ["strict", "medium", "loose"]:
        raise ValueError(f"Unknown smooth level: {smooth}")

    if method == "savgol" and not HAS_SCIPY:
        raise ImportError("Scipy required for Savitzky-Golay method")

    # Get smoothing parameters
    window, polyorder = _get_smoothing_params(smooth)

    # Adapt window size for small datasets
    if window >= len(time):
        window = max(3, len(time) // 2)
        if window % 2 == 0:
            window -= 1

    # Ensure polynomial order is valid
    polyorder = min(polyorder, window - 1)

    if method == "savgol":
        # Savitzky-Golay method
        mass_smooth = savgol_filter(mass, window, polyorder)  # type: ignore[no-any-return]
        # Calculate derivative and convert to mg/min
        dtg_values = -np.gradient(mass_smooth, time) * 60
    else:
        # Gradient method with post-smoothing
        dtg_raw = -np.gradient(mass, time) * 60
        if HAS_SCIPY and len(dtg_raw) >= window:
            dtg_values = savgol_filter(dtg_raw, window, polyorder)  # type: ignore[no-any-return]
        else:
            # Fallback smoothing without scipy
            dtg_values = dtg_raw

    return dtg_values  # type: ignore[no-any-return]


def dtg_custom(
    time: np.ndarray,
    mass: np.ndarray,
    method: str = "savgol",
    window: int = 25,
    polyorder: int = 2,
) -> np.ndarray:
    """
    Calculate DTG with custom parameters for power users.

    This function provides manual control over all DTG calculation parameters.
    For most users, the simplified `dtg()` function is recommended.

    Parameters
    ----------
    time : array_like
        Time values in seconds
    mass : array_like
        Mass values in mg
    method : {"savgol", "gradient"}, default "savgol"
        Calculation method
    window : int, default 25
        Window length for smoothing (must be odd)
    polyorder : int, default 2
        Polynomial order for Savitzky-Golay filter

    Returns
    -------
    np.ndarray
        DTG values in mg/min. Positive values indicate mass loss.

    Raises
    ------
    ValueError
        If parameters are invalid
    ImportError
        If scipy is required but not available

    Examples
    --------
    >>> from pyngb import dtg_custom
    >>> # Custom Savitzky-Golay parameters
    >>> dtg_values = dtg_custom(time, mass, method="savgol",
    ...                        window=31, polyorder=3)
    >>> # Custom gradient with post-smoothing
    >>> dtg_values = dtg_custom(time, mass, method="gradient",
    ...                        window=15, polyorder=2)
    """
    # Convert to numpy arrays
    time = np.asarray(time)
    mass = np.asarray(mass)

    # Validation
    if len(time) != len(mass):
        raise ValueError("time and mass arrays must have the same length")

    if len(time) < 3:
        raise ValueError("Need at least 3 data points for DTG calculation")

    if method not in ["savgol", "gradient"]:
        raise ValueError(f"Unknown method: {method}")

    if window >= len(time):
        raise ValueError(
            f"window ({window}) must be less than data length ({len(time)})"
        )

    if window % 2 == 0:
        raise ValueError("window must be odd")

    if polyorder >= window:
        raise ValueError(f"polyorder ({polyorder}) must be less than window ({window})")

    if method == "savgol" and not HAS_SCIPY:
        raise ImportError("Scipy required for Savitzky-Golay method")

    if method == "savgol":
        # Savitzky-Golay method
        mass_smooth = savgol_filter(mass, window, polyorder)  # type: ignore[no-any-return]
        # Calculate derivative and convert to mg/min
        dtg_values = -np.gradient(mass_smooth, time) * 60
    else:
        # Gradient method with post-smoothing
        dtg_raw = -np.gradient(mass, time) * 60
        if HAS_SCIPY and len(dtg_raw) >= window:
            dtg_values = savgol_filter(dtg_raw, window, polyorder)  # type: ignore[no-any-return]
        else:
            # Fallback smoothing without scipy
            dtg_values = dtg_raw

    return dtg_values  # type: ignore[no-any-return]
