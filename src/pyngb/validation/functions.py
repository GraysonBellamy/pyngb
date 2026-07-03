"""Standalone validation functions for STA data."""

import numpy as np
import polars as pl
import pyarrow as pa
from scipy.signal import find_peaks

from ..constants import FileMetadata
from .checker import QualityChecker
from .helpers import _ensure_polars_dataframe


def validate_sta_data(
    data: pa.Table | pl.DataFrame, metadata: FileMetadata | None = None
) -> list[str]:
    """Quick validation function that returns a list of issues.

    Convenience function for basic validation without detailed reporting.

    Args:
        data: STA data table or dataframe
        metadata: Optional metadata dictionary

    Returns:
        List of validation issues found

    Examples:
    >>> from pyngb import read_ngb
    >>> from pyngb.validation import validate_sta_data
        >>>
        >>> table = read_ngb("sample.ngb-ss3")
        >>> issues = validate_sta_data(table)
        >>>
        >>> if issues:
        ...     print("Validation issues found:")
        ...     for issue in issues:
        ...         print(f"  - {issue}")
        ... else:
        ...     print("Data validation passed!")
    """
    checker = QualityChecker(data, metadata)
    return checker.quick_check()


def check_temperature_profile(
    data: pa.Table | pl.DataFrame,
) -> dict[str, str | float | bool]:
    """Check temperature profile for common issues.

    Args:
        data: STA data

    Returns:
        Dictionary with temperature profile analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "sample_temperature" not in df.columns:
        return {"error": "No sample_temperature column found"}

    temp_data = df.select("sample_temperature").to_numpy().flatten()

    # Handle NaN and infinite values
    temp_data_clean = temp_data[~np.isnan(temp_data) & ~np.isinf(temp_data)]

    if len(temp_data_clean) == 0:
        return {"error": "No valid temperature data (all NaN or infinite)"}

    analysis: dict[str, str | float | bool] = {
        "temperature_range": float(np.ptp(temp_data_clean)),
        "min_temperature": float(np.min(temp_data_clean)),
        "max_temperature": float(np.max(temp_data_clean)),
        "is_monotonic_increasing": bool(np.all(np.diff(temp_data_clean) >= 0)),
        "is_monotonic_decreasing": bool(np.all(np.diff(temp_data_clean) <= 0)),
        "average_rate": float(np.mean(np.diff(temp_data_clean)))
        if len(temp_data_clean) > 1
        else 0.0,
    }

    return analysis


def check_mass_data(
    data: pa.Table | pl.DataFrame,
) -> dict[str, str | float | bool]:
    """Check mass data for quality issues.

    Args:
        data: STA data

    Returns:
        Dictionary with mass data analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "mass" not in df.columns:
        return {"error": "No mass column found"}

    mass_data = df.select("mass").to_numpy().flatten()

    # Handle NaN and infinite values
    mass_data_clean = mass_data[~np.isnan(mass_data) & ~np.isinf(mass_data)]

    if len(mass_data_clean) == 0:
        return {"error": "No valid mass data (all NaN or infinite)"}

    initial_mass = mass_data_clean[0]
    final_mass = mass_data_clean[-1]

    # For thermal analysis, mass change is measured from the zeroed starting point
    mass_change = final_mass - initial_mass

    analysis: dict[str, str | float | bool] = {
        "initial_mass": float(initial_mass),
        "final_mass": float(final_mass),
        "mass_change": float(mass_change),
        "mass_range": float(np.ptp(mass_data_clean)),
        "has_negative_values": bool(np.any(mass_data_clean < 0)),
    }

    return analysis


def _significant_extrema(x: np.ndarray) -> tuple[int, int]:
    """Count local maxima and minima that clearly stand out from the trace.

    Significance is judged against a robust (median/MAD) noise scale so real
    peaks don't inflate their own threshold. An extremum counts only when
    both its prominence and its deviation from the median exceed 5 sigma:
    prominence alone still passes the most extreme excursions of pure noise,
    and deviation alone counts every noise wiggle on top of a broad peak.
    """
    median = float(np.median(x))
    sigma = 1.4826 * float(np.median(np.abs(x - median)))
    if sigma == 0.0:
        # Perfectly flat trace (or >50 % identical values): nothing can be
        # significant relative to it.
        return 0, 0

    threshold = 5.0 * sigma
    maxima, _ = find_peaks(x, prominence=threshold, height=median + threshold)
    # For minima, mirror the trace: -x[i] >= threshold - median <=> x[i] <= median - threshold
    minima, _ = find_peaks(-x, prominence=threshold, height=threshold - median)
    return len(maxima), len(minima)


def check_dsc_data(data: pa.Table | pl.DataFrame) -> dict[str, str | float | int]:
    """Check DSC data for quality issues.

    Args:
        data: STA data

    Returns:
        Dictionary with DSC data analysis
    """
    # Optimize: use helper function to avoid unnecessary conversions
    df = _ensure_polars_dataframe(data)

    if "dsc_signal" not in df.columns:
        return {"error": "No dsc_signal column found"}

    dsc_data = df.select("dsc_signal").to_numpy().flatten()

    # Handle NaN and infinite values
    dsc_data_clean = dsc_data[~np.isnan(dsc_data) & ~np.isinf(dsc_data)]

    if len(dsc_data_clean) == 0:
        return {"error": "No valid DSC data (all NaN or infinite)"}

    peaks_positive, peaks_negative = _significant_extrema(dsc_data_clean)

    analysis: dict[str, str | float | int] = {
        "signal_range": float(np.ptp(dsc_data_clean)),
        "signal_std": float(np.std(dsc_data_clean)),
        "peaks_detected": int(peaks_positive + peaks_negative),
        "positive_peaks": int(peaks_positive),
        "negative_peaks": int(peaks_negative),
        "signal_to_noise": float(
            np.max(np.abs(dsc_data_clean)) / np.std(dsc_data_clean)
        )
        if np.std(dsc_data_clean) > 0
        else 0.0,
    }

    return analysis
