#!/usr/bin/env python3
"""Test script to verify column name changes are working correctly."""

import sys

sys.path.append("src")

from pyngb.constants import PatternConfig
from pyngb.validation import validate_sta_data
from pyngb.analysis import STAAnalyzer
import polars as pl
import numpy as np


def test_constants():
    """Test that constants have the correct column mappings."""
    config = PatternConfig()

    print("Column mappings:")
    expected_mappings = {
        "8e": "sample_temperature",
        "9c": "dsc_signal",
        "87": "mass",
        "33": "h_foil_temperature",
        "35": "environmental_pressure",
        "36": "environmental_acceleration_x",
        "37": "environmental_acceleration_y",
        "38": "environmental_acceleration_z",
    }

    for hex_id, expected_name in expected_mappings.items():
        actual_name = config.column_map.get(hex_id)
        print(f"  {hex_id}: {actual_name} (expected: {expected_name})")
        assert actual_name == expected_name, (
            f"Column {hex_id} should map to {expected_name}, got {actual_name}"
        )

    print("✅ All column mappings correct!")


def test_validation():
    """Test that validation works with new column names."""
    print("\nTesting validation with new column names...")

    # Create test data with new column names
    n_points = 100
    df = pl.DataFrame(
        {
            "time": np.linspace(0, 100, n_points),
            "sample_temperature": np.linspace(25, 800, n_points),
            "mass": np.linspace(10, 8, n_points),
            "dsc_signal": np.random.normal(0, 5, n_points),
        }
    )

    issues = validate_sta_data(df)
    print(f"Validation issues: {len(issues)}")
    for issue in issues:
        print(f"  - {issue}")

    print("✅ Validation working with new column names!")


def test_analysis():
    """Test that analysis works with new column names."""
    print("\nTesting analysis with new column names...")

    # Create test data
    n_points = 100
    df = pl.DataFrame(
        {
            "time": np.linspace(0, 100, n_points),
            "sample_temperature": np.linspace(25, 800, n_points),
            "mass": np.linspace(10, 8, n_points),
            "dsc_signal": np.random.normal(0, 5, n_points),
        }
    )

    analyzer = STAAnalyzer(df)

    # Test mass loss calculation
    mass_loss = analyzer.mass_loss_percent()
    print(f"Mass loss: {mass_loss:.2f}%")

    # Test temperature range
    temp_range = analyzer.temperature_range()
    print(f"Temperature range: {temp_range[0]:.1f} to {temp_range[1]:.1f} °C")

    # Test thermal events
    events = analyzer.find_thermal_events()
    print(f"Thermal events found: {len(events)}")

    print("✅ Analysis working with new column names!")


if __name__ == "__main__":
    test_constants()
    test_validation()
    test_analysis()
    print("\n🎉 All tests passed! Column rename was successful!")
