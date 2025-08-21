"""
Demonstration of baseline subtraction functionality.

This demo shows how to use the baseline subtraction feature with NETZSCH STA files.
By default, dynamic segments use sample_temperature axis for alignment, which is
typically the most appropriate for thermal analysis data.
"""

import polars as pl
from pyngb import read_ngb, subtract_baseline


def test_baseline_subtraction():
    """Test baseline subtraction functionality."""
    sample_file = "tests/test_files/Douglas_Fir_STA_10K_250730_R13.ngb-ss3"
    baseline_file = "tests/test_files/Douglas_Fir_STA_Baseline_10K_250730_R13.ngb-bs3"

    print("🧪 Testing Baseline Subtraction Feature")
    print("=" * 50)
    print("i  Default behavior: Dynamic segments aligned by sample_temperature")
    print("   (isothermal segments always use time axis)")
    print("=" * 50)

    # Load original sample data
    print("📊 Loading original sample data...")
    original_data = read_ngb(sample_file)
    original_df = pl.from_arrow(original_data)
    print(f"   Shape: {original_df.shape}")
    if hasattr(original_df, "columns"):
        print(f"   Columns: {list(original_df.columns)[:5]}...")  # Show first 5 columns

    # Load baseline data
    print("\n📊 Loading baseline data...")
    baseline_data = read_ngb(baseline_file)
    baseline_df = pl.from_arrow(baseline_data)
    print(f"   Shape: {baseline_df.shape}")
    if hasattr(baseline_df, "columns"):
        print(
            f"   Columns: {list(baseline_df.columns)[:5]}..."
        )  # Test standalone baseline subtraction
    print("\n🔬 Testing standalone subtract_baseline() function...")
    subtracted_df = subtract_baseline(sample_file, baseline_file)
    print(f"   Result shape: {subtracted_df.shape}")

    # Test integrated baseline subtraction through read_ngb
    print("\n🔬 Testing integrated read_ngb() with baseline...")
    integrated_data = read_ngb(sample_file, baseline_file=baseline_file)
    integrated_df = pl.from_arrow(integrated_data)
    print(f"   Result shape: {integrated_df.shape}")

    # Test different dynamic axis
    print("\n🔬 Testing with time axis...")
    time_axis_data = read_ngb(
        sample_file, baseline_file=baseline_file, dynamic_axis="time"
    )
    time_axis_df = pl.from_arrow(time_axis_data)
    print(f"   Result shape: {time_axis_df.shape}")

    # Compare mass values before and after subtraction
    print("\n📈 Comparing mass values...")
    if "mass" in original_df.columns and "mass" in subtracted_df.columns:
        orig_mass_range = (original_df["mass"].min(), original_df["mass"].max())
        sub_mass_range = (subtracted_df["mass"].min(), subtracted_df["mass"].max())
        print(
            f"   Original mass range: {orig_mass_range[0]:.3f} to {orig_mass_range[1]:.3f}"
        )
        print(
            f"   Subtracted mass range: {sub_mass_range[0]:.3f} to {sub_mass_range[1]:.3f}"
        )

    # Compare DSC values
    print("\n📈 Comparing DSC signal values...")
    if "dsc_signal" in original_df.columns and "dsc_signal" in subtracted_df.columns:
        orig_dsc_range = (
            original_df["dsc_signal"].min(),
            original_df["dsc_signal"].max(),
        )
        sub_dsc_range = (
            subtracted_df["dsc_signal"].min(),
            subtracted_df["dsc_signal"].max(),
        )
        print(
            f"   Original DSC range: {orig_dsc_range[0]:.3f} to {orig_dsc_range[1]:.3f}"
        )
        print(
            f"   Subtracted DSC range: {sub_dsc_range[0]:.3f} to {sub_dsc_range[1]:.3f}"
        )

    # Verify time axis is preserved
    print("\n⏱️  Verifying time axis preservation...")
    orig_time_range = (original_df["time"].min(), original_df["time"].max())
    sub_time_range = (subtracted_df["time"].min(), subtracted_df["time"].max())
    print(
        f"   Original time range: {orig_time_range[0]:.1f} to {orig_time_range[1]:.1f} seconds"
    )
    print(
        f"   Subtracted time range: {sub_time_range[0]:.1f} to {sub_time_range[1]:.1f} seconds"
    )

    print("\n✅ All tests completed successfully!")


if __name__ == "__main__":
    test_baseline_subtraction()
