#!/usr/bin/env python3
"""
Example: Basic NGB File Parsing

This example demonstrates the fundamental operations for parsing NGB files
and accessing the extracted data and metadata.

Requirements:
    - pyngb
    - polars (automatically installed with pyngb)

Usage:
    python basic_parsing.py [path_to_ngb_file]
"""

import json
import sys
from pathlib import Path

import polars as pl

from pyngb import read_ngb


def demonstrate_basic_parsing(file_path: str):
    """Demonstrate basic NGB file parsing operations."""

    print(f"🔬 Parsing NGB file: {file_path}")
    print("=" * 60)

    # Method 1: Load as PyArrow table (recommended for most users)
    print("\n📊 Method 1: Using read_ngb()")
    print("-" * 40)

    try:
        table = read_ngb(file_path)

        # Display basic information
        print(f"Data shape: {table.num_rows} rows x {table.num_columns} columns")
        print(f"Columns: {table.column_names}")

        # Extract embedded metadata
        if b"file_metadata" in table.schema.metadata:
            metadata_json = table.schema.metadata[b"file_metadata"].decode()
            metadata = json.loads(metadata_json)

            print("\n📋 Sample Information:")
            print(f"  Sample name: {metadata.get('sample_name', 'Unknown')}")
            print(f"  Instrument: {metadata.get('instrument', 'Unknown')}")
            print(f"  Operator: {metadata.get('operator', 'Unknown')}")
            print(f"  Sample mass: {metadata.get('sample_mass', 'Unknown')} mg")

        # Convert to Polars DataFrame for analysis
        df = pl.from_arrow(table)

        # Display data preview
        print("\n📈 Data Preview:")
        print(df.head(5))

        # Basic statistics
        if df.height > 0:
            print("\n📊 Basic Statistics:")

            # Time information
            if "time" in df.columns:
                _time_stats = df.select("time").describe()
                print(
                    f"  Time range: {df['time'].min():.1f} to {df['time'].max():.1f} seconds"
                )
                print(
                    f"  Duration: {(df['time'].max() - df['time'].min()) / 60:.1f} minutes"
                )

            # Temperature information
            temp_cols = [col for col in df.columns if "temperature" in col.lower()]
            for temp_col in temp_cols:
                temp_min = df[temp_col].min()
                temp_max = df[temp_col].max()
                print(f"  {temp_col}: {temp_min:.1f} to {temp_max:.1f} °C")

            # Mass information
            if "mass" in df.columns:
                initial_mass = df["mass"].max()
                final_mass = df["mass"].min()
                mass_loss = ((initial_mass - final_mass) / initial_mass) * 100
                print(
                    f"  Mass loss: {mass_loss:.2f}% ({initial_mass:.3f} → {final_mass:.3f} mg)"
                )

    except Exception as e:
        print(f"❌ Error with read_ngb(): {e}")
        return False

    # Method 2: Get separate metadata and data objects
    print("\n\n📋 Method 2: Using read_ngb(return_metadata=True)")
    print("-" * 40)

    try:
        metadata, data = read_ngb(file_path, return_metadata=True)

        print(f"Data shape: {data.num_rows} rows x {data.num_columns} columns")
        print(f"Metadata fields: {len(metadata)} fields")

        # Display metadata categories
        print("\n📝 Available Metadata:")
        metadata_categories = {
            "Sample Info": ["sample_name", "sample_mass", "material", "sample_id"],
            "Instrument": ["instrument", "application_version", "licensed_to"],
            "Experimental": ["operator", "date_performed", "lab", "project"],
            "Method": ["crucible_type", "furnace_type", "carrier_type"],
            "Flows": ["purge_1_mfc_gas", "purge_2_mfc_gas", "protective_mfc_gas"],
        }

        for category, fields in metadata_categories.items():
            available_fields = [field for field in fields if field in metadata]
            if available_fields:
                print(f"  {category}:")
                for field in available_fields:
                    value = metadata[field]
                    if isinstance(value, (int, float)) and field.endswith("_mass"):
                        print(f"    {field}: {value} mg")
                    else:
                        print(f"    {field}: {value}")

        # Temperature program information
        if "temperature_program" in metadata:
            temp_prog = metadata["temperature_program"]
            print("\n🌡️  Temperature Program:")
            print(f"    Steps: {len(temp_prog)}")
            for step_name, step_data in temp_prog.items():
                if isinstance(step_data, dict):
                    temp = step_data.get("temperature", "Unknown")
                    rate = step_data.get("heating_rate", "Unknown")
                    print(f"    {step_name}: {temp}°C at {rate}°C/min")

        return True

    except Exception as e:
        print(f"❌ Error with read_ngb(return_metadata=True): {e}")
        return False


def main():
    """Main function to run the example."""

    print("🔬 pyngb Basic Parsing Example")
    print("=" * 60)
    print("This example demonstrates basic NGB file parsing operations.")
    print()

    # Get file path from command line or use default
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Look for test files in common locations
        possible_files = [
            "sample.ngb-ss3",
            "test.ngb-ss3",
            "../tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
            "tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
        ]

        file_path = None
        for possible_file in possible_files:
            if Path(possible_file).exists():
                file_path = possible_file
                break

        if not file_path:
            print("❌ No NGB file found!")
            print()
            print("Usage:")
            print("  python basic_parsing.py path/to/your/file.ngb-ss3")
            print()
            print("Or place a sample file in one of these locations:")
            for pf in possible_files:
                print(f"  - {pf}")
            return 1

    # Check if file exists
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return 1

    # Run the demonstration
    success = demonstrate_basic_parsing(file_path)

    if success:
        print("\n✅ Basic parsing demonstration completed successfully!")
        print("\n💡 Next Steps:")
        print("  - Try the batch_processing.py example for multiple files")
        print("  - Explore validation with QualityChecker in docs")
    else:
        print("\n❌ Demonstration encountered errors.")
        print("💡 Troubleshooting:")
        print("  - Ensure the file is a valid NGB file")
        print("  - Check the troubleshooting guide in the documentation")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
