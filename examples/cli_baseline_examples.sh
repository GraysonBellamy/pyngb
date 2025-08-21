#!/bin/bash

# CLI Examples for pyngb Baseline Subtraction
# This script demonstrates various ways to use the command line interface

echo "=== pyngb CLI Examples for Baseline Subtraction ==="
echo

# Example 1: Basic baseline subtraction
echo "1. Basic baseline subtraction with default settings:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3"
echo "   → Creates: sample_baseline_subtracted.parquet"
echo

# Example 2: Custom output directory
echo "2. Baseline subtraction with custom output directory:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -o ./corrected_data/"
echo "   → Creates: ./corrected_data/sample_baseline_subtracted.parquet"
echo

# Example 3: Different dynamic axis
echo "3. Baseline subtraction using time axis for dynamic segments:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time"
echo "   → Uses time axis instead of default sample_temperature"
echo

# Example 4: All output formats
echo "4. Generate both Parquet and CSV outputs:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -f all"
echo "   → Creates both .parquet and .csv files"
echo

# Example 5: Verbose logging
echo "5. Baseline subtraction with detailed logging:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 -v"
echo "   → Shows detailed processing information"
echo

# Example 6: Batch processing with shell loop
echo "6. Process multiple files (bash):"
echo "for file in *.ngb-ss3; do"
echo "    baseline=\"\${file%.ngb-ss3}_baseline.ngb-bs3\""
echo "    if [[ -f \"\$baseline\" ]]; then"
echo "        python -m pyngb \"\$file\" -b \"\$baseline\" -o ./results/"
echo "    fi"
echo "done"
echo

# Example 7: Advanced usage with different axes
echo "7. Compare different dynamic axes:"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis sample_temperature -o ./temp_axis/"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis time -o ./time_axis/"
echo "python -m pyngb sample.ngb-ss3 -b baseline.ngb-bs3 --dynamic-axis furnace_temperature -o ./furnace_axis/"
echo

# Example 8: Error handling
echo "8. The CLI provides helpful error messages:"
echo "python -m pyngb nonexistent.ngb-ss3 -b baseline.ngb-bs3"
echo "   → Error: Input file does not exist"
echo
echo "python -m pyngb sample.ngb-ss3 -b nonexistent.ngb-bs3"
echo "   → Error: Baseline file does not exist"
echo

echo "=== Additional CLI Options ==="
echo "python -m pyngb --help    # Show all available options"
echo "python -m pyngb sample.ngb-ss3    # Process without baseline (normal mode)"
echo
echo "=== Output File Naming ==="
echo "Without baseline: input_name.{format}"
echo "With baseline:    input_name_baseline_subtracted.{format}"
echo

echo "For more information, see the documentation at:"
echo "https://graysonbellamy.github.io/pyngb/"
