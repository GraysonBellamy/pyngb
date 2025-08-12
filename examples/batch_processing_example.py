#!/usr/bin/env python3
"""
Example: Batch Processing and Data Validation

This example demonstrates how to use the new batch processing and data validation
features in pyNGB to handle multiple NGB files efficiently.
"""


def main():
    """Demonstrate batch processing and validation capabilities."""

    # Example 1: Quick validation of a single file
    print("=== Example 1: Quick Data Validation ===")

    # In a real scenario, you would have actual NGB files
    # For this example, we'll demonstrate the API structure
    print("# Quick validation function")
    print("from pyngb import validate_sta_data, load_ngb_data")
    print("table = load_ngb_data('sample.ngb-ss3')")
    print("issues = validate_sta_data(table)")
    print("if issues:")
    print("    print('Validation issues found:')")
    print("    for issue in issues:")
    print("        print(f'  - {issue}')")
    print("else:")
    print("    print('✅ Data validation passed!')")
    print()

    # Example 2: Comprehensive quality checking
    print("=== Example 2: Comprehensive Quality Checking ===")
    print("# Detailed quality assessment")
    print("from pyngb import QualityChecker")
    print("checker = QualityChecker(table)")
    print("result = checker.full_validation()")
    print("print(result.report())")
    print()

    # Example 3: Batch processing setup
    print("=== Example 3: Batch Processing ===")
    print("# Process multiple files in parallel")
    print("from pyngb import BatchProcessor")
    print("processor = BatchProcessor(")
    print("    max_workers=4,")
    print("    output_format='parquet',")
    print("    progress_callback=lambda current, total, filename: ")
    print("        print(f'Processing {current}/{total}: {filename}')")
    print(")")
    print()
    print("# Process all NGB files in a directory")
    print("results = processor.process_directory(")
    print("    '/path/to/ngb/files',")
    print("    output_dir='/path/to/output',")
    print("    pattern='*.ngb-ss3'")
    print(")")
    print()
    print("# Process specific files")
    print("file_list = ['file1.ngb-ss3', 'file2.ngb-ss3', 'file3.ngb-ss3']")
    print("results = processor.process_files(file_list, output_dir='/path/to/output')")
    print()

    # Example 4: Dataset management
    print("=== Example 4: Dataset Management ===")
    print("# Create and manage datasets")
    print("from pyngb import NGBDataset")
    print("dataset = NGBDataset()")
    print("dataset.add_directory('/path/to/ngb/files')")
    print("print(f'Dataset contains {len(dataset)} files')")
    print()
    print("# Get dataset summary")
    print("summary = dataset.get_summary()")
    print("print(f'Unique instruments: {summary[\"unique_instruments\"]}')")
    print("print(f'Temperature range: {summary[\"temp_range\"]}')")
    print()
    print("# Filter dataset")
    print("filtered = dataset.filter_by_instrument('STA 449 F3')")
    print("print(f'Filtered dataset has {len(filtered)} files')")
    print()
    print("# Export dataset")
    print("dataset.to_parquet('/path/to/dataset.parquet')")
    print()

    # Example 5: Real workflow example
    print("=== Example 5: Complete Workflow ===")
    print("# Complete batch processing and validation workflow")
    print("""
# 1. Set up batch processor with validation
processor = BatchProcessor(
    max_workers=4,
    validate_data=True,  # Enable validation during processing
    output_format='both'  # Save as both CSV and Parquet
)

# 2. Process directory with progress tracking
def progress_callback(current, total, filename):
    percentage = (current / total) * 100
    print(f'Progress: {percentage:.1f}% - {filename}')

results = processor.process_directory(
    input_dir='./data/raw_files',
    output_dir='./data/processed',
    pattern='*.ngb-ss3',
    progress_callback=progress_callback
)

# 3. Check processing results
print(f'Successfully processed: {results[\"successful\"]} files')
print(f'Failed to process: {len(results[\"failed\"])} files')

if results['failed']:
    print('Failed files:')
    for file_path, error in results['failed'].items():
        print(f'  {file_path}: {error}')

# 4. Create dataset for analysis
dataset = NGBDataset()
dataset.add_directory('./data/processed')

# 5. Generate dataset report
summary = dataset.get_summary()
print(f'Dataset summary:')
print(f'  Files: {summary[\"file_count\"]}')
print(f'  Instruments: {summary[\"unique_instruments\"]}')
print(f'  Temperature range: {summary[\"temp_range\"]}°C')
print(f'  Time range: {summary[\"time_range\"]} min')

# 6. Export consolidated dataset
dataset.to_parquet('./data/consolidated_dataset.parquet')
print('✅ Workflow completed successfully!')
""")


if __name__ == "__main__":
    main()
