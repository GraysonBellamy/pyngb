Quick Start Guide
=================

This guide will help you get started with pyngb quickly.

Loading Data
------------

pyngb provides a simple function for loading data:

.. code-block:: python

   from pyngb import read_ngb

   # Method 1: Load as PyArrow Table (recommended for large datasets)
   table = read_ngb("sample.ngb-ss3")
   print(f"Loaded {table.num_rows} rows with {len(table.column_names)} columns")

   # Method 2: Get structured data with metadata
   metadata, data = read_ngb("sample.ngb-ss3", return_metadata=True)
   print(f"Sample: {metadata.get('sample_name', 'Unknown')}")

Working with Data
-----------------

Convert to different formats:

.. code-block:: python

   import polars as pl

   # Convert to Polars DataFrame
   df = pl.from_arrow(table)

   # Basic analysis
   print(df.describe())

   # Save to file
   df.write_parquet("output.parquet")
   df.write_csv("output.csv")

Command Line Usage
------------------

pyngb includes a command-line interface:

.. code-block:: bash

   # Convert a single file
   python -m pyngb input.ngb-ss3 -f parquet

   # Process multiple files
   for f in *.ngb-ss3; do python -m pyngb "$f" -f all -o ./results/; done

   # Get help
   python -m pyngb --help

Common Use Cases
----------------

**Data Exploration:**

.. code-block:: python

   # Load and explore
   table = read_ngb("sample.ngb-ss3")
   df = pl.from_arrow(table)

   # Check available columns
   print(df.columns)

   # Plot temperature vs time
   if 'time' in df.columns and 'sample_temperature' in df.columns:
       import matplotlib.pyplot as plt
      plt.plot(df['time'], df['sample_temperature'])
       plt.xlabel('Time (s)')
      plt.ylabel('Sample Temperature (°C)')
       plt.show()

**Batch Processing:**

.. code-block:: python

   from pathlib import Path

   # Process all files in a directory
   data_dir = Path("./sta_files")
   results = []

   for file in data_dir.glob("*.ngb-ss3"):
      try:
         metadata, data = read_ngb(str(file), return_metadata=True)
           results.append({
               'filename': file.name,
               'sample_name': metadata.get('sample_name'),
               'data_points': data.num_rows
           })
       except Exception as e:
           print(f"Error processing {file}: {e}")

   # Create summary
   summary_df = pl.DataFrame(results)
   summary_df.write_csv("processing_summary.csv")
