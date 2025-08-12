PyNetzsch Documentation
=======================

PyNetzsch is a Python library for parsing and analyzing NETZSCH STA (Simultaneous Thermal Analysis) data files.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api
   development

Quick Start
-----------

Install PyNetzsch:

.. code-block:: bash

   pip install pynetzsch

Basic usage:

.. code-block:: python

   from pynetzsch import load_ngb_data, get_sta_data

   # Load data as PyArrow Table
   table = load_ngb_data("your_file.ngb-ss3")

   # Get structured data with metadata
   metadata, data = get_sta_data("your_file.ngb-ss3")

Features
--------

* Parse NETZSCH .ngb-ss3 files
* Extract metadata and measurement data
* Export to multiple formats (Parquet, CSV, JSON)
* Command-line interface for batch processing
* Type-safe with modern Python features

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
