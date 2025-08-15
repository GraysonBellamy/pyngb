pyngb Documentation
=======================

pyngb is a Python library for parsing and analyzing NETZSCH STA (Simultaneous Thermal Analysis) data files.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api
   development

Quick Start
-----------

Install pyngb:

.. code-block:: bash

   pip install pyngb

Basic usage:

.. code-block:: python

   from pyngb import read_ngb

   # Load data as PyArrow Table
   table = read_ngb("your_file.ngb-ss3")

   # Get structured data with metadata
   metadata, data = read_ngb("your_file.ngb-ss3", return_metadata=True)

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
