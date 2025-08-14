Development
===========

This page contains information for developers contributing to pyngb.

Setting Up Development Environment
----------------------------------

1. Clone the repository:

.. code-block:: bash

   git clone https://github.com/GraysonBellamy/pyngb.git
   cd pyngb

2. Install with development dependencies:

.. code-block:: bash

   pip install -e .[dev]

3. Install pre-commit hooks:

.. code-block:: bash

   pre-commit install

Running Tests
-------------

Run the full test suite:

.. code-block:: bash

   pytest

Run tests with coverage:

.. code-block:: bash

   pytest --cov=pyngb --cov-report=html

Code Quality
------------

The project uses several tools for code quality:

* **Ruff**: Linting and formatting
* **mypy**: Type checking
* **Bandit**: Security scanning
* **Safety**: Dependency vulnerability scanning

Run quality checks:

.. code-block:: bash

   # Linting and formatting
   ruff check src/ tests/
   ruff format src/ tests/

   # Type checking
   mypy src/pyngb

   # Security scanning
   bandit -r src/
   safety scan

Building Documentation
----------------------

Build the documentation locally:

.. code-block:: bash

   cd docs
   make html

The documentation will be available in ``docs/_build/html/``.

Project Structure
-----------------

.. code-block:: text

   pyngb/
   ├── src/pyngb/              # Main package
   │   ├── api/               # High-level API
   │   ├── binary/            # Binary parsing
   │   ├── core/              # Core parsing logic
   │   ├── extractors/        # Data extraction
   │   └── ...
   ├── tests/                 # Test suite
   ├── docs/                  # Documentation
   ├── benchmarks.py          # Performance benchmarks
   └── pyproject.toml         # Project configuration

Contributing Guidelines
-----------------------

1. **Fork** the repository
2. **Create** a feature branch
3. **Write** tests for new functionality
4. **Ensure** all tests pass and code quality checks pass
5. **Submit** a pull request

All contributions should:

* Follow the existing code style (enforced by Ruff)
* Include appropriate tests
* Update documentation as needed
* Pass all CI/CD checks

Release Process
---------------

Releases are managed through GitHub Actions:

1. Update version in ``src/pyngb/__about__.py``
2. Create a Git tag: ``git tag v0.2.0``
3. Push tag: ``git push origin v0.2.0``
4. GitHub Actions will automatically build and publish to PyPI

Performance Considerations
--------------------------

When working with large files:

* Use ``read_ngb()`` for PyArrow Tables (more memory efficient)
* Consider processing files in chunks for very large datasets
* Use Parquet format for intermediate storage (faster than CSV)

The benchmarking script can help identify performance regressions:

.. code-block:: bash

   python benchmarks.py --runs 5
