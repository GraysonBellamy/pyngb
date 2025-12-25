"""Module entry point for `python -m pyngb`.

This forwards to the canonical CLI implementation in `pyngb.api.cli.main`.
"""

import sys

from .api.cli import main

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
