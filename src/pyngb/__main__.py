"""Module entry point for `python -m pyngb`.

This forwards to the canonical CLI implementation in `pyngb.api.loaders.main`.
"""

import sys

from .api.loaders import main

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
