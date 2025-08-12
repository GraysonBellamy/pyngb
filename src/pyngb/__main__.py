"""
Command-line interface for pyngb.
"""

import sys

from .api import main

if __name__ == "__main__":
    sys.exit(main())
