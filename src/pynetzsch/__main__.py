"""
Command-line interface for pynetzsch.
"""

import sys
from .api import main

if __name__ == "__main__":
    sys.exit(main())
