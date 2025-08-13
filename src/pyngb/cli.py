#!/usr/bin/env python3
"""Deprecated CLI shim for pyngb.

This module is retained for backward compatibility. The canonical CLI entrypoint
is `pyngb.api.loaders.main`. This shim simply forwards to that implementation
and emits a deprecation warning when invoked directly.
"""

from __future__ import annotations

import sys
import warnings


def main() -> int:  # pragma: no cover - thin wrapper
    warnings.warn(
        "pyngb.cli is deprecated; use 'python -m pyngb' or the 'pyngb' command",
        DeprecationWarning,
        stacklevel=2,
    )
    from .api.loaders import main as real_main

    return real_main()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
