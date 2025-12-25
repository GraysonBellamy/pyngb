"""File hashing utilities for pyNGB."""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_hash(path: str | Path, max_size_mb: int = 1000) -> str | None:
    """Generate file hash for metadata.

    Args:
        path: Path to the file to hash
        max_size_mb: Maximum file size in MB to hash (default: 1000MB)

    Returns:
        BLAKE2b hash as hex string, or None if hashing fails

    Raises:
        OSError: If there are file system related errors
        PermissionError: If file access is denied
    """
    path = Path(path)
    try:
        # Pre-flight: ensure blake2b constructor is callable. If a hashing backend
        # failure occurs (e.g., during unit tests that patch blake2b to raise),
        # surface it as an unexpected error per contract.
        try:
            _ = hashlib.blake2b()
        except (
            RuntimeError,
            AttributeError,
            TypeError,
        ) as e:  # pragma: no cover - exercised in tests via patch
            # Specific exceptions that can occur during hash algorithm initialization
            logger.error(f"Hash algorithm unavailable for file {path}: {e}")
            return None
        except Exception as e:  # pragma: no cover - exercised in tests via patch
            # Catch any other unexpected exceptions during hash initialization
            logger.error(f"Unexpected error while generating hash for file {path}: {e}")
            return None
        # Check file size before hashing
        file_size = path.stat().st_size
        max_size_bytes = max_size_mb * 1024 * 1024

        if file_size > max_size_bytes:
            logger.warning(
                f"File too large for hashing ({file_size // (1024 * 1024)} MB > {max_size_mb} MB): {path}"
            )
            return None

        with path.open("rb") as file:
            return hashlib.blake2b(file.read()).hexdigest()
    except FileNotFoundError:
        logger.warning(f"File not found while generating hash: {path}")
        return None
    except PermissionError:
        logger.error(f"Permission denied while generating hash for file: {path}")
        return None
    except OSError as e:
        logger.error(f"OS error while generating hash for file {path}: {e}")
        return None
    except (RuntimeError, MemoryError) as e:
        # Handle unexpected runtime issues (e.g., memory exhausted, hash computation failed)
        logger.error(f"Runtime error while generating hash for file {path}: {e}")
        return None
