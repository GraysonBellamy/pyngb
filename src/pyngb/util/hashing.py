"""File hashing utilities for pyNGB."""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def get_hash(path: str | Path, max_size_mb: int = 1000) -> str | None:
    """Generate a BLAKE2b file hash for metadata.

    The file is read in 1 MiB chunks, so memory use stays flat regardless of
    file size. The hash is optional provenance metadata and must never fail a
    parse: any failure (missing file, permissions, oversized file, broken
    hashlib backend) is logged and reported as None rather than raised.

    Args:
        path: Path to the file to hash
        max_size_mb: Maximum file size in MB to hash (default: 1000MB)

    Returns:
        BLAKE2b hash as hex string, or None if hashing fails
    """
    path = Path(path)
    try:
        file_size = path.stat().st_size
        max_size_bytes = max_size_mb * 1024 * 1024

        if file_size > max_size_bytes:
            logger.warning(
                f"File too large for hashing ({file_size // (1024 * 1024)} MB > {max_size_mb} MB): {path}"
            )
            return None

        digest = hashlib.blake2b()
        with path.open("rb") as file:
            while chunk := file.read(_CHUNK_SIZE):
                digest.update(chunk)
        return digest.hexdigest()
    except FileNotFoundError:
        logger.warning(f"File not found while generating hash: {path}")
        return None
    except Exception as e:
        logger.error(f"Failed to generate hash for file {path}: {e}")
        return None
