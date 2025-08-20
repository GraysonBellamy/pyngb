"""
Batch processing tools for handling multiple NGB files.
"""

from __future__ import annotations

import logging
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Union
from collections.abc import Callable

import polars as pl
import pyarrow.parquet as pq

from .api.loaders import read_ngb
from .constants import FileMetadata

__all__ = ["BatchProcessor", "NGBDataset", "process_directory", "process_files"]

logger = logging.getLogger(__name__)


def _process_single_file_worker(
    file_path: str,
    output_format: str,
    output_dir: str,
    skip_errors: bool,
) -> dict[str, str | float | None]:
    """Top-level worker function to process a single file (multiprocessing-safe).

    Using a module-level function avoids pickling bound methods and reduces
    fork-related issues with libraries like PyArrow/Polars when using processes.
    """
    import json

    start_time = time.perf_counter()
    file_p = Path(file_path)

    try:
        metadata, data = read_ngb(str(file_p), return_metadata=True)
        base_name = file_p.stem
        out_dir = Path(output_dir)

        if output_format in ("parquet", "both"):
            # Attach metadata to Arrow table and write parquet
            metadata_json = json.dumps(metadata, default=str)
            existing_meta = data.schema.metadata or {}
            new_meta = {**existing_meta, b"file_metadata": metadata_json.encode()}
            table_with_meta = data.replace_schema_metadata(new_meta)
            pq.write_table(
                table_with_meta, out_dir / f"{base_name}.parquet", compression="snappy"
            )

        if output_format in ("csv", "both"):
            # Optimize: Only convert to Polars when needed for CSV output
            df = pl.from_arrow(data)
            if isinstance(df, pl.Series):
                df = pl.DataFrame(df)
            df.write_csv(out_dir / f"{base_name}.csv")

            # Also save metadata JSON alongside
            metadata_path = out_dir / f"{base_name}_metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)

        processing_time = time.perf_counter() - start_time
        return {
            "file": str(file_p),
            "status": "success",
            "rows": data.num_rows,
            "columns": data.num_columns,
            "sample_name": metadata.get("sample_name"),
            "processing_time": processing_time,
            "error": None,
        }

    except Exception as e:
        processing_time = time.perf_counter() - start_time
        if not skip_errors:
            # Re-raise in strict mode so caller can surface the error
            raise
        return {
            "file": str(file_p),
            "status": "error",
            "rows": None,
            "columns": None,
            "sample_name": None,
            "processing_time": processing_time,
            "error": f"{type(e).__name__}: {e!s}",
        }


class BatchProcessor:
    """High-performance batch processing for multiple NGB files.

    Provides parallel processing, progress tracking, error handling, and
    flexible output formats for processing collections of NGB files.

    Examples:
    >>> from pyngb.batch import BatchProcessor
        >>>
        >>> processor = BatchProcessor(max_workers=4)
        >>> results = processor.process_directory("./data/", output_format="parquet")
        >>> print(f"Processed {len(results)} files")
        >>>
        >>> # Custom processing with error handling
        >>> results = processor.process_files(
        ...     file_list,
        ...     output_dir="./output/",
        ...     skip_errors=True
        ... )
    """

    def __init__(self, max_workers: int | None = None, verbose: bool = True):
        """Initialize batch processor.

        Args:
            max_workers: Maximum number of parallel processes (default: CPU count)
            verbose: Whether to show progress information
        """
        self.max_workers = max_workers
        self.verbose = verbose
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging for batch processing without altering global config."""
        if self.verbose and not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    def process_directory(
        self,
        directory: Union[str, Path],
        pattern: str = "*.ngb-ss3",
        output_format: str = "parquet",
        output_dir: Union[str, Path] | None = None,
        skip_errors: bool = True,
    ) -> list[dict[str, str | float | None]]:
        """Process all NGB files in a directory.

        Args:
            directory: Directory containing NGB files
            pattern: File pattern to match (default: "*.ngb-ss3")
            output_format: Output format ("parquet", "csv", "both")
            output_dir: Output directory (default: same as input)
            skip_errors: Whether to continue processing if individual files fail

        Returns:
            List of processing results with status and metadata

        Examples:
            >>> processor = BatchProcessor()
            >>> results = processor.process_directory(
            ...     "./experiments/",
            ...     output_format="both",
            ...     skip_errors=True
            ... )
            >>>
            >>> # Check for errors
            >>> errors = [r for r in results if r['status'] == 'error']
            >>> print(f"Failed to process {len(errors)} files")
        """
        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        # Find all matching files
        files = list(directory.glob(pattern))
        if not files:
            logger.warning(
                f"No files matching pattern '{pattern}' found in {directory}"
            )
            return []

        logger.info(f"Found {len(files)} files to process")

        return self.process_files(
            files,  # type: ignore[arg-type]
            output_format=output_format,
            output_dir=output_dir or directory,
            skip_errors=skip_errors,
        )

    def process_files(
        self,
        files: list[Union[str, Path]],
        output_format: str = "parquet",
        output_dir: Union[str, Path] | None = None,
        skip_errors: bool = True,
    ) -> list[dict[str, str | float | None]]:
        """Process a list of NGB files with parallel execution.

        Args:
            files: List of file paths to process
            output_format: Output format ("parquet", "csv", "both")
            output_dir: Output directory
            skip_errors: Whether to continue if individual files fail

        Returns:
            List of processing results
        """
        if not files:
            return []

        output_dir = Path(output_dir) if output_dir else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)

        results = []
        start_time = time.perf_counter()

        if self.max_workers == 1:
            # Sequential processing for debugging
            for file_path in files:
                result = _process_single_file_worker(
                    str(file_path), output_format, str(output_dir), skip_errors
                )
                results.append(result)
                if self.verbose:
                    self._log_progress(len(results), len(files), start_time)
        else:
            # Parallel processing
            # Use 'spawn' to avoid fork-safety issues with PyArrow/Polars
            with ProcessPoolExecutor(
                max_workers=self.max_workers,
                mp_context=mp.get_context("spawn"),
            ) as executor:
                # Submit all tasks
                future_to_file = {
                    executor.submit(
                        _process_single_file_worker,
                        str(file_path),
                        output_format,
                        str(output_dir),
                        skip_errors,
                    ): str(file_path)
                    for file_path in files
                }

                # Collect results as they complete
                for future in as_completed(future_to_file):
                    src = future_to_file[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        # Convert worker exception into an error record
                        result = {
                            "file": str(src),
                            "status": "error",
                            "rows": None,
                            "columns": None,
                            "sample_name": None,
                            "processing_time": 0.0,
                            "error": f"{type(e).__name__}: {e!s}",
                        }
                        logger.error(f"Failed to process {src}: {e!s}")
                    results.append(result)

                    if self.verbose:
                        self._log_progress(len(results), len(files), start_time)

        self._log_summary(results, start_time)
        return results

    # Note: per-file processing moved to module-level worker to be multiprocessing-safe

    def _log_progress(self, completed: int, total: int, start_time: float) -> None:
        """Log processing progress."""
        if (
            completed % 10 == 0 or completed == total
        ):  # Log every 10 files or at completion
            elapsed = time.perf_counter() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (total - completed) / rate if rate > 0 else 0

            logger.info(
                f"Progress: {completed}/{total} ({completed / total * 100:.1f}%) "
                f"- Rate: {rate:.1f} files/sec - ETA: {eta:.0f}s"
            )

    def _log_summary(self, results: list[dict], start_time: float) -> None:
        """Log processing summary."""
        total_time = time.perf_counter() - start_time
        successful = sum(1 for r in results if r["status"] == "success")
        failed = len(results) - successful

        total_rows = sum(r["rows"] or 0 for r in results if r["rows"])
        avg_rate = len(results) / total_time if total_time > 0 else 0

        logger.info(
            f"Batch processing completed in {total_time:.1f}s:\n"
            f"  ✅ Successful: {successful}\n"
            f"  ❌ Failed: {failed}\n"
            f"  📊 Total rows processed: {total_rows:,}\n"
            f"  ⚡ Average rate: {avg_rate:.1f} files/sec"
        )


class NGBDataset:
    """Dataset management for collections of NGB files.

    Provides high-level operations for managing and analyzing
    collections of NGB files including metadata aggregation,
    summary statistics, and batch operations.

    Examples:
    >>> from pyngb.batch import NGBDataset
        >>>
        >>> # Create dataset from directory
        >>> dataset = NGBDataset.from_directory("./experiments/")
        >>>
        >>> # Get overview
        >>> summary = dataset.summary()
        >>> print(f"Dataset contains {len(dataset)} files")
        >>>
        >>> # Export metadata
        >>> dataset.export_metadata("experiment_summary.csv")
        >>>
        >>> # Filter by criteria
        >>> polymer_samples = dataset.filter_by_metadata(
        ...     lambda meta: 'polymer' in meta.get('material', '').lower()
        ... )
    """

    def __init__(self, files: list[Path]):
        """Initialize dataset with file list.

        Args:
            files: List of NGB file paths
        """
        self.files = files
        self._metadata_cache: dict[str, FileMetadata] = {}

    @classmethod
    def from_directory(
        cls, directory: Union[str, Path], pattern: str = "*.ngb-ss3"
    ) -> NGBDataset:
        """Create dataset from directory.

        Args:
            directory: Directory containing NGB files
            pattern: File pattern to match

        Returns:
            NGBDataset instance
        """
        directory = Path(directory)
        files = list(directory.glob(pattern))
        return cls(files)

    def __len__(self) -> int:
        """Return number of files in dataset."""
        return len(self.files)

    def summary(
        self,
    ) -> dict[str, int | float | list[str] | tuple[float, float] | None]:
        """Generate dataset summary statistics.

        Returns:
            Dictionary with summary information
        """
        if not self.files:
            return {"file_count": 0}

        # Load all metadata (cached)
        all_metadata = []
        for file_path in self.files:
            try:
                metadata = self._get_metadata(file_path)
                all_metadata.append(metadata)
            except Exception as e:
                logger.warning(f"Failed to load metadata for {file_path}: {e}")

        if not all_metadata:
            return {"file_count": len(self.files), "loadable_files": 0}

        # Extract statistics
        instruments = [m.get("instrument", "Unknown") for m in all_metadata]
        operators = [m.get("operator", "Unknown") for m in all_metadata]
        materials = [m.get("material", "Unknown") for m in all_metadata]

        sample_masses = [
            float(mass)
            for m in all_metadata
            if (mass := m.get("sample_mass")) is not None
        ]

        return {
            "file_count": len(self.files),
            "loadable_files": len(all_metadata),
            "unique_instruments": list(set(instruments)),
            "unique_operators": list(set(operators)),
            "unique_materials": list(set(materials)),
            "sample_mass_range": (min(sample_masses), max(sample_masses))
            if sample_masses
            else None,
            "avg_sample_mass": sum(sample_masses) / len(sample_masses)
            if sample_masses
            else None,
        }

    def export_metadata(
        self, output_path: Union[str, Path], format: str = "csv"
    ) -> None:
        """Export metadata for all files.

        Args:
            output_path: Output file path
            format: Output format ("csv", "json", "parquet")
        """
        all_metadata = []

        for file_path in self.files:
            try:
                metadata = self._get_metadata(file_path)
                # Flatten metadata for tabular export
                flat_meta = {
                    "file_path": str(file_path),
                    "file_name": file_path.name,
                    **metadata,
                }
                all_metadata.append(flat_meta)
            except Exception as e:
                logger.warning(f"Failed to load metadata for {file_path}: {e}")
                all_metadata.append(
                    {
                        "file_path": str(file_path),
                        "file_name": file_path.name,
                        "error": str(e),
                    }
                )

        if not all_metadata:
            logger.warning("No metadata to export")
            return

        # Convert to DataFrame for export
        df = pl.DataFrame(all_metadata)

        output_path = Path(output_path)
        if format.lower() == "csv":
            # Flatten nested data for CSV compatibility
            df_flattened = self._flatten_dataframe_for_csv(df)
            df_flattened.write_csv(output_path)
        elif format.lower() == "json":
            df.write_json(output_path)
        elif format.lower() == "parquet":
            df.write_parquet(output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"Exported metadata for {len(all_metadata)} files to {output_path}")

    def _flatten_dataframe_for_csv(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten nested data structures for CSV export compatibility.

        Args:
            df: DataFrame with potentially nested data

        Returns:
            DataFrame with flattened data suitable for CSV export
        """
        import json

        # Create a new dataframe with flattened columns
        flattened_data = []

        for row in df.iter_rows(named=True):
            flattened_row = {}
            for key, value in row.items():
                if isinstance(value, (dict, list)):
                    # Convert nested structures to JSON strings
                    flattened_row[key] = (
                        json.dumps(value) if value is not None else None
                    )
                else:
                    flattened_row[key] = value
            flattened_data.append(flattened_row)

        return pl.DataFrame(flattened_data)

    def filter_by_metadata(
        self, predicate: Callable[[FileMetadata], bool]
    ) -> NGBDataset:
        """Filter dataset by metadata criteria.

        Args:
            predicate: Function that takes metadata dict and returns bool

        Returns:
            New NGBDataset with filtered files
        """
        filtered_files = []

        for file_path in self.files:
            try:
                metadata = self._get_metadata(file_path)
                if predicate(metadata):
                    filtered_files.append(file_path)
            except Exception as e:
                logger.warning(f"Failed to check metadata for {file_path}: {e}")

        return NGBDataset(filtered_files)

    def _get_metadata(self, file_path: Path) -> FileMetadata:
        """Get metadata for file with caching.

        Args:
            file_path: Path to NGB file

        Returns:
            File metadata
        """
        cache_key = str(file_path)

        if cache_key not in self._metadata_cache:
            metadata, _ = read_ngb(str(file_path), return_metadata=True)
            self._metadata_cache[cache_key] = metadata

        return self._metadata_cache[cache_key]


# Convenience functions
def process_directory(
    directory: Union[str, Path],
    pattern: str = "*.ngb-ss3",
    output_format: str = "parquet",
    max_workers: int | None = None,
) -> list[dict[str, str | float | None]]:
    """Process all NGB files in a directory.

    Convenience function for quick batch processing.

    Args:
        directory: Directory containing NGB files
        pattern: File pattern to match
        output_format: Output format ("parquet", "csv", "both")
        max_workers: Maximum parallel processes

    Returns:
        List of processing results

    Examples:
    >>> from pyngb.batch import process_directory
        >>>
        >>> results = process_directory("./data/", output_format="both")
        >>> successful = [r for r in results if r['status'] == 'success']
        >>> print(f"Successfully processed {len(successful)} files")
    """
    processor = BatchProcessor(max_workers=max_workers)
    return processor.process_directory(directory, pattern, output_format)


def process_files(
    files: list[Union[str, Path]],
    output_format: str = "parquet",
    max_workers: int | None = None,
) -> list[dict[str, str | float | None]]:
    """Process a list of NGB files.

    Convenience function for batch processing specific files.

    Args:
        files: List of file paths
        output_format: Output format ("parquet", "csv", "both")
        max_workers: Maximum parallel processes

    Returns:
        List of processing results
    """
    processor = BatchProcessor(max_workers=max_workers)
    return processor.process_files(files, output_format=output_format)
