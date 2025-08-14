#!/usr/bin/env python3
"""
Performance benchmarks for pyNGB library.

Usage:
    python benchmarks.py [--ngb-file path/to/file.ngb-ss3]
"""

import argparse
import time
import tracemalloc
from pathlib import Path
from typing import Any

import psutil

from pyngb import read_ngb


def benchmark_parsing(ngb_file: str, runs: int = 5) -> dict[str, Any]:
    """Benchmark parsing performance."""
    print(f"Benchmarking parsing performance with {runs} runs...")

    results = {
        "parse_times": [],
        "memory_peaks": [],
        "file_size": Path(ngb_file).stat().st_size,
        "rows": 0,
        "columns": 0,
    }

    for run in range(runs):
        # Track memory usage
        tracemalloc.start()
        process = psutil.Process()
        memory_before = process.memory_info().rss

        # Time parsing
        start_time = time.perf_counter()
        table = read_ngb(ngb_file)
        end_time = time.perf_counter()

        # Measure memory
        memory_after = process.memory_info().rss
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        parse_time = end_time - start_time
        memory_delta = memory_after - memory_before

        results["parse_times"].append(parse_time)
        results["memory_peaks"].append(peak)

        if run == 0:  # Store table info on first run
            results["rows"] = table.num_rows
            results["columns"] = len(table.column_names)

        print(
            f"Run {run + 1}: {parse_time:.3f}s, Memory: {memory_delta / 1024 / 1024:.1f}MB"
        )

    return results


def benchmark_memory_efficiency(ngb_file: str) -> dict[str, Any]:
    """Benchmark memory efficiency."""
    print("\nBenchmarking memory efficiency...")

    tracemalloc.start()
    process = psutil.Process()

    # Baseline memory
    baseline_memory = process.memory_info().rss

    # Load data
    table = read_ngb(ngb_file)

    # Peak memory
    current, peak = tracemalloc.get_traced_memory()
    loaded_memory = process.memory_info().rss

    # Calculate efficiency metrics
    file_size = Path(ngb_file).stat().st_size
    memory_ratio = (loaded_memory - baseline_memory) / file_size

    tracemalloc.stop()

    return {
        "file_size_mb": file_size / 1024 / 1024,
        "memory_used_mb": (loaded_memory - baseline_memory) / 1024 / 1024,
        "memory_ratio": memory_ratio,
        "rows": table.num_rows,
        "columns": len(table.column_names),
        "bytes_per_row": (loaded_memory - baseline_memory) / table.num_rows,
    }


def print_results(parse_results: dict[str, Any], memory_results: dict[str, Any]):
    """Print benchmark results."""
    print("\n" + "=" * 60)
    print("PERFORMANCE BENCHMARK RESULTS")
    print("=" * 60)

    # Parsing performance
    times = parse_results["parse_times"]
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"File size: {parse_results['file_size'] / 1024 / 1024:.1f} MB")
    print(f"Data: {parse_results['rows']:,} rows x {parse_results['columns']} columns")
    print("")
    print(f"Parse time (avg): {avg_time:.3f}s")
    print(f"Parse time (min): {min_time:.3f}s")
    print(f"Parse time (max): {max_time:.3f}s")
    print(f"Throughput: {parse_results['rows'] / avg_time:,.0f} rows/sec")

    # Memory efficiency
    print("")
    print("Memory efficiency:")
    print(f"  Peak memory: {memory_results['memory_used_mb']:.1f} MB")
    print(f"  Memory ratio: {memory_results['memory_ratio']:.1f}x file size")
    print(f"  Bytes per row: {memory_results['bytes_per_row']:.0f}")

    # Performance rating
    rows_per_sec = parse_results["rows"] / avg_time
    if rows_per_sec > 50000:
        rating = "Excellent"
    elif rows_per_sec > 10000:
        rating = "Good"
    elif rows_per_sec > 5000:
        rating = "Fair"
    else:
        rating = "Needs optimization"

    print("")
    print(f"Performance rating: {rating}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark pyngb performance")
    parser.add_argument(
        "--ngb-file",
        default="tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3",
        help="Path to NGB file for benchmarking",
    )
    parser.add_argument("--runs", type=int, default=5, help="Number of benchmark runs")

    args = parser.parse_args()

    ngb_file = args.ngb_file
    if not Path(ngb_file).exists():
        print(f"Error: File not found: {ngb_file}")
        return 1

    print(f"Benchmarking with file: {ngb_file}")

    # Run benchmarks
    parse_results = benchmark_parsing(ngb_file, args.runs)
    memory_results = benchmark_memory_efficiency(ngb_file)

    # Print results
    print_results(parse_results, memory_results)

    return 0


if __name__ == "__main__":
    exit(main())
