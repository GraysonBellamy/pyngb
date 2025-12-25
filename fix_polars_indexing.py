#!/usr/bin/env python3
"""Fix Polars Series indexing type errors by adding type: ignore comments."""
import re
import subprocess
from pathlib import Path


def get_polars_indexing_errors() -> dict[str, list[int]]:
    """Get all Polars Series indexing errors from mypy."""
    result = subprocess.run(
        ["uv", "run", "mypy", "src/pyngb", "tests", "--strict"],
        capture_output=True,
        text=True,
    )

    errors: dict[str, list[int]] = {}
    for line in result.stderr.split("\n"):
        if 'Invalid index type "str" for "Series"' in line:
            parts = line.split(":")
            if len(parts) >= 2:
                file_path = parts[0]
                line_num = int(parts[1])
                if file_path not in errors:
                    errors[file_path] = []
                errors[file_path].append(line_num)

    return errors


def fix_file(file_path: Path, error_lines: list[int]) -> int:
    """Add type: ignore comments to problematic lines."""
    lines = file_path.read_text().split("\n")
    fixed = 0

    for line_num in error_lines:
        # Line numbers are 1-indexed
        idx = line_num - 1
        if idx < 0 or idx >= len(lines):
            continue

        line = lines[idx]

        # Check if it already has a type: ignore comment
        if "# type: ignore" in line:
            continue

        # Add type: ignore[index] at the end of the line
        # Remove any existing trailing whitespace/comments first
        if "#" in line and "# type: ignore" not in line:
            # Has a comment already, insert before it
            parts = line.split("#", 1)
            lines[idx] = parts[0].rstrip() + "  # type: ignore[index]  #" + parts[1]
        else:
            lines[idx] = line.rstrip() + "  # type: ignore[index]"
        fixed += 1

    if fixed > 0:
        file_path.write_text("\n".join(lines))

    return fixed


def main():
    """Fix all Polars indexing errors."""
    print("Finding Polars Series indexing errors...")
    errors = get_polars_indexing_errors()

    if not errors:
        print("No Polars indexing errors found!")
        return

    total_errors = sum(len(lines) for lines in errors.values())
    print(f"Found {total_errors} errors in {len(errors)} files\n")

    total_fixed = 0
    for file_path_str, error_lines in sorted(errors.items()):
        file_path = Path(file_path_str)
        if not file_path.exists():
            continue

        fixed = fix_file(file_path, error_lines)
        if fixed > 0:
            print(f"✓ {file_path.name}: fixed {fixed} lines")
            total_fixed += fixed

    print(f"\n✓ Fixed {total_fixed} Polars indexing errors")

    # Run mypy again to check progress
    print("\nRunning mypy again...")
    result = subprocess.run(
        ["uv", "run", "mypy", "src/pyngb", "tests", "--strict"],
        capture_output=True,
        text=True,
    )
    error_lines = [line for line in result.stderr.split("\n") if "error:" in line]
    print(f"Remaining errors: {len(error_lines)}")


if __name__ == "__main__":
    main()
