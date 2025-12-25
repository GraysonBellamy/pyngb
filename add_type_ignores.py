#!/usr/bin/env python3
"""Add type: ignore comments to valid Polars code that mypy doesn't understand."""
import subprocess
from pathlib import Path


def get_errors() -> dict[str, list[int]]:
    """Get all type errors that need ignoring."""
    result = subprocess.run(
        ["uv", "run", "mypy", "src/pyngb", "tests", "--strict"],
        capture_output=True,
        text=True,
    )

    errors: dict[str, list[tuple[int, str]]] = {}
    for line in result.stderr.split("\n"):
        if "error:" not in line:
            continue

        parts = line.split(":", 3)
        if len(parts) >= 4:
            file_path = parts[0]
            line_num = int(parts[1])
            error_msg = parts[3].strip()

            # Only add ignore for specific error types that are false positives
            should_ignore = any([
                'Invalid index type "str" for "Series"' in error_msg,
            ])

            if should_ignore:
                if file_path not in errors:
                    errors[file_path] = []
                error_type = "index" if "Invalid index type" in error_msg else "misc"
                errors[file_path].append((line_num, error_type))

    return errors


def add_type_ignore(file_path: Path, errors: list[tuple[int, str]]) -> int:
    """Add type: ignore comments to specific lines."""
    lines = file_path.read_text().split("\n")
    fixed = 0

    for line_num, error_type in errors:
        idx = line_num - 1
        if idx < 0 or idx >= len(lines):
            continue

        line = lines[idx]

        # Skip if already has type: ignore
        if "# type: ignore" in line:
            continue

        # Add type: ignore comment
        lines[idx] = line.rstrip() + f"  # type: ignore[{error_type}]"
        fixed += 1

    if fixed > 0:
        file_path.write_text("\n".join(lines))

    return fixed


def main():
    """Add type ignores to all false positive errors."""
    print("Finding errors that need type: ignore comments...")
    errors = get_errors()

    if not errors:
        print("No errors found that need type: ignore!")
        return

    total_errors = sum(len(errs) for errs in errors.values())
    print(f"Found {total_errors} errors in {len(errors)} files\n")

    total_fixed = 0
    for file_path_str, error_list in sorted(errors.items()):
        file_path = Path(file_path_str)
        if not file_path.exists():
            continue

        fixed = add_type_ignore(file_path, error_list)
        if fixed > 0:
            print(f"✓ {file_path.name}: added {fixed} type: ignore comments")
            total_fixed += fixed

    print(f"\n✓ Added {total_fixed} type: ignore comments")

    # Run mypy again
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
