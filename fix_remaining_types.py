#!/usr/bin/env python3
"""Fix remaining specific type errors."""
import re
import subprocess
from pathlib import Path


def run_mypy() -> list[str]:
    """Run mypy and return error lines."""
    result = subprocess.run(
        ["uv", "run", "mypy", "tests", "--strict"],
        capture_output=True,
        text=True,
    )
    return [line for line in result.stderr.split("\n") if "error:" in line]


def fix_series_indexing(file_path: Path) -> int:
    """Fix Series indexing errors by using .loc accessor."""
    content = file_path.read_text()
    original = content

    # Pattern: series["string_key"] -> series.loc["string_key"]
    # But be careful not to change dictionary access
    lines = content.split("\n")
    modified_lines = []

    for line in lines:
        # Look for patterns like: variable["string"] where variable is likely a Series
        # This is a heuristic - we'll check for common Series variable names
        if '["' in line and any(
            name in line
            for name in ["result", "data", "df", "series", "column", "row", "baseline"]
        ):
            # Replace ["str"] with .loc["str"] for Series access
            # But only if it looks like it's accessing a pandas object
            if re.search(r'\b(result|data|df|series|column|row|baseline)\["', line):
                line = re.sub(
                    r'(\b(?:result|data|df|series|column|row|baseline))\["([^"]+)"\]',
                    r'\1.loc["\2"]',
                    line,
                )
        modified_lines.append(line)

    content = "\n".join(modified_lines)

    if content != original:
        file_path.write_text(content)
        return 1
    return 0


def fix_list_type_annotations(file_path: Path) -> int:
    """Fix missing list type parameters."""
    content = file_path.read_text()
    original = content

    # Add specific imports if needed
    if "list[" in content and "from typing import" not in content:
        # Python 3.9+ doesn't need typing.List, built-in list works
        pass

    # Fix: list -> list[Any] in specific contexts
    # Look for return type annotations without parameters
    content = re.sub(
        r": list\s*$",
        r": list[Any]",
        content,
        flags=re.MULTILINE,
    )

    if content != original:
        file_path.write_text(content)
        return 1
    return 0


def fix_queue_annotations(file_path: Path) -> int:
    """Fix queue type annotations."""
    content = file_path.read_text()
    original = content

    # Add Queue import if needed
    if "Queue()" in content:
        if "from queue import Queue" not in content and "import queue" not in content:
            # Add import
            import_section = re.search(r"^(import .*\n|from .* import .*\n)+", content, re.MULTILINE)
            if import_section:
                insert_pos = import_section.end()
                content = content[:insert_pos] + "from queue import Queue\n" + content[insert_pos:]

        # Fix: results_queue = Queue() -> results_queue: Queue[Any] = Queue()
        content = re.sub(
            r"(\s+)(results_queue) = Queue\(\)",
            r"\1\2: Queue[Any] = Queue()",
            content,
        )

    if content != original:
        file_path.write_text(content)
        return 1
    return 0


def main():
    """Fix all remaining type errors."""
    print("Running mypy to identify errors...")
    errors = run_mypy()
    print(f"Found {len(errors)} errors\n")

    # Group errors by file
    files_with_errors: dict[str, list[str]] = {}
    for error in errors:
        if not error.strip():
            continue
        parts = error.split(":", 2)
        if len(parts) >= 2:
            file_path = parts[0]
            if file_path not in files_with_errors:
                files_with_errors[file_path] = []
            files_with_errors[file_path].append(error)

    # Fix each file
    fixed_count = 0
    for file_path_str in sorted(files_with_errors.keys()):
        file_path = Path(file_path_str)
        if not file_path.exists():
            continue

        fixes = 0
        file_errors = files_with_errors[file_path_str]

        # Check what types of errors exist in this file
        error_text = "\n".join(file_errors)

        if "Invalid index type" in error_text and 'Series"' in error_text:
            fixes += fix_series_indexing(file_path)

        if "Missing type parameters for generic type \"list\"" in error_text:
            fixes += fix_list_type_annotations(file_path)

        if "Need type annotation for \"results_queue\"" in error_text:
            fixes += fix_queue_annotations(file_path)

        if fixes > 0:
            print(f"✓ {file_path.name}: applied {fixes} fixes")
            fixed_count += 1

    print(f"\n✓ Fixed {fixed_count} files")
    print("\nRunning mypy again to check progress...")
    final_errors = run_mypy()
    print(f"Remaining errors: {len(final_errors)}")


if __name__ == "__main__":
    main()
