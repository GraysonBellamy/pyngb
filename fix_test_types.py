#!/usr/bin/env python3
"""Script to automatically add type annotations to test files."""
import re
from pathlib import Path

def fix_test_file(file_path: Path) -> tuple[int, list[str]]:
    """Fix type annotations in a test file."""
    content = file_path.read_text()
    original_content = content
    changes = []

    # Add typing imports if not present
    if "from typing import" not in content:
        # Find the right place to add the import (after other imports)
        import_pattern = r"(^import .*\n|^from .* import .*\n)+"
        match = re.search(import_pattern, content, re.MULTILINE)
        if match:
            # Add after existing imports
            insert_pos = match.end()
            content = content[:insert_pos] + "from typing import Any\n" + content[insert_pos:]
            changes.append("Added typing import")

    # Fix test methods without return type annotations
    # Pattern: def test_XXXX(self): or def test_XXXX(self, ...)
    pattern = r'(\n    def test_[^(]+\(self)((?:, [^)]+)?)(\):)'

    def replace_func(match):
        before = match.group(1)
        params = match.group(2)
        after = match.group(3)

        # Check if params already has type hints
        if params and '->' not in params and ': ' not in params:
            # Add type hints to mock parameters
            params = re.sub(r', ([a-z_]+)', r', \1: Any', params)

        return f"{before}{params}) -> None:"

    new_content = re.sub(pattern, replace_func, content)

    if new_content != content:
        changes.append(f"Fixed {len(re.findall(pattern, content))} test methods")
        content = new_content

    # Fix fixture/property methods
    # Pattern: @property or @pytest.fixture followed by def method(self):
    property_pattern = r'(@(?:property|pytest\.fixture[^\n]*)\n\s+def [^(]+\(self)(\):)'

    def replace_property(match):
        before = match.group(1)
        after = match.group(2)
        # Check if already has type annotation
        if ' -> ' in before:
            return match.group(0)
        return f"{before}) -> Any:"

    new_content = re.sub(property_pattern, replace_property, content)

    if new_content != content:
        changes.append("Fixed property/fixture methods")
        content = new_content

    # Write back if changed
    if content != original_content:
        file_path.write_text(content)
        return len(changes), changes

    return 0, []


def main():
    """Fix all test files."""
    tests_dir = Path("tests")
    test_files = list(tests_dir.glob("test_*.py"))

    total_fixed = 0
    for test_file in sorted(test_files):
        num_changes, changes = fix_test_file(test_file)
        if num_changes > 0:
            print(f"✓ {test_file.name}: {', '.join(changes)}")
            total_fixed += 1
        else:
            print(f"  {test_file.name}: no changes needed")

    print(f"\n✓ Fixed {total_fixed} files")


if __name__ == "__main__":
    main()
