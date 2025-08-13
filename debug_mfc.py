#!/usr/bin/env python3
"""
Debug script to trace exactly what the MFC extraction finds
"""

import struct
from pyngb import get_sta_data
from pyngb.binary.parser import BinaryParser


def debug_mfc_extraction():
    test_file = "tests/test_files/DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3"

    # First, verify the extraction works
    print("=== Testing high-level extraction ===")
    metadata, data = get_sta_data(test_file)

    mfc_fields = [
        "purge_1_mfc_gas",
        "purge_2_mfc_gas",
        "protective_mfc_gas",
        "purge_1_mfc_range",
        "purge_2_mfc_range",
        "protective_mfc_range",
    ]

    for field in mfc_fields:
        value = metadata.get(field, "NOT_FOUND")
        print(f"  {field}: {value}")

    # Now let's examine the raw binary to understand the discrepancy
    print("\n=== Examining raw binary ===")

    parser = BinaryParser()
    with open(test_file, "rb") as f:
        content = f.read()

    tables = parser.split_tables(content)
    combined = b"".join(tables)

    print(f"File size: {len(content)} bytes")
    print(f"Tables: {len(tables)} (combined {len(combined)} bytes)")

    # Search for category/field patterns that our extraction uses
    category_pattern = b"\x00\x03"
    field_pattern = b"\x0c\x00"

    print(f"\nSearching for category pattern {category_pattern.hex()}")
    cat_positions = []
    start = 0
    while True:
        pos = combined.find(category_pattern, start)
        if pos == -1:
            break
        cat_positions.append(pos)
        start = pos + 1
    print(f"Found {len(cat_positions)} category occurrences")

    print(f"\nSearching for field pattern {field_pattern.hex()}")
    field_positions = []
    start = 0
    while True:
        pos = combined.find(field_pattern, start)
        if pos == -1:
            break
        field_positions.append(pos)
        start = pos + 1
    print(f"Found {len(field_positions)} field occurrences")

    # Look for contexts where both patterns are close together
    print("\nFinding category/field pairs...")
    valid_contexts = []

    for cat_pos in cat_positions:
        for field_pos in field_positions:
            if field_pos > cat_pos and field_pos - cat_pos < 200:
                context_start = max(0, cat_pos - 50)
                context_end = min(len(combined), field_pos + 200)
                context = combined[context_start:context_end]

                valid_contexts.append(
                    {
                        "cat_pos": cat_pos,
                        "field_pos": field_pos,
                        "context_start": context_start,
                        "context": context,
                    }
                )

    print(f"Found {len(valid_contexts)} valid category/field contexts")

    # Examine each context for potential MFC data
    for i, ctx in enumerate(valid_contexts[:5]):  # First 5
        print(f"\n--- Context {i + 1} ---")
        print(f"Category at {hex(ctx['cat_pos'])}, Field at {hex(ctx['field_pos'])}")

        # Look for strings and floats in this context
        context = ctx["context"]

        # Search for known gas names in different encodings
        gas_names = ["NITROGEN", "OXYGEN", "O2"]
        for gas in gas_names:
            # ASCII
            ascii_pos = context.find(gas.encode("ascii"))
            if ascii_pos != -1:
                abs_pos = ctx["context_start"] + ascii_pos
                print(f"  Found {gas} (ASCII) at {hex(abs_pos)}")

            # UTF-16LE
            utf16_pos = context.find(gas.encode("utf-16le"))
            if utf16_pos != -1:
                abs_pos = ctx["context_start"] + utf16_pos
                print(f"  Found {gas} (UTF-16LE) at {hex(abs_pos)}")

        # Search for float values
        float_values = [250.0, 252.5]
        for val in float_values:
            # Little endian
            le_bytes = struct.pack("<f", val)
            le_pos = context.find(le_bytes)
            if le_pos != -1:
                abs_pos = ctx["context_start"] + le_pos
                print(f"  Found {val} (LE float) at {hex(abs_pos)}")

            # Big endian
            be_bytes = struct.pack(">f", val)
            be_pos = context.find(be_bytes)
            if be_pos != -1:
                abs_pos = ctx["context_start"] + be_pos
                print(f"  Found {val} (BE float) at {hex(abs_pos)}")

        # Show hex dump of this context
        print("  Hex dump:")
        for j in range(0, min(len(context), 128), 16):
            chunk = context[j : j + 16]
            hex_str = " ".join(f"{b:02x}" for b in chunk)
            ascii_str = "".join(chr(b) if 32 <= b <= 126 else "." for b in chunk)
            offset = ctx["context_start"] + j

            # Mark category and field positions
            marker = ""
            chunk_start = ctx["context_start"] + j
            chunk_end = chunk_start + len(chunk)
            if ctx["cat_pos"] >= chunk_start and ctx["cat_pos"] < chunk_end:
                marker += " <CAT>"
            if ctx["field_pos"] >= chunk_start and ctx["field_pos"] < chunk_end:
                marker += " <FIELD>"

            print(f"    {offset:06x}: {hex_str:<48} |{ascii_str}|{marker}")


if __name__ == "__main__":
    debug_mfc_extraction()
