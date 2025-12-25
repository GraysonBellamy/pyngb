# NGB Binary Format Reference

## Overview

NETZSCH STA NGB files are proprietary binary files containing thermal analysis data. This document describes the reverse-engineered binary format to aid in maintaining and extending the parser.

⚠️ **Disclaimer**: This format documentation is based on reverse engineering and may not be complete or accurate for all NGB file versions. This is an unofficial implementation not affiliated with NETZSCH-Gerätebau GmbH.

## File Structure

### Container Format

NGB files are ZIP archives containing binary streams:

```
file.ngb-ss3
├── Streams/
│   ├── stream_1.table    # File metadata and settings
│   ├── stream_2.table    # Measurement data
│   └── stream_3.table    # Additional data (optional)
└── [other files]         # May contain additional resources
```

### Stream Organization

- **Stream 1** (Metadata): File-level metadata, calibration constants, temperature programs
- **Stream 2** (Data): Time-series measurement data (time, temperature, mass, DSC, etc.)
- **Stream 3+** (Auxiliary): Additional streams may contain derived data or settings

## Binary Markers

### Core Markers

| Marker Name | Hex Bytes | Purpose |
|------------|-----------|---------|
| `START_DATA` | `00 00 08 80 07` | Marks the beginning of a data array block |
| `END_TABLE` | `FF FE FF 00 00` | Marks the end of a complete table |
| `TABLE_SEPARATOR` | `FF FE FF 04` | Separates multiple tables within a stream |
| `TEMP_PROG_TYPE_PREFIX` | `03 80 01` | Prefix for temperature program entries |

### String Encoding Markers

| Marker Name | Hex Bytes | Purpose |
|------------|-----------|---------|
| `STRING_DATA_TYPE` | `1F` | Indicates string data follows |
| `FFFEFF_PATTERN` | `FF FE FF` | Alternative string encoding marker |

### Metadata Markers

| Marker Name | Hex Bytes | Purpose |
|------------|-----------|---------|
| `APP_LICENSE_CATEGORY` | `00 03` | Application license category marker |
| `APP_LICENSE_FIELD` | `18 FC` | License field identifier |

## String Encoding

NGB files use multiple string encoding schemes:

### 1. Standard String (4-byte length prefix)

```
[4 bytes: length (little-endian int32)][N bytes: UTF-8 data]
```

**Example**:
```
0C 00 00 00 48 65 6C 6C 6F 20 57 6F 72 6C 64 21
│           │
│           └─ "Hello World!" (12 bytes UTF-8)
└─ Length: 12 (0x0000000C)
```

### 2. UTF-16LE String

```
[4 bytes: length (little-endian int32)][N*2 bytes: UTF-16LE data]
```

**Example**:
```
18 00 00 00 48 00 65 00 6C 00 6C 00 6F 00 21 00
│           │
│           └─ "Hello!" (6 characters = 12 bytes UTF-16LE)
└─ Length: 24 (0x00000018) - byte count, not char count
```

### 3. FFFEFF Format (Special)

```
[FF FE FF][length byte][string data][00 00]
```

This format is used for specific metadata fields. The length byte indicates the string length, and the format ends with null terminators.

## Data Types

### Type IDs

| Type ID (Hex) | Format | Description | Size |
|--------------|--------|-------------|------|
| `0B` | `float64[]` | Array of double-precision floats | 8 bytes per value |
| `0A` | `float32[]` | Array of single-precision floats | 4 bytes per value |
| `08` | `int32` | 32-bit signed integer | 4 bytes |
| `09` | `int64` | 64-bit signed integer | 8 bytes |
| `1F` | `string` | Variable-length string | Variable |

### Data Array Format

Arrays are stored with a header and data section:

```
[START_DATA marker][Type ID][4 bytes: element count][data elements]
```

**Example - Float64 Array**:
```
00 00 08 80 07  0B  03 00 00 00  [24 bytes of float64 data]
│               │   │            │
│               │   │            └─ 3 float64 values (3 * 8 = 24 bytes)
│               │   └─ Element count: 3
│               └─ Type: 0x0B (float64[])
└─ START_DATA marker
```

## Metadata Patterns

### Column Identification

Columns in the data stream are identified by hex IDs. Common IDs:

| Hex ID | Column Name | Units | Description |
|--------|------------|-------|-------------|
| `0x0154` | `time` | seconds | Elapsed time |
| `0x02bc` | `sample_temperature` | °C | Sample temperature |
| `0x00e1` | `furnace_temperature` | °C | Furnace temperature |
| `0x0167` | `mass` | mg | Sample mass |
| `0x02c1` | `dsc_signal` | μV | DSC signal |

### Crucible Mass Patterns

**Sample Crucible**:
```
Signature: 07 00 0C 00  s  a  m  p  l  e  _  c  r  u  c  i  b  l  e
           │           │
           │           └─ "sample_crucible" string
           └─ Type and length markers
```

**Reference Crucible**:
```
Signature: 13 00 12 00  r  e  f  e  r  e  n  c  e  _  c  r  u  c  i  b  l  e
```

### Temperature Program Format

Temperature programs describe heating/cooling segments:

```
[TEMP_PROG_TYPE_PREFIX][Type byte][Start temp][End temp][Rate][...]
```

**Fields**:
- Type: `0x01` = heating, `0x02` = cooling, `0x03` = isothermal
- Temperatures: float64 (°C)
- Rate: float64 (°C/min for dynamic, minutes for isothermal)

## File Metadata Fields

### Common Metadata

| Field Name | Type | Description |
|-----------|------|-------------|
| `instrument` | string | Instrument model (e.g., "STA 449 F3 Jupiter") |
| `sample_name` | string | Sample identifier |
| `operator` | string | Operator name |
| `sample_mass` | float | Initial sample mass (mg) |
| `reference_mass` | float | Reference mass (mg) |
| `crucible_mass` | float | Crucible mass (mg) |
| `date` | string | Measurement date (ISO format) |
| `time` | string | Measurement time |

### Calibration Constants

DSC calibration constants for signal correction:

| Field Name | Type | Description |
|-----------|------|-------------|
| `tau` | float | Time constant (τ) |
| `sensitivity` | float | DSC sensitivity |
| `E` | float | Calibration constant E |

**Formula**: `DSC_corrected = (DSC_raw + τ * dDSC/dt) / Sensitivity + E`

## Column Metadata

Each data column can have metadata:

| Field | Type | Description |
|-------|------|-------------|
| `units` | string | Measurement units |
| `processing_history` | list[string] | Processing steps applied |
| `baseline_subtracted` | bool | Whether baseline correction applied |
| `source` | string | Data source identifier |

## Parsing Algorithm

### High-Level Algorithm

```python
def parse_ngb_file(file_path):
    # 1. Open ZIP archive
    with ZipFile(file_path) as zf:
        # 2. Read stream files
        streams = [zf.read(f"Streams/stream_{i}.table") for i in range(1, 4)]

        # 3. Parse binary streams
        for stream in streams:
            tables = split_tables(stream)  # Split on TABLE_SEPARATOR

            for table in tables:
                # 4. Extract data arrays
                arrays = extract_data_arrays(table)

                # 5. Parse metadata
                metadata = extract_metadata(table)

        # 6. Construct PyArrow table
        return build_table(arrays, metadata)
```

### Table Splitting

```python
def split_tables(stream: bytes) -> list[bytes]:
    """Split stream on TABLE_SEPARATOR markers."""
    separator = b'\xFF\xFE\xFF\x04'
    tables = stream.split(separator)
    return [t for t in tables if t]  # Remove empty tables
```

### Data Extraction

```python
def extract_data_arrays(table: bytes) -> dict:
    """Extract all data arrays from a table."""
    arrays = {}
    pos = 0

    while (start_pos := table.find(START_DATA, pos)) != -1:
        type_id = table[start_pos + 5]
        count = int.from_bytes(table[start_pos+6:start_pos+10], 'little')

        # Extract based on type
        if type_id == 0x0B:  # float64[]
            data_size = count * 8
            data_start = start_pos + 10
            values = struct.unpack(f'<{count}d',
                                  table[data_start:data_start+data_size])
            arrays[...] = values

        pos = start_pos + 10 + data_size

    return arrays
```

## Discovery Methodology

This binary format was reverse-engineered using:

1. **Hex Dump Analysis**: Manual inspection of files in hex editors
2. **Pattern Recognition**: Identifying repeated byte sequences
3. **Cross-File Comparison**: Comparing multiple NGB files
4. **Trial and Error**: Testing different interpretations
5. **Validation**: Checking parsed values against instrument software output

### Key Insights

- **ZIP Container**: Recognized standard ZIP signatures
- **Marker Patterns**: Identified repeated sequences before data blocks
- **Length Prefixes**: Common pattern in binary formats
- **Little-Endian**: Standard for Windows-based instruments
- **String Encodings**: Multiple formats found through trial-and-error

## Limitations and Unknowns

### Known Limitations

1. **Version Compatibility**: Format may vary across NETZSCH software versions
2. **Undocumented Fields**: Many byte sequences remain unidentified
3. **Conditional Logic**: Some patterns appear only in specific configurations
4. **Proprietary Extensions**: Vendor-specific features may exist

### Unknowns

- Complete list of all possible column hex IDs
- All temperature program segment types
- Meaning of some metadata fields
- Versioning scheme (if any)

## Validation Strategy

To validate parsing correctness:

1. **Cross-Check**: Compare with NETZSCH software export (CSV/text)
2. **Physical Validity**: Check temperature ranges, mass values
3. **Conservation**: Verify mass loss calculations
4. **Metadata Consistency**: Check sample mass matches data
5. **Multiple Files**: Test across different experiments/instruments

## Contributing

If you discover new patterns or corrections:

1. Document the pattern with hex dumps
2. Provide test files (if possible)
3. Explain the discovery methodology
4. Verify against multiple files

## References

- NETZSCH Proteus software documentation (limited)
- ZIP format specification (RFC 1951, RFC 1952)
- IEEE 754 floating-point standard
- UTF-8 and UTF-16LE encoding standards
