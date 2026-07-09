---
description: Complete API reference for pyngb — read_ngb, read_ngb_metadata, the document layer, BatchProcessor, the CLI subcommands, and utilities for parsing NETZSCH STA NGB files.
---

# API Reference

Complete reference for all pyngb functions and classes. Everything documented
here is importable from the package root (`from pyngb import ...`) unless
noted otherwise.

## Loading Functions

### read_ngb()

Main function for loading NGB files with optional baseline subtraction.

```python
def read_ngb(
    path: str | Path,
    *,
    return_metadata: bool = False,
    baseline_file: str | Path | None = None,
    dynamic_axis: str = "sample_temperature",
    limits: ParsingConfig | None = None,
) -> pa.Table | tuple[FileMetadata, pa.Table]
```

**Parameters:**

- `path`: Path to NGB file (`.ngb-ss3` or `.ngb-bs3`)
- `return_metadata`: If True, return a `(metadata, table)` tuple instead of a
  table with embedded metadata
- `baseline_file`: Path to a baseline (`.ngb-bs3`) file; when given, mass and
  DSC columns are baseline-subtracted and marked in column metadata
- `dynamic_axis`: Axis for dynamic-segment alignment during baseline
  subtraction — `"time"`, `"sample_temperature"`, or `"furnace_temperature"`
- `limits`: Optional `ParsingConfig` overriding the default resource limits

**Returns:** a PyArrow table with metadata embedded in the schema (default),
or `(FileMetadata, pa.Table)` when `return_metadata=True`. The metadata
includes a BLAKE2b `file_hash` of the source file.

**Raises:** `ValueError` (bad `dynamic_axis`), `FileNotFoundError`,
`zipfile.BadZipFile`, `NGBStreamNotFoundError` (streams 1/2 required; 3
optional), `NGBCorruptedFileError`, `NGBResourceLimitError`.

```python
# Basic loading
table = read_ngb("sample.ngb-ss3")

# With metadata separated
metadata, table = read_ngb("sample.ngb-ss3", return_metadata=True)

# With baseline subtraction
corrected = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")
```

### read_ngb_metadata()

Metadata-only fast path: reads and extracts stream 1 only (roughly half the
time of a full parse; no data assembly, no `file_hash`).

```python
def read_ngb_metadata(
    path: str | Path,
    *,
    limits: ParsingConfig | None = None,
) -> FileMetadata
```

A parity test guarantees this never drifts from the metadata returned by
`read_ngb(..., return_metadata=True)` (apart from `file_hash`).

```python
meta = read_ngb_metadata("sample.ngb-ss3")
print(meta["sample_name"], meta["sample_mass"])
```

## Document Layer

For programmatic exploration below the metadata/data level — every table and
field of every stream, including the streams `read_ngb` does not consume.
See [Binary Format](binary-format.md) for the underlying model.

### load_document()

```python
def load_document(
    path: str | Path,
    *,
    streams: Iterable[int] | None = None,   # default: all streams present
    limits: ParsingConfig | None = None,
) -> NGBDocument
```

### NGBDocument

Immutable snapshot of a parsed file.

**Attributes:** `streams` (raw `StreamData` per stream), `tables`
(`dict[int, tuple[Table, ...]]`), `spans` (unknown spans per stream),
`orphans` (fields outside any table).

**Methods:**

- `tables_of(stream_id) -> tuple[Table, ...]`
- `by_category(stream_id, category) -> Iterator[Table]`
- `find(stream_id, *, category=None, type_ref=None, with_fields=()) -> Iterator[Table]`
- `first(stream_id, *, category=None, type_ref=None, with_fields=()) -> Table | None`
- `has_defect(stream_id) -> bool` / `defects(stream_id) -> list[UnknownSpan]` —
  malformed/truncated spans
- `unknown_fields() -> dict[int, list[tuple[int, int, int]]]` — the
  `(category, field_id, dtype)` triples not yet mapped by pyngb, per stream

### Table

One serialized object: `stream_id`, `index` (position in stream order —
semantically meaningful), `category`, `type_ref`, `class_name`,
`fields: dict[int, Field]`, `preamble`, `span`.

**Methods:** `get(field_id) -> Field | None`, `value(field_id)`,
`has_fields(*field_ids) -> bool`, `strings() -> list[str]`.

### Field

One record: `field_id`, `dtype`, `mode`, `value` (eagerly decoded scalar),
`element_count`, `raw` (zero-copy payload view), `span`. Arrays decode
lazily via `.array()` (f64 NumPy array, or `bytes` for dtype-`0x10`).

```python
from pyngb import load_document

doc = load_document("sample.ngb-ss3")

# Read the sample name straight off the document (category 0x7530, field 0x0840)
sample_table = doc.first(1, category=0x7530, with_fields=(0x0840,))
print(sample_table.value(0x0840))

# What hasn't pyngb mapped yet?
print(doc.unknown_fields()[1][:10])
```

## Analysis Functions

### add_dtg()

Add a DTG (derivative thermogravimetry) column to a PyArrow table.

```python
def add_dtg(
    table: pa.Table,
    method: str = "savgol",
    smooth: str = "medium",
    column_name: str = "dtg",
) -> pa.Table
```

**Parameters:**

- `table`: Table with `time` and `mass` columns
- `method`: `"savgol"` or `"gradient"`
- `smooth`: `"strict"`, `"medium"`, or `"loose"`
- `column_name`: Name for the DTG column

### dtg() / dtg_custom() / calculate_table_dtg()

Array-level DTG calculation (mg/min):

```python
def dtg(time: np.ndarray, mass: np.ndarray,
        method: str = "savgol", smooth: str = "medium") -> np.ndarray

def calculate_table_dtg(table: pa.Table,
                        method: str = "savgol", smooth: str = "medium") -> np.ndarray
```

**Smoothing options:** `"strict"` (minimal, preserves all features),
`"medium"` (balanced, recommended), `"loose"` (heavy, for noisy data).
`dtg_custom()` accepts explicit Savitzky-Golay window/polyorder parameters.

### normalize_to_initial_mass()

```python
def normalize_to_initial_mass(
    table: pa.Table,
    columns: list[str] | None = None,
) -> pa.Table
```

Divides the given columns (default: `["mass", "dsc_signal"]` where present)
by the initial sample mass from the embedded metadata, **in place** — the
column names do not change; units gain a `/mg` suffix and `"normalized"` is
appended to their processing history.

### apply_dsc_calibration()

Convert the DSC signal from µV to mW using the calibration constants
(`p0`–`p5`) embedded in the file metadata.

```python
def apply_dsc_calibration(
    table: pa.Table,
    temperature_column: str = "sample_temperature",
    dsc_column: str = "dsc_signal",
) -> pa.Table
```

Samples whose temperature falls outside the calibration's valid range
(vanishing sensitivity) are set to NaN with a logged warning. Calling it
twice raises `ValueError`.

## Column Metadata Helpers

Column-level metadata (units, processing history, baseline status) is
embedded in the Arrow schema; these helpers read and write it:

```python
get_column_units(table, column) -> str | None
set_column_units(table, column, units) -> pa.Table
get_column_baseline_status(table, column) -> bool | None
mark_baseline_corrected(table, columns) -> pa.Table
inspect_column_metadata(table, column) -> dict
```

## Batch Processing

### BatchProcessor

Parallel multi-file processing on a process pool.

```python
class BatchProcessor:
    def __init__(self, max_workers: int | None = None, verbose: bool = True)

    def process_files(self, files, output_format="parquet",
                      output_dir=None, skip_errors=True) -> list[BatchResult]

    def process_directory(self, directory, pattern="*.ngb-ss3",
                          output_format="parquet", output_dir=None,
                          skip_errors=True) -> list[BatchResult]
```

`output_format` is `"parquet"`, `"csv"`, or `"both"`. With
`skip_errors=True` (default), per-file failures are recorded and processing
continues; results are returned in input order.

Module-level wrappers `process_files(...)` and `process_directory(...)`
cover the common case without instantiating the class.

### BatchResult

Per-file outcome TypedDict: `file`, `status` (`"success"`/`"error"`),
`rows`, `columns`, `sample_name`, `processing_time`, `error`.

### NGBDataset

Metadata-level view over a file collection (uses `read_ngb_metadata`, so no
data parsing):

```python
dataset = NGBDataset.from_directory("./data/")
dataset.summary()                       # aggregate stats
dataset.export_metadata("meta.csv")     # csv / json / parquet
subset = dataset.filter_by_metadata(lambda m: m.get("operator") == "GB")
```

## Validation

```python
validate_sta_data(table) -> list[str]        # quick issue list
checker = QualityChecker(table)
result = checker.full_validation()           # -> ValidationResult
result.is_valid, result.summary()
```

`QualityChecker` composes structural, temperature, mass, and DSC validators;
degenerate data produces findings, not exceptions.

## Baseline Subtraction

The public path is `read_ngb(path, baseline_file=...)` (see above), which
also tags column metadata. `BaselineSubtractor` is exported for advanced use
on already-parsed data: dynamic segments are aligned per temperature-program
stage on the chosen `dynamic_axis`; isothermal segments subtract on time.

## Data Structures

### FileMetadata

TypedDict of everything extracted from stream 1. All fields are optional —
absence means the file didn't carry it. Keys:

**Sample and run identity:** `sample_name`, `sample_id`, `material`,
`sample_mass` (mg), `instrument`, `project`, `lab`, `operator`, `comment`,
`date_performed` (ISO 8601 UTC), `application_version`, `licensed_to`,
`file_hash` (`{"file", "method": "BLAKE2b", "hash"}`; full parse only).

**Hardware configuration:** `crucible_type`, `furnace_type`, `carrier_type`,
`crucible_mass`, `reference_mass`, `reference_crucible_mass` (mg).

**Temperature program** — stage durations in seconds:

```python
{
    "stage_0": {"stage_type": ..., "temperature": 25.0, "heating_rate": 0.0,
                "acquisition_rate": ..., "time": 300.0},
    "stage_1": {...},
    # exactly the stages programmed for the run
}
```

**PID settings:** `furnace_xp`, `furnace_tn`, `furnace_tv`, `sample_xp`,
`sample_tn`, `sample_tv`.

**Run environment:** `timezone` (Windows timezone name active on the
instrument PC), `utc_offset_minutes` (DST-aware; `date_performed` is UTC —
these recover local wall-clock time), `correction_file_path` (for sample
runs, the matching baseline file).

**MFC gas metadata:** `purge_1_mfc_gas` / `purge_2_mfc_gas` /
`protective_mfc_gas` (gas identity), `..._mfc_range` (full-scale range,
ml/min), `..._mfc_flow` (configured setpoint, ml/min — for MFC channels
without a data column this is the only record of the flow).

**Calibrations:**

- `calibration_constants` (dict): DSC sensitivity polynomial `p0`–`p5`. This
  is the one calibration that *must* be applied downstream
  (`apply_dsc_calibration`) — DSC is stored raw in µV.
- `temperature_calibration` (`TemperatureCalibration`): captured for
  traceability/QA **only** — the `sample_temperature` channel is already
  temperature-corrected by Proteus:

```python
{
    "coefficients": [-43.898, -811.727, 247.131],   # [B0, B1, B2]
    "fixpoints": [
        {
            "name": "Biphenyl",      # phase-transition standard (varies per cal)
            "actual_c": 69.2,        # literature/reference temperature (°C)
            "measured_c": 69.8,      # raw measured transition temperature (°C)
            "weight": 1.0,           # regression weight for this point
            "corrected_c": 69.202,   # measured value after the polynomial
        },
        # ... 6-9 standards total, ascending temperature
    ],
    "record_path": r"C:\NETZSCH\...\Calibrations\K_44_....ngb-ts3",
    "date_measured": "2025-07-27T19:12:18+00:00",
    "gas": "NITROGEN",
    "crucible_type": "PtRh20 85 µl, with lid",
    "heating_rate": 10.0,            # K/min
    "comment": "Swapped crucible positions, 70 mL/min total flowrate",
}
```

  The fixpoint columns satisfy
  `corrected_c == measured_c + (1e-3*B0 + 1e-5*B1*T + 1e-8*B2*T**2)` with
  `T = measured_c` (verified by round-trip on all sample files); the residual
  `actual_c - corrected_c` is the calibration fit error.

- `sensitivity_calibration` (`SensitivityCalibration`): provenance of the DSC
  sensitivity calibration — `record_path` (the external `.ngb-es3` record),
  `date_measured`, `gas`, `crucible_type`, `heating_rate`, `comment`.

### ParsingConfig

Frozen dataclass of resource limits, passed as `limits=` to all loading
functions. Limits are enforced *before* allocation.

```python
@dataclass(frozen=True, slots=True)
class ParsingConfig:
    max_stream_size_mb: int = 1000      # declared decompressed stream size
    max_tables_per_stream: int = 10000
    max_array_size_mb: int = 500        # declared array payload size
```

### Column Names

Standard column names in processed data:

- `time` (s), `sample_temperature` (°C), `furnace_temperature` (°C)
- `mass` (mg), `dsc_signal` (µV raw; mW after `apply_dsc_calibration`),
  `dtg` (mg/min, when calculated)
- `purge_flow_1`, `purge_flow_2`, `protective_flow` (ml/min) — presence
  depends on the instrument configuration

Each column carries metadata (`units`, `processing_history`, `source`, and
where applicable `baseline_subtracted` / `calibration_applied`) readable via
the [column metadata helpers](#column-metadata-helpers).

## Exceptions

```
NGBParseError (base)
├── NGBCorruptedFileError
├── NGBStreamNotFoundError
├── NGBResourceLimitError
└── NGBDataTypeError
```

- **`NGBParseError`** — base class; catch this for "anything went wrong
  parsing".
- **`NGBCorruptedFileError`** — container integrity failures, grammar
  violations in data streams, channel-assembly mismatches. Structured
  attributes: `stream`, `offset`, `table_index`, `declared`, `available`.
- **`NGBStreamNotFoundError`** — a required stream is missing from the
  archive (`read_ngb` needs streams 1 and 2; `read_ngb_metadata` needs 1).
- **`NGBResourceLimitError`** — a `ParsingConfig` limit was exceeded; raised
  before allocation. Attributes: `stream`, `offset`, `declared`, `limit`.
- **`NGBDataTypeError`** — unknown or invalid data type.

Non-NGB failures surface as standard exceptions: `FileNotFoundError`,
`zipfile.BadZipFile` (not a ZIP archive), `ValueError` (bad arguments).

## Command Line Interface

Installed as the `pyngb` console script (`python -m pyngb` is equivalent).
Three subcommands; all accept multiple files with per-file error isolation
and exit non-zero if any file fails.

### pyngb convert

Parse NGB files and export Parquet/CSV.

```
pyngb convert FILE... [-o DIR] [-f {parquet,csv,both}] [-b BASELINE]
              [--dynamic-axis {time,sample_temperature,furnace_temperature}] [-v]
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-o, --output` | `.` | Output directory |
| `-f, --format` | `parquet` | Output format |
| `-b, --baseline` | — | Baseline file for subtraction (output gains a `_baseline_subtracted` suffix) |
| `--dynamic-axis` | `sample_temperature` | Axis for dynamic-segment alignment |
| `-v, --verbose` | off | Debug logging |

### pyngb inspect

Show document structure: sections, tables, coverage, unknown fields.

```
pyngb inspect FILE... [--stream N] [--values] [--unknown] [--coverage] [--json]
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--stream` | `1` | Stream for the table listing / cross-file comparison |
| `--values` | off | Show decoded field values in the table listing |
| `--unknown` | off | Only the unknown-field census (the format-mapping to-do list) |
| `--coverage` | off | Only byte-coverage accounting (gap bytes and span kinds) |
| `--json` | off | Machine-readable output |

With multiple files, `inspect` cross-references scalar fields on the selected
stream and reports which differ between files.

### pyngb validate

Run data-quality checks (`QualityChecker.full_validation`) on each file.

```
pyngb validate FILE... [--json]
```

Exit code 1 if any file is invalid or unparseable.

## Usage Examples

### Complete Analysis Workflow

```python
from pyngb import read_ngb, add_dtg, normalize_to_initial_mass, BatchProcessor
import polars as pl
import json

# Single file analysis
table = read_ngb("sample.ngb-ss3", baseline_file="baseline.ngb-bs3")
table = normalize_to_initial_mass(table)
table = add_dtg(table, smooth="medium")

# Convert to DataFrame
df = pl.from_arrow(table)

# Access metadata
metadata = json.loads(table.schema.metadata[b"file_metadata"])
print(f"Sample: {metadata['sample_name']}")
print(f"Mass loss: {(1 - df['mass'].min()) * 100:.1f}%")  # mass is normalized in place

# Batch processing
processor = BatchProcessor(max_workers=4)
results = processor.process_directory("./data/", output_format="parquet")
```

### Error Handling

```python
from pyngb import read_ngb, NGBParseError, NGBCorruptedFileError

try:
    table = read_ngb("sample.ngb-ss3")
except FileNotFoundError:
    print("File not found")
except NGBCorruptedFileError as e:
    print(f"Corrupted file (stream={e.stream}, offset={e.offset}): {e}")
except NGBParseError as e:
    print(f"Parsing failed: {e}")
```

### Custom Analysis

```python
import numpy as np
import polars as pl
from scipy.signal import find_peaks

df = pl.from_arrow(table)
temperature = df["sample_temperature"].to_numpy()
dtg_values = df["dtg"].to_numpy()

peaks, _ = find_peaks(-dtg_values, height=0.01)
print(f"Decomposition peaks at: {temperature[peaks]} °C")
```

For the format itself see [Binary Format](binary-format.md); for the
internal design see [Architecture](architecture.md).
