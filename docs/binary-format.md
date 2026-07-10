---
description: Reverse-engineered reference for the NETZSCH STA NGB binary format — container layout, section directory, record grammar, data types, table object model, and stream contents.
---

# NGB Binary Format Reference

## Overview

NETZSCH STA NGB files are proprietary binary files containing thermal analysis
data. This document describes the reverse-engineered format as pyngb parses it:
a ZIP container of streams, each stream a sectioned blob of serialized objects,
every object a **table** of **fields** following one uniform record grammar.

Everything here was verified empirically against six real fixtures (two
Proteus vintages, 2022 and 2025, including two baseline files) during the
2026-07 format investigation — 25,075 grammar records across all streams with
zero counter-examples. Numbers quoted below (record censuses, span sizes) come
from those fixtures via `scripts/make_goldens.py census`; regenerate them with
`pyngb inspect` if the fixture set changes.

⚠️ **Disclaimer**: This format documentation is based on reverse engineering
and may not be complete or accurate for all NGB file versions. This is an
unofficial implementation not affiliated with NETZSCH-Gerätebau GmbH.

Byte-level constants live in [`pyngb.format.grammar`][grammar] and
[`pyngb.format.container`][container]; all declarative field knowledge lives in
[`pyngb.format.maps`][maps]. If this document and the code disagree, the code
(pinned by golden tests) wins.

[grammar]: https://github.com/GraysonBellamy/pyngb/blob/main/src/pyngb/format/grammar.py
[container]: https://github.com/GraysonBellamy/pyngb/blob/main/src/pyngb/format/container.py
[maps]: https://github.com/GraysonBellamy/pyngb/blob/main/src/pyngb/format/maps.py

## Container

NGB files are ZIP archives containing binary streams:

```
file.ngb-ss3
├── Streams/
│   ├── stream_1.table    # metadata: sample, program, calibrations, MFC, …
│   ├── stream_2.table    # primary measurement channels
│   ├── stream_3.table    # additional measurement channels (optional)
│   ├── stream_4.table    # end-of-run snapshot (not parsed)
│   ├── stream_5.table    # furnace usage histograms (not parsed)
│   └── stream_6.table    # embedded plot previews (not parsed)
├── Props.xml             # stream/section manifest
└── [Content_Types].xml
```

`.ngb-ss3` is a sample run; `.ngb-bs3` is a baseline (correction) run with the
same structure. `read_ngb` requires streams 1 and 2 and uses stream 3 when
present; `read_ngb_metadata` reads stream 1 only; `load_document` models any
requested stream, including 4–6.

### Stream header and section directory

Every `stream_N.table` starts with two magic strings and a section directory:

| Offset | Content |
|--------|---------|
| 0x02 | `"Netzsch TA file"` |
| 0x1C | `"_db_format_1"` |
| 0x50 | section directory: consecutive 14-byte entries |

Each directory entry:

```
ff ff | <section id u16 LE> | <offset u32 LE> | <size u32 LE>
```

2022-vintage files terminate the directory with an all-zero entry
(`ff ff 00 00 00 00 00 00 00 00 00 00 00 00`); 2025-vintage directories simply
stop (the next bytes are not `ff ff`-prefixed).

Invariants — checked by `parse_container`, violations raise
`NGBCorruptedFileError`:

- every entry is prefixed `ff ff`;
- offsets are strictly increasing and sections are contiguous
  (each section starts where the previous one ends);
- the last section ends exactly at end-of-file;
- a **main section** whose id equals the stream number exists.

Each stream also carries a small **section 1** (~480–580 bytes): a
table-of-contents of class records for the stream (stream 1 folds it into its
main section). The measurement content is entirely in the main section.

## Record grammar

The streams are MFC `CArchive`-style object serialization, not loose byte
soup. Every field of every table in every stream follows one grammar:

```
18 fc ff ff 03 80 01            RECORD_HEADER
<field_id u16 LE>
00 00 01 00 00 00               FIELD_BRIDGE
0c 00                           FIELD_KIND (u16, always 0x000C)
17 fc ff ff                     TYPE_PREFIX
<dtype u8>                      one of the nine data types below
80 01 <scalar payload>          SCALAR mode …
  — or —
a0 01 <count u32 LE> <payload>  ARRAY mode
01 00 00 00 02 00 01 00 00      END_FIELD
```

Two rules with teeth:

- **Array counts are element counts**, not byte counts: the payload occupies
  `count × ITEM_SIZE[dtype]` bytes.
- A scalar's extent is fully determined by its dtype (fixed `ITEM_SIZE`, or
  the string/REF headers below), so `END_FIELD` is *verified at the computed
  position*, never searched for. Record anchors occurring inside array
  payloads are therefore harmless — a linear walk never sees them.

### String encoding

String payloads (dtype `1f`) come in two forms:

```
ff fe ff | <char_count u8> | <char_count × 2 bytes UTF-16LE>
```

or a length-prefixed form:

```
<byte_len u32 LE> | <byte_len bytes>     UTF-8, falling back to UTF-16LE
```

Decoding is strict in both encodings; an undecodable payload yields `None`
rather than mangled text.

### Data types

Nine dtypes are observed across all fixtures and streams (`DType` enum;
counts are strict-grammar records summed over all six fixtures, all streams):

| dtype | Type | Item size | Records | Notes |
|-------|------|-----------|---------|-------|
| `0x02` | u16 | 2 | 4,525 | |
| `0x03` | i32 | 4 | 6,668 | plus the 66 END_FIELD-less "bare records" below |
| `0x04` | f32 | 4 | 2,694 | |
| `0x05` | f64 | 8 | 828 | |
| `0x10` | u8 / byte array | 1 | 1,749 | arrays decode as raw `bytes` |
| `0x14` | packed 8-byte record | 8 | 492 | opaque; e.g. `eb03eb03…` tokens matching calibration record filenames |
| `0x1a` | object reference (REF) | variable | 4,518 | table-open records; exactly one per table |
| `0x1f` | string | variable | 3,589 | |
| `0x48` | 16-byte hash/GUID | 16 | 12 | stream 6 only |

Numeric arrays decode via `np.frombuffer` and widen to f64
(`.astype('<f8')`); scalars decode with precompiled `struct.Struct`s.

### Non-record spans

The tokenizer (`pyngb.format.grammar.tokenize`) is a strict linear walk that
is **total**: every byte of a section is either part of a decoded
`FieldToken` or covered by an explicit `UnknownSpan` — nothing is silently
skipped. Grammar records cover 95–97% of section bytes on every fixture
stream; the residue is exactly these enumerable forms (`SpanKind`):

| Kind | Size | Count (all fixtures) | Meaning |
|------|------|----------------------|---------|
| `prologue` | 64 B | 66 (one per section) | section preamble, starts `02 00 00 80`; 6 observed variants differing in 2 bytes |
| `preamble` | 47 B | 4,518 (one per table) | record variant with mode bytes `00 01`; not yet semantically decoded |
| `table_trailer` | 3 B | 20,557 | `00 03 00` sequence closing each table |
| `bare_record` | 28 B | 66 (11 per fixture, both vintages) | i32 scalar record with **no** END_FIELD, directly after a table trailer; field ids `0x0FDE`/`0x1165` |
| `malformed` | variable | 0 in healthy files | grammar violation; tokenizer resyncs to the next record anchor |
| `truncated` | variable | 0 in healthy files | array whose declared extent overruns the section; the walk stops (a broken length forfeits resync trust) |

The tokenizer itself never raises on corruption — severity policy lives in
the consumers (see [Corruption semantics](#corruption-semantics-and-coverage)).
Its only exception is `NGBResourceLimitError` for an array whose declared
payload exceeds `max_array_size_mb`, checked *before* any allocation.

## Table object model

Fields are grouped into **tables**. A table opens with a REF record (dtype
`0x1a`) and closes with END_FIELD + the `00 03 00` trailer:

- The open record's **field_id is the table's category** (u16).
- Its payload starts with either a class back-reference (`01 80` = class
  index 1, etc.) or — first table of a stream — an inline class definition
  `ff ff <schema u16> <name_len u16> <name>` (observed name: `CDbTable`).
- The payload ends `02 00 00 80 <type_ref u16> 00 00`; the **type_ref**
  identifies the table's kind. Class-definition records for non-table
  classes have no type_ref.
- After the open record comes one `preamble` span, then the field records.

Two identity rules matter for extraction:

- **Field ids never repeat within a table** (`Table.fields` is a dict keyed
  by field id).
- **Categories DO repeat across tables**: `0x7530` alone is used by the
  sample table, temperature/sensitivity fixpoint tables, MFC
  device-parameter tables, and accessory records. Tables are identified by
  field membership and type_ref, never by category alone.

Known type_refs (the full per-fixture set is pinned by the census goldens):

| type_ref | Table kind |
|----------|-----------|
| `0x2B22` | channel header (streams 2/3) |
| `0x2B23` | channel segment values (streams 2/3) |
| `0x0Bxx` | stream-1 metadata table families |
| others | structural/device tables, ignored by extraction |

## Stream 1: metadata catalog

Extraction resolves each metadata key against the document with one rule:
**the first stream-1 table of the category that carries the field wins**
(stream order is semantic). The declarative map (`FIELD_MAP` in
`pyngb.format.maps`):

| Metadata key | Category | Field | Conversion |
|--------------|----------|-------|------------|
| `instrument` | `0x1775` | `0x1059` | string |
| `project` | `0x1772` | `0x083C` | string |
| `date_performed` | `0x1772` | `0x083E` | Unix → ISO 8601 UTC |
| `lab` | `0x1772` | `0x0834` | string |
| `operator` | `0x1772` | `0x0835` | string |
| `comment` | `0x1772` | `0x083D` | string |
| `crucible_type` | `0x177E` | `0x0840` | string |
| `furnace_type` | `0x177A` | `0x0840` | string |
| `carrier_type` | `0x1779` | `0x0840` | string |
| `sample_id` | `0x7530` | `0x0898` | string |
| `sample_name` | `0x7530` | `0x0840` | string |
| `material` | `0x7530` | `0x0962` | string |
| `sample_mass` | `0x7530` | `0x0C9E` | positive float |

The remaining structures are procedural (each one function in
`pyngb.format.extract`, all warn-and-continue):

### Temperature program

Stage tables (type ref `0x2B0C`) are the stream-1 tables carrying **all
five** stage fields. The table's **category encodes the program ordinal**:
stage N has category `0x7530 + N`, exposed as `stage_N`. Stream order is
edit order, not program order — programs edited in Proteus serialize out
of order (two fixtures store 0, 2, 3, 4, 1; verified against the recorded
temperature data, which executes in category order):

| Field | Key |
|-------|-----|
| `0x083F` | `stage_type` (i32) — 0 = initial, 1 = ramp/isothermal (`heating_rate` > 0 vs = 0), 2 = final entry carrying the emergency-reset temperature (a limit, never executed) |
| `0x0E17` | `temperature` (°C) |
| `0x0E13` | `heating_rate` (°C/min) |
| `0x0E14` | `acquisition_rate` |
| `0x0E15` | `time` — stored in minutes, exposed in **seconds** (×60) |

Each stage table is followed by per-device state snapshots (see the MFC
device tree below) whose flow setpoints are merged into the stage dict as
`purge_1_mfc_flow` / `purge_2_mfc_flow` / `protective_mfc_flow` (ml/min).

### PID settings

Tables carrying all three of `0x0FE7` (xp), `0x0FE8` (tn), `0x0FE9` (tv):
the first occurrence in stream order is the **furnace** controller
(`furnace_xp/tn/tv`), the second is the **sample** controller
(`sample_xp/tn/tv`).

### Crucible masses

Two category-`0x177E` tables each carry field `0x0C9E` (f64). The
**preceding table** discriminates them: a trailing `0x0C83` (f32) field marks
the *sample* crucible; a trailing `0x10C4` (u16) field marks the *reference*
crucible. In all observed files the reference crucible comes first in stream
order. `reference_mass` is recovered as the last numeric scalar of the table
preceding the reference-crucible table.

### DSC calibration constants

From the first category-`0x01F5` table yielding them: `p0`=`0x044F`,
`p1`=`0x0450`, `p2`=`0x0451`, `p3`=`0x0452`, `p4`=`0x0453`, `p5`=`0x04C3`.

These feed `apply_dsc_calibration`: sensitivity
`y = (P2 + P3·z + P4·z² + P5·z³)·exp(−z²)` with `z = (T − P0)/P1`, converting
the DSC signal µV → mW.

### Temperature calibration

Surfaced as the `temperature_calibration` metadata block plus a sibling
`sensitivity_calibration` block. Captured for **traceability/QA only**: the
`sample_temperature` channel is already temperature-corrected by Proteus, so
re-applying these coefficients would double-correct the data.

**Coefficients** — three f32 values stored as a dtype-`0x10` byte array on
field `0x04BE` of the category-`0x01F7` table (12 bytes = 3 × f32 LE),
giving `[B0, B1, B2]`.

**Fixpoints** — the phase-transition standards used for the calibration, one
table per standard, categories `0x7530`–`0x753F` (ascending temperature; real
files carry 6–9 standards — Biphenyl, Benzoeacid, KClO4, Ag2SO4, CsCl,
K2CrO4, BaCO3, …). Because those categories are shared with sample/MFC/DSC
tables, a *temperature* fixpoint table is confirmed by carrying **both**
`0x0444` and `0x0447` (DSC sensitivity fixpoint tables carry
`0x0454`/`0x0455`/`0x0456` instead of `0x0444`):

| Field | Key | Description |
|-------|-----|-------------|
| `0x0443` | `name` | standard name |
| `0x0444` | `actual_c` | literature transition temperature (°C) |
| `0x0445` | `measured_c` | raw measured transition temperature (°C) |
| `0x0446` | `weight` | regression weight (1.0 in observed files) |
| `0x0447` | `corrected_c` | measured value after the calibration polynomial |

**Relationship** (verified by round-trip on all fixtures, residual < 1e-3):

```
corrected_c = measured_c + (1e-3·B0 + 1e-5·B1·T + 1e-8·B2·T²)   # T = measured_c
```

The residual `actual_c − corrected_c` is the calibration fit error.

**DSC sensitivity fixpoints** — the enthalpy standards behind the p0–p5
calibration constants, surfaced as `sensitivity_calibration.fixpoints`. A
second table family in the same `0x7530`–`0x753F` categories (ascending
temperature; 6–8 standards in real files), reusing field ids
`0x0443`/`0x0445`–`0x0447` with different meanings — the family is
discriminated by carrying `0x0454` and **no** `0x0444`:

| Field | Key | Description |
|-------|-----|-------------|
| `0x0443` | `name` | standard name |
| `0x0454` | `temperature_c` | transition temperature (°C) |
| `0x0455` | `enthalpy` | literature transition enthalpy (J/g, endothermic negative) |
| `0x0456` | `peak_area` | measured DSC peak area (µV·s/mg, sign matches enthalpy) |
| `0x0445` | `measured_sensitivity` | measured sensitivity point (µV/mW) |
| `0x0446` | `weight` | regression weight (1.0 in observed files) |
| `0x0447` | `fitted_sensitivity` | calibration curve at `temperature_c` (µV/mW) |

**Relationships** (both exact on every fixpoint of every fixture, residual
< 4e-7 — f32 precision):

```
measured_sensitivity = peak_area / enthalpy
fitted_sensitivity   = (P2 + P3·z + P4·z² + P5·z³)·exp(−z²)   # z = (temperature_c − P0)/P1
```

i.e. these standards are the regression behind the `calibration_constants`
curve used by `apply_dsc_calibration`, and `fitted_sensitivity` is that
curve evaluated at each standard's transition temperature. Values were stored
negative (endothermic-negative convention) and are reported as stored.

**Record paths and provenance** — each external calibration record has one
category-`0x01F5` source table, located by the suffix of its path string
field: `.ngb-ts3` → `temperature_calibration.record_path`, `.ngb-es3` →
`sensitivity_calibration.record_path`. The same table carries the conditions
of the calibration run, extracted into both blocks:

| Field | Key | Description |
|-------|-----|-------------|
| `0x083E` | `date_measured` | Unix timestamp of the calibration run (→ ISO 8601 UTC) |
| `0x0431` | `gas` | purge gas used |
| `0x044C` / `0x0433` | `crucible_type` | crucible used (`0x044C` in the es3 table, `0x0433` in the ts3 table) |
| `0x0435` | `heating_rate` | heating rate in K/min |
| `0x083D` | `comment` | operator comment on the calibration run |

### Run environment

**Timezone** — the category-`0x1859` table is a Windows
`TIME_ZONE_INFORMATION`-style snapshot: `0x1135` name (string), `0x1134` bias
(i32 minutes, UTC = local + bias), `0x1137` DST bias (i32), `0x1138` state
(i32; 1 = standard, 2 = daylight). Exposed as `timezone` and
`utc_offset_minutes` (`−(bias + dst_bias)` when daylight time is active, else
`−bias`). `date_performed` is UTC; this recovers the local wall-clock time of
the run. Files carry several snapshots; the first (run start) is used.

**Correction file link** — the category-`0x1770` measurement-definition table
stores, in field `0x0843`, the path of the correction file selected for the
run (→ `correction_file_path`). For sample (`.ngb-ss3`) runs this identifies
the matching baseline (`.ngb-bs3`) file; for correction runs it may reference
the related sample or a prior correction run.

### MFC gas metadata: the device tree

Every file carries one self-describing device block in stream 1 (identical
type refs in the Proteus 7.5 and 8.0 fixtures, sample and baseline files
alike). All MFC metadata comes from it — no string matching, no ordinal
pairing:

- **Device definitions** (type ref `0x2B07`, categories `0x1BAC`+), one per
  device: device id (`0x083F`, i32), configured gas name (`0x0840`), gas
  GUID (`0x0C8F`), device kind (`0x104B`, 2 = MFC; kinds 10/8 are non-MFC
  devices), hardware type (`0x1075`, 104 ↔ MFC400). The device id → role
  map is fixed — **30 = purge 1, 31 = purge 2, 32 = protective** —
  confirmed three independent ways on the 2022 fixture (device-parameter
  names, recorded flow channels, category order). An MFC definition with
  any other id (a real fourth controller) is logged as a warning, never
  silently dropped. The gas name → `purge_1_mfc_gas`, ….
- **Range table** (type ref `0x2B0A`, category `0x1780`), immediately after
  each MFC definition: full scale `0x1048` → `purge_1_mfc_range`, … plus
  the overrange limit `0x104D` (full scale × 1.02) and gas correction
  factor `0x104C` (not extracted).
- **Gas record** (type ref `0x2B81`, category `0x1BE4`), after the range
  table: GUID `0x17FC` (must match the definition's `0x0C8F`), gas name
  `0x0840`, short formula `0x0C88` → `purge_1_mfc_gas_formula`, …, and
  density `0x1040` (g/l, not extracted). Gas records of the same shape
  also occur inside calibration-context blocks elsewhere in the stream
  (plus empty ones at category `0x1B58`); anchoring on the definitions and
  GUID-matching keeps those out.
- **Per-stage states** (type ref `0x2B11`, same categories as the
  definitions): after every temperature-program stage table, one state
  table per device, its following range table carrying that stage's flow
  setpoint in `0x1047` (ml/min). These merge into the stage dicts, and
  `purge_1_mfc_flow` / … is emitted when the flow is uniform across the
  program's body stages (`stage_type` 1) — the setpoint the run actually
  used. A 0.0 means the MFC was configured but not flowing (e.g. the O2
  controller during an N2-only run). Programs that vary a flow per stage
  get no scalar key for that MFC.

The `0x7530`-category **device-parameter tables** (type ref `0x2B65`;
UTF-16LE name in `0x1062`, f32 value in `0x1061`, e.g.
`Purge 1 MFC_MFC400_LastUsedFlow`) are documented but deliberately **not
extracted**: `LastUsedFlow` is persisted instrument config — the last flow
ever used on the channel, stale for MFCs the run did not use — and Proteus
8.0.3 writes parameter blocks even for hardware that does not exist
(`Purge 3` on a three-MFC instrument).

### Application and license

Strings of the category-`0x0300` table: the first matching
`Version N.N.N` → `application_version`; the longest multiline string →
`licensed_to`.

## Streams 2 and 3: measurement channels

Data streams hold one **channel header table** per channel followed by that
channel's **segment value tables** — a type_ref state machine, not byte
positions, drives assembly:

- **Header table** — type_ref `0x2B22`. The category's **low byte is the
  channel id** (header categories are `<ch> 17` in stream 2 and `<ch> 75` in
  stream 3 — the latter collide with segment categories and are
  disambiguated purely by type_ref).
- **Segment value tables** — type_ref `0x2B23`, categories `0x7530`,
  `0x7531`, … (segment ordinals). Each carries **exactly one data array**:
  field `0x0F40` (f64) for f64 channels (time `8c`, mass `90`) or field
  `0x0F3D` (f32) for f32 channels (temperatures, DSC, flows). Segment arrays
  concatenate in stream order to form the channel.
- The `time` channel is stored in minutes and exposed in seconds (×60).
- Channel `0x87` is a data-less trailer header (intentionally unmapped);
  other unmapped channel ids pass through as hex column names.
- Structural ~90-byte tables with other type_refs are ignored.

Channel id → column name (`CHANNEL_MAP`):

| id | Column | id | Column |
|----|--------|----|--------|
| `0x8C` | `time` | `0x30` | `furnace_temperature` |
| `0x8D` | `sample_temperature` | `0x32` | `furnace_power` |
| `0x8E` | `dsc_signal` | `0x33` | `h_foil_temperature` |
| `0x90` | `mass` | `0x34` | `uc_module` |
| `0x9C` | `purge_flow_1` | `0x35` | `environmental_pressure` |
| `0x9D` | `purge_flow_2` | `0x36`–`0x38` | `environmental_acceleration_x/y/z` |
| `0x9E` | `protective_flow` | | |

Channel presence varies by configuration: the 2022 fixture has all three MFC
flow channels; the 2025 fixtures lack `0x9D` (`purge_flow_2`) — its flow
setpoint metadata is then the only record of that flow.

## Streams 4, 5, 6: modeled but not extracted

`load_document` tokenizes these fully (dtypes `0x14`/`0x48` surface as raw
bytes); no metadata or data is extracted from them yet. What they contain
(see `FORMAT_FINDINGS.md` for the extraction backlog):

- **stream_4** (~34 KB): end-of-run snapshot of the same table families as
  stream 1 — measurement-end timestamp, run counter, acquisition PC FQDN.
- **stream_5** (~12 KB): temperature-band residence histograms — furnace
  usage/wear telemetry.
- **stream_6** (~215–283 KB): two embedded Windows EMF vector images (the
  Proteus plot previews) inside `0x10E4` byte arrays, plus 16-byte dtype-`0x48`
  hash records with counts.

## Corruption semantics and coverage

Severity policy lives in the consumers, not the tokenizer:

- **Data streams (2/3)**: any `malformed` or `truncated` span fails hard —
  `build_dataframe` raises `NGBCorruptedFileError` before assembly, as do
  data-before-header and segment-length mismatches.
- **Metadata streams (1, 4–6)**: grammar violations log a warning and become
  spans; extraction proceeds (every `FileMetadata` field is optional by
  contract).
- **Container integrity failures** (bad magic, broken directory,
  non-contiguous sections, missing main section) raise
  `NGBCorruptedFileError` regardless of stream.
- **Resource limits** (`ParsingConfig`: `max_stream_size_mb` checked against
  the ZIP member's declared size before decompression,
  `max_array_size_mb` before array allocation, `max_tables_per_stream`)
  raise `NGBResourceLimitError`.

Corruption exceptions carry structured attributes (`stream`, `offset`,
`table_index`, `declared`, `available` / `limit`) rather than encoding
details in prose.

To explore a file's structure, coverage, and unknown fields:

```bash
pyngb inspect file.ngb-ss3                # per-table listing
pyngb inspect file.ngb-ss3 --coverage     # byte accounting: records vs spans
pyngb inspect file.ngb-ss3 --unknown      # unmapped (category, field) census
pyngb inspect a.ngb-ss3 b.ngb-ss3         # cross-file field comparison
```

`NGBDocument.unknown_fields()` gives the same unmapped-field census
programmatically — it is the systematic to-do list for future extraction.

## Column metadata

Each data column in the output table carries metadata:

| Field | Type | Description |
|-------|------|-------------|
| `units` | string | measurement units |
| `processing_history` | list[string] | processing steps applied |
| `source` | string | data source identifier |
| `baseline_subtracted` | bool | mass/DSC only |
| `calibration_applied` | bool | DSC only |

## Discovery methodology

This format was reverse-engineered by hex-dump analysis, cross-file
comparison, and validation against instrument software output. The single
most useful step was recognizing the uniform record grammar: once every byte
is either a record or a classified span, unknown fields become an enumerable
census instead of a search problem.

To validate parsing correctness: cross-check against NETZSCH software
exports, check physical validity (temperature ranges, mass values), and
compare across files (`pyngb inspect` multi-file mode diffs scalar fields
across runs).

## Contributing format knowledge

If you discover new patterns or corrections:

1. Locate the field with `pyngb inspect --unknown` / `--values` and document
   the `(category, field_id, dtype)` triple and observed values.
2. Verify against multiple files (both vintages if possible).
3. Provide test files if possible, and add the mapping to
   `pyngb.format.maps` with a golden-test pin.

## References

- ZIP format specification (RFC 1951, RFC 1952)
- IEEE 754 floating-point standard
- UTF-8 and UTF-16LE encoding standards
- MFC `CArchive` serialization format (the record grammar's ancestry)
