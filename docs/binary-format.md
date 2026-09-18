---
description: Reverse-engineered reference for the NETZSCH NGB binary format (STA and push-rod dilatometer) — container layout, section directory, record grammar, data types, table object model, and stream contents.
---

# NGB Binary Format Reference

## Overview

NETZSCH NGB files are proprietary binary files containing thermal analysis
data. This document describes the reverse-engineered format as pyngb parses it:
a ZIP container of streams, each stream a sectioned blob of serialized objects,
every object a **table** of **fields** following one uniform record grammar.

Everything here was verified empirically against six real STA fixtures (two
Proteus vintages, 2022 and 2025, including two baseline files) during the
2026-07 format investigation — 25,075 grammar records across all streams with
zero counter-examples. Numbers quoted below (record censuses, span sizes) come
from those fixtures via `scripts/make_goldens.py census`; regenerate them with
`pyngb inspect` if the fixture set changes.

The same container, grammar and table model carry **push-rod dilatometer**
data. Dilatometer support (2026-09) was verified against eight DIL 402
Expedis Select fixtures (Proteus 8.0.3: four Sample + Correction `.ngb-dla`
files and the four `.ngb-cla` corrections they embed) — every stream of every
file tokenizes with zero malformed or truncated spans — and cross-checked
against Proteus's own CSV exports of the four runs; three of those files
are committed as test fixtures. The dilatometer-specific
structures are described in [Dilatometer metadata](#dilatometer-metadata),
[the channel map](#streams-2-and-3-measurement-channels) and
[the dilatometer correction](#the-dilatometer-correction).

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
same structure. The dilatometer writes `.ngb-dla` (Sample + Correction, the
counterpart of `.ngb-ds3` below) and `.ngb-cla` (correction, the counterpart
of `.ngb-bs3`); no stand-alone dilatometer sample extension was observed.
`read_ngb` requires streams 1 and 2 and uses stream 3 when present;
`read_ngb_metadata` reads stream 1 only; `load_document` models any requested
stream, including 4–6.

The measurement definition table records which kind a file is, independent
of its extension: field `0x103A` of the category-`0x1770` table is 1 for a
correction, 2 for a sample and 3 for Sample + Correction (→
`measurement_type`: `correction` / `sample` / `sample_correction`; the same
table's `0x083F` is 2 for the sample kinds and 4 for corrections).

`.ngb-ds3` ("Sample + Correction") uses the same container but packs **two
complete measurements per stream, back-to-back**: the raw sample run first,
then a verbatim copy of the correction run it was measured against (byte-
identical across every file sharing that correction). In streams 2/3 each
run carries the full channel-header + segment-value sequence; the run
boundary is a channel header repeating a channel already seen, and
`count_runs`/`build_dataframe(doc, run=...)` split there. Stream 1 likewise
appends the correction's calibration-context block after a second section
prologue; metadata extraction is bounded to the sample's blocks. Neither run
is subtracted from the other in the stored data — Proteus applies the
correction at display time.

`.ngb-dla` files follow the same layout. The embedded correction's channel
header and segment tables are byte-identical to the stand-alone `.ngb-cla`
correction file (the structural tables between runs differ), so
`read_ngb(dla, run="correction")` equals `read_ngb(cla)` on all four
fixture pairs. The two runs need not have the same length: in two of the
fixtures the sample run has one more row than its correction.

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
<terminator>
```

The canonical terminator — on every record of the 2022/2025 fixtures — is
the 9-byte `END_FIELD`:

```
01 00 00 00 02 00 01 00 00      END_FIELD
```

with the following `00 03 00`, where present, covered as a `table_trailer`
span. The underlying serialization actually ends records with a run of
6-byte **trailer units** `01 00 00 00 <K u16>` — the canonical 12-byte
ending is unit(K=2) + unit(K=3), chunked as 9+3 for historical parity —
and STA-449F3A `.ngb-ds3` files write variant runs directly: unit(3)
alone, or unit(4) + unit(3), notably throughout their timezone blocks.
Those three forms are the only ones observed across all fixtures, and the
tokenizer matches exactly them — `END_FIELD` first, preserving the
canonical segmentation byte-for-byte. Anything else, including a truncated
`END_FIELD` or an unobserved unit combination, surfaces as a malformed
span, as a drift tripwire.

Two rules with teeth:

- **Array counts are element counts**, not byte counts: the payload occupies
  `count × ITEM_SIZE[dtype]` bytes.
- A scalar's extent is fully determined by its dtype (fixed `ITEM_SIZE`, or
  the string/REF headers below), so the terminator is *verified at the
  computed position*, never searched for. Record anchors occurring inside
  array payloads are therefore harmless — a linear walk never sees them.

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

Ten dtypes are observed across all fixtures and streams (`DType` enum;
counts are strict-grammar records summed over the original six fixtures,
all streams):

| dtype | Type | Item size | Records | Notes |
|-------|------|-----------|---------|-------|
| `0x00` | null (empty scalar) | 0 | — | ds3 timezone blocks; decodes to `None` |
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
| `0x2AFB` | sample descriptor (stream 1) |
| `0x2B06` | stream-2 channel configuration: measuring range |
| `0x2BA9` | stream-3 channel definition: display name |
| `0x2B17` | instrument identity: model string |
| `0x2422` / `0x23F0` | temperature / DSC-sensitivity calibration record |
| `0x2454` / `0x2459` | dilatometer expansion standard / its literature curve |
| `0x2BCF` | dilatometer sample geometry |
| `0x2B07` / `0x2B11` / `0x2B0A` | device definition / per-stage device state / device range (MFC tree, force controller) |
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
| `sample_length` | `0x7530` | `0x0C9F` | positive float (mm; dilatometer L0) |

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
category-`0x01F5` source table, identified by its **type ref**, with the path
in field `0x07D4`: type `0x2422` → `temperature_calibration.record_path`,
type `0x23F0` → `sensitivity_calibration.record_path`. The type ref is what
identifies the record, not the path: besides measured `.ngb-ts3` / `.ngb-es3`
records, Proteus ships identity records — `TCALZERO.TMX` on the
dilatometer, `TCALZERO.TCX` and `SENSZERO.EXX` on the STA-449F3A `.ngb-ds3`
fixtures — whose provenance the pre-0.6 suffix rule missed. (The
`TCALZERO.TMX` record carries twelve metal fixpoints with `measured_c ==
corrected_c` and coefficients `[0, 0, 0]`: an identity calibration.) The
same table carries the conditions of the calibration run, extracted into
both blocks:

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
the run. Files carry many snapshots — every referenced calibration or
correction embeds one from when *that* file was measured (NETZSCH factory
calibrations appear as German-timezone entries worldwide), and the
calibration-context block precedes the main metadata document in stream 1.
The run's own snapshot is the first one **inside the main document** (at or
after the first section-prologue table); a calibration measured under a
different DST state than the run makes the first-in-stream table wrong.

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

### Instrument, measurement kind and measuring ranges

- **Instrument model** — field `0x0432` of the type-`0x2B17` instrument
  table: `NETZSCH DIL 402 Expedis Select`, `NETZSCH STA 449F3`, `NETZSCH
  STA 449 F3 Jupiter` (→ `instrument_model`; absent from the two
  STA-449F3A `.ngb-ds3` fixtures). The same table's `0x083F` is an
  instrument type code (44 = STA 449, 69 = DIL 402 — the number Proteus
  embeds in calibration record file names, `K_44_STA449F3A-…`); not
  extracted. `instrument` (`0x1775`/`0x1059`) is the serial-style string
  (`STA449F3A-0333-M`, `DIL402SEA-0342-L`); its leading letters give the
  `type` tag `read_ngb` writes into the table's schema metadata (`STA`,
  `DIL`).
- **Measurement kind** — see [Container](#container).
- **Measuring ranges** — one type-`0x2B06` channel-configuration table per
  stream-2 channel, categorised like the channel's header; f32 field
  `0x0BBC` is the range in the column's units: 20000 µm for `length_change`
  (the `M.RANGE` of Proteus exports), 35000 mg for `mass`, 5000 µV for
  `dsc_signal` (→ `length_change_range`, `mass_range`, `dsc_range`); force
  channels read 6 N, MFC flows their full scale. `0x0BB9` is 1 on every
  channel stored as f64 and 0 on f32 channels.

### Dilatometer metadata

- **Sample length** — the initial length L0 in mm, field `0x0C9F` of the
  sample descriptor (type `0x2AFB`), the id next to the STA sample mass
  (→ `sample_length`). Blank corrections store 0.0 (rejected, like a zero
  mass). A Sample + Correction file carries two descriptors in its main
  document — the sample's, then the embedded correction's — and the second
  one decides whether the correction was measured blank.
- **Sample geometry** — the category-`0x1857` table (type `0x2BCF`): `0x111B`
  diameter (mm), `0x111D` cross-section (mm², exactly π·d²/4), `0x111E` =
  cross-section / (1000·L0) (derived, not extracted), `0x1120` a shape code
  (3 is the only observed value). Extracted as `sample_diameter` /
  `sample_cross_section` only alongside a positive `sample_length`: the
  three RO_Para corrections carry a stale 7.98 mm diameter with L0 = 0.
- **Expansion standard** — the reference material the push-rod system is
  corrected against, a category-`0x01F5` record of type `0x2454`: `0x0462`
  name (`FUSED SILICA`), `0x0463` source (`NBS 739/1971`), `0x083D`
  comment, `0x0464`/`0x0465` validity range in °C (−200 to 1100), `0x07D4`
  record path (`.scl`), `0x083E` date (Unix time); `0x0461` = 154 equals
  the channel-config id of the unmapped channel `0x82`. The literature curve
  is field `0x04C0` (u8 array) of the adjacent category-`0x01F7` table of
  type `0x2459`:

  ```
  u16 byte_length | u16 n | n × (f32 T [°C], f32 dL/L0)
  ```

  with dL/L0 relative to 20 °C (53 points in the fixtures). Stream 1 holds
  the record in nested copies — the original NETZSCH record (1994,
  `S:\NGBWIN\…\FUSED_SI.SCL`) and the instrument PC's copy (2006,
  `C:\NETZSCH\Proteus80\_Records\cal\Fused_si.scl`), four per run —
  with identical curves; first match wins (→ `expansion_standard`, the
  curve as `{"temperature_c": [...], "expansion": [...]}`).
- **Force setpoint** — the push-rod force controller is device id 58 in the
  [device tree](#mfc-gas-metadata-the-device-tree) (definition category
  `0x1C00`, kind 10, no gas or range). Its per-stage state table is
  followed by a type-`0x2B0A` table carrying the stage's force setpoint in
  `0x10FA` (f32, N), merged into the stage dicts as `force_setpoint`; the
  run-level `force_setpoint` follows the MFC-flow rule (emitted when
  uniform across the body stages). 0.2 N in every fixture.
- **Deliberately not extracted** — a third PID table (category `0x185B`,
  type `0x2BD6`, xp/tn/tv 4/4/4, controller unknown; the furnace/sample PID
  rule is unaffected because it takes the first two), the force hardware
  limits (0.01–3 N), the calibration record's author, the `*.ngb-dla-cc`
  path in field `0x1170`, and the −1000 sentinels in sample-table fields
  `0x0C84`/`0x0C85`. The export header's `MEASMODE Standard Expansion` and
  `CORR. CODE 080` are not stored in the file; Proteus derives them.

### Application and license

Strings of the category-`0x0300` table: the first matching
`Version N.N.N` → `application_version`; the longest multiline string →
`licensed_to`.

## Streams 2 and 3: measurement channels

Data streams hold one **channel header table** per channel followed by that
channel's **segment value tables** — a type_ref state machine, not byte
positions, drives assembly:

- **Header table** — type_ref `0x2B22`. The category's **low byte is the
  channel id** (header categories are `<ch> 17` in stream 2 — `<ch> 18` for
  the dilatometer's dL and force channels `0x82`, `0x4E`, `0x4F` — and
  `<ch> 75` in stream 3; the latter collide with segment categories and are
  disambiguated purely by type_ref). The low byte is unique across both
  streams in every fixture.
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
| `0x8D` | `sample_temperature` | `0x31` | `cooling_power` |
| `0x8E` | `dsc_signal` | `0x32` | `furnace_power` |
| `0x90` | `mass` | `0x33` | `h_foil_temperature` |
| `0x9C` | `purge_flow_1` | `0x34` | `uc_module` |
| `0x9D` | `purge_flow_2` | `0x35` | `environmental_pressure` |
| `0x9E` | `protective_flow` | `0x36`–`0x38` | `environmental_acceleration_x/y/z` |
| `0x8F` | `length_change` (DIL, µm, f64) | | |
| `0x4E` | `force` (DIL, N) | | |
| `0x4F` | `force_setpoint` (DIL, N) | | |

Channel presence varies by configuration: the 2022 fixture has all three MFC
flow channels; the 2025 fixtures lack `0x9D` (`purge_flow_2`) — its flow
setpoint metadata is then the only record of that flow. The dilatometer
fixtures carry `time`, `sample_temperature`, `length_change`, `0x82`,
`force`, `force_setpoint`, `purge_flow_2`, `protective_flow` in stream 2 and
`0x30`–`0x33` in stream 3.

Dilatometer channels, verified against Proteus CSV exports and the plot
preview embedded in stream 6: `0x8F` is the push-rod displacement dL in µm
(zero at the start); `0x4E` is the measured push-rod force, matching the
export's `Force/N` to 1e-4 N; `0x4F` is the force setpoint (constant 0.2 N).
`0x82` (f64) is zero in all twelve runs; its channel configuration gives the
same ±20000 range as dL, and the expansion-standard record's `0x0461`
equals its configuration id, so it may be a second (reference) displacement
— its meaning is unknown and it stays unmapped (column `82`).

**Stream-3 channels are named in the file.** Stream 1 carries one
type-`0x2BA9` definition table per stream-3 channel, categorised like the
channel's header (`0x7530` + ordinal), with the Proteus display name in
field `0x10C3`: `0x30` "Furnace", `0x31` "Cooling", `0x32` "Furnace",
`0x33` "HFoilTemp", `0x34` "uC Module", `0x35` "Air Pressure", `0x36`–`0x38`
"Acc. X/Y/Z" (identical in every fixture). Two class codes (`0x10C4`,
`0x10C9`) group channels by kind: "Cooling" shares the class of the furnace
power (1, 6), so `0x31` is named `cooling_power`; it has never been
observed non-zero.

**Power units.** No unit is stored for the power channels. They are
labelled `%`, heater output as a percentage of maximum, because watts are
physically implausible: the DIL holds 952 °C at 7–12, the STA 449 holds
834 °C at about 37, and the STA-449F3A `.ngb-ds3` runs reach 316 °C at 0.4.
Across all 22 runs the channel never exceeds 100 (maximum 94.1, during a DIL
correction ramp). `cooling_power` takes the same unit by its shared channel
class. The names are display strings and not unique
("Furnace" twice), so `CHANNEL_MAP` stays the naming authority; a test pins
it against the in-file names on every fixture as a drift tripwire.

The flow columns hold exactly the configured flow value for the whole run in
all 22 runs of the 16 fixtures (the 2022 fixture shows one 0 at t = 0, then a
clean step). The files cannot tell a commanded value from a steadily held
MFC readback stored at coarse resolution; Proteus labels the column "Gas
Flow".

## The dilatometer correction

A push-rod dilatometer measures the sample's length change against its own
sample holder and push rod. The correction run is measured **blank** (no
sample, L0 = 0) and records the whole system's expansion; subtracting it
over-corrects by the holder material's expansion along the sample length,
which Proteus restores from the expansion standard's literature curve.
Corrected dL/L0, as Proteus exports it:

```
dL/L0(t) = (dL_sample(t) − dL_correction(t)) / L0 + curve(T_sample(t))
```

- The correction run is aligned to the sample on **time**. Aligning on
  temperature over the heating ramp is 3–10× worse at the maximum (max
  residual 2e-5 to 6e-5 against 5e-6 to 7e-6).
- `curve` is the literature curve, linearly interpolated (cubic, PCHIP and
  Akima make no difference) and used as stored, relative to 20 °C — not
  re-zeroed at the run's start temperature.
- Against the four Proteus exports of the heating ramp (segment 2,
  resampled at 1 °C) the residual has mean ~5e-8, standard deviation
  ~2.3e-6 (about 0.04 µm on a 16 mm sample) and maximum ~7e-6; without the
  correction the error reaches 1.3e-3.

`read_ngb(dla, run="corrected")` applies it: baseline subtraction of
`length_change`, then `apply_expansion_standard` adds `L0 · curve(T)`;
`normalize_to_initial_length` divides by L0. Only blank corrections were
verifiable. A correction measured with a real reference sample presumably
needs a length-scaling term, so a correction with a positive sample length
is refused rather than corrected wrongly.

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

Dilatometer files add a component registry to stream 4 (hardware ids of
sample holders and push rods with last-used dates as OLE dates); not
extracted. No dilatometer file carries a pressure channel or an event/alarm
log.

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
| `baseline_subtracted` | bool | mass, DSC and `length_change` only |
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
