---
description: pyngb is a Python library for parsing NETZSCH NGB binary files from STA (Simultaneous Thermal Analysis) instruments and dilatometers into Polars and PyArrow tables with embedded metadata for thermal analysis and dilatometry workflows.
---

# pyNGB

pyNGB is an unofficial Python library for parsing and analyzing NETZSCH NGB
binary files from simultaneous thermal analyzers (STA) and push-rod
dilatometers (DIL). It converts instrument output into analysis-ready tables,
preserves embedded metadata, and includes helpers for common thermal analysis
workflows.

## Install

```bash
pip install pyngb
```

## Quick Example

```python
import json

import polars as pl

from pyngb import read_ngb

table = read_ngb("experiment.ngb-ss3")
df = pl.from_arrow(table)
metadata = json.loads(table.schema.metadata[b"file_metadata"])

print(f"Loaded {df.height} rows")
print(f"Sample: {metadata.get('sample_name', 'Unknown')}")
```

## What You Can Do

- Parse `.ngb-ss3` sample files, `.ngb-bs3` baseline files, and `.ngb-ds3`
  Sample + Correction files (raw, correction, or corrected curves)
- Parse dilatometer `.ngb-dla` and `.ngb-cla` files, including the corrected
  length change and dL/L0 that Proteus displays
- Extract metadata, temperature programs, mass data, and instrument details
- Apply baseline correction and derivative thermogravimetry analysis
- Export parsed data to Parquet, CSV, and JSON-friendly metadata
- Process batches of NGB files from Python or the command line

## Start Here

- [Getting Started](getting-started.md)
- [User Guide](user-guide.md)
- [API Reference](api-reference.md)
- [Troubleshooting](troubleshooting.md)

## Disclaimer

pyNGB is not affiliated with, endorsed by, or approved by NETZSCH.
