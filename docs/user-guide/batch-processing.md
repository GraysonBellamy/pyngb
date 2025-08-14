# Batch Processing

Process multiple files efficiently.

## Process a directory

```python
from pyngb import process_directory

results = process_directory("./data", pattern="*.ngb-ss3", output_format="parquet")
print(len(results))
```
