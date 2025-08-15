# Batch Processing

Process multiple files efficiently.

## Process a directory

```python
from pyngb import process_directory

results = process_directory("./data", pattern="*.ngb-ss3", output_format="parquet")
print(len(results))
```

## Use the BatchProcessor class

```python
from pyngb import BatchProcessor

processor = BatchProcessor(max_workers=4)
results = processor.process_directory("./data", pattern="*.ngb-ss3", output_format="both")
successful = [r for r in results if r["status"] == "success"]
print(f"Processed {len(successful)} files successfully")
```
