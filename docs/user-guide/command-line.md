# Command Line

Use the CLI for quick conversions and batch tasks.

```bash
# Convert a single file to Parquet
python -m pyngb sample.ngb-ss3 -f parquet

# Convert a single file to CSV
python -m pyngb sample.ngb-ss3 -f csv

# Choose output directory
python -m pyngb sample.ngb-ss3 -f all -o ./out
```
# Multiple files: use a shell loop or BatchProcessor
for f in *.ngb-ss3; do
	python -m pyngb "$f" -f parquet -o ./out
done
