# pyngb Examples

This directory contains practical examples demonstrating how to use pyngb for various thermal analysis tasks.

## Example Files

### Basic Usage
- **[basic_parsing.py](basic_parsing.py)**: Simple file parsing and data extraction

### Advanced Features
- **[batch_processing.py](batch_processing.py)**: Processing multiple files efficiently
- **[batch_processing_example.py](batch_processing_example.py)**: Validation + batch + dataset

### Integration Examples
- (Bring your own) Jupyter notebook to try code snippets from docs

### Specialized Use Cases
- See docs User Guide sections for analysis patterns

## Running Examples

### Prerequisites

```bash
# Install pyngb
pip install pyngb

# Optional tools for plotting/notebooks
pip install matplotlib pandas jupyter seaborn
```

### Using Example Data

Some examples use sample NGB files. You can:

1. **Use your own NGB files**: Replace file paths in examples
2. **Download sample files**: Check the test_files directory
3. **Generate mock data**: Some examples create synthetic data

### Running Individual Examples

```bash
# Basic parsing example
python examples/basic_parsing.py

# Batch processing example
python examples/batch_processing.py --input-dir ./data/ --output-dir ./results/

# Interactive notebook
jupyter notebook examples/jupyter_notebook.ipynb
```

## Example Categories

### 🚀 Getting Started
Perfect for new users learning pyngb basics.

### 📊 Data Analysis
Examples showing how to analyze thermal data effectively.

### 🔧 Advanced Usage
Complex scenarios and customization examples.

### 🏭 Production Use
Examples suitable for production environments and automation.

## Contributing Examples

Have a useful example? We'd love to include it!

1. Create a new Python file with a descriptive name
2. Include comprehensive comments and docstrings
3. Add error handling and user-friendly output
4. Update this README with your example
5. Submit a pull request

### Example Template

```python
"""
Example: [Brief Description]

Description:
    [Detailed description of what this example demonstrates]

Requirements:
    - pyngb
    - [other dependencies]

Usage:
    python example_name.py [arguments]

Author: [Your Name]
"""

import pyngb
# ... rest of example
```

## Tips for Using Examples

1. **Read the Comments**: Each example includes detailed explanations
2. **Modify Paths**: Update file paths to match your data location
3. **Check Requirements**: Some examples need additional packages
4. **Handle Errors**: Examples include error handling patterns you can reuse
5. **Experiment**: Try modifying examples to fit your specific needs

## Getting Help

- **Documentation**: [Full documentation](https://graysonbellamy.github.io/pyngb/)
- **Issues**: [Report problems](https://github.com/GraysonBellamy/pyngb/issues)
- **Discussions**: [Ask questions](https://github.com/GraysonBellamy/pyngb/discussions)
