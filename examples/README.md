# pyngb Examples

This directory contains practical examples demonstrating how to use pyngb for various thermal analysis tasks.

## Example Files

### Basic Usage
- **[basic_parsing.py](basic_parsing.py)**: Simple file parsing and data extraction
- **[data_exploration.py](data_exploration.py)**: Data exploration and basic analysis
- **[plotting_examples.py](plotting_examples.py)**: Creating plots and visualizations

### Advanced Features
- **[batch_processing.py](batch_processing.py)**: Processing multiple files efficiently
- **[custom_validation.py](custom_validation.py)**: Custom data validation rules
- **[dataset_management.py](dataset_management.py)**: Managing collections of NGB files

### Integration Examples
- **[pandas_integration.py](pandas_integration.py)**: Working with Pandas DataFrames
- **[jupyter_notebook.ipynb](jupyter_notebook.ipynb)**: Interactive Jupyter notebook
- **[automation_script.py](automation_script.py)**: Automated processing pipeline

### Specialized Use Cases
- **[temperature_analysis.py](temperature_analysis.py)**: Temperature program analysis
- **[mass_loss_analysis.py](mass_loss_analysis.py)**: TGA mass loss calculations
- **[comparative_analysis.py](comparative_analysis.py)**: Comparing multiple samples

## Running Examples

### Prerequisites

```bash
# Install pyngb with optional dependencies
pip install pyngb[examples]

# Or install dependencies manually
pip install pyngb matplotlib pandas jupyter seaborn
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
