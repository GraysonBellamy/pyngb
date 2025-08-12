# pyNGB Test Suite

This directory contains comprehensive unit tests for the pyNGB library.

## Test Structure

- `conftest.py` - Pytest configuration and shared fixtures
- `test_exceptions.py` - Tests for custom exception classes
- `test_constants.py` - Tests for constants, enums, and configurations
- `test_binary_handlers.py` - Tests for binary data type handlers
- `test_binary_parser.py` - Tests for low-level binary parsing
- `test_api.py` - Tests for high-level API functions
- `test_integration.py` - Integration and end-to-end tests

## Running Tests

### Option 1: Using pytest (recommended)

```bash
# Install test dependencies
uv sync --extra dev

# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run only fast tests (skip slow integration tests)
pytest -m "not slow"

# Run specific test file
pytest tests/test_exceptions.py

# Run with verbose output
pytest -v
```

### Option 2: Using the simple test runner

If pytest is not available, you can run basic tests with:

```bash
python run_tests.py
```

This will run a subset of critical tests without requiring pytest.

## Test Categories

### Unit Tests
- Test individual components in isolation
- Fast execution (< 1 second per test)
- Mock external dependencies
- Cover edge cases and error conditions

### Integration Tests
- Test complete workflows end-to-end
- May take longer to execute
- Use realistic mock data
- Test component interactions

### Performance Tests
- Marked with `@pytest.mark.slow`
- Test parsing of larger datasets
- Memory usage validation
- Skip by default in CI/development

## Test Data

The tests use:
- **Mock binary data**: Realistic NGB file structures created in memory
- **Fixtures**: Reusable test components and data
- **Parameterized tests**: Multiple inputs for thorough coverage

## Coverage Goals

- **Exceptions**: 100% coverage (simple classes)
- **Constants**: 90%+ coverage (configuration testing)
- **Binary parsing**: 85%+ coverage (core functionality)
- **API functions**: 90%+ coverage (user-facing code)
- **Integration**: Key workflows covered

## Writing New Tests

### Test Naming Convention
- Files: `test_<module_name>.py`
- Classes: `Test<ComponentName>`
- Functions: `test_<specific_behavior>`

### Example Test Structure
```python
class TestMyComponent:
    """Test MyComponent class."""

    def test_basic_functionality(self):
        """Test basic usage scenario."""
        component = MyComponent()
        result = component.do_something()
        assert result is not None

    def test_error_handling(self):
        """Test error conditions."""
        component = MyComponent()
        with pytest.raises(SpecificError):
            component.do_invalid_thing()

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Test with empty input, null values, etc.
        pass
```

### Using Fixtures
```python
def test_with_fixture(self, sample_pattern_config):
    """Test using shared fixture."""
    config = sample_pattern_config
    assert "8d" in config.column_map
```

## Continuous Integration

Tests are designed to:
- Run quickly in CI environments
- Not require external dependencies
- Provide clear failure messages
- Support parallel execution

## Debugging Tests

```bash
# Run single test with debugging
pytest tests/test_api.py::TestLoadNGBData::test_basic -v -s

# Show local variables on failure
pytest --tb=long

# Run with Python debugger
pytest --pdb
```

## Mock Data Strategy

Instead of requiring real NGB files, tests create minimal mock data that:
- Contains the essential binary structures
- Exercises the parsing logic
- Is fast to create and parse
- Covers various data scenarios

This approach ensures tests are:
- Self-contained
- Fast to execute
- Easy to understand
- Robust against data file changes
