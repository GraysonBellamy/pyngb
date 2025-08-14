"""
CLI tests for the package entry point (__main__) and forwarding to api.loaders.
"""

from unittest.mock import patch


class TestMainModule:
    """Test the __main__ module functionality."""

    @patch("sys.exit")
    @patch("pyngb.api.loaders.main")
    def test_main_module_forwards_to_loaders_main(self, mock_main, mock_exit):
        """Test that __main__ module forwards to api.loaders.main."""

        mock_main.return_value = 0

        # Import the __main__ module
        import pyngb.__main__

        # The module should import the main function
        assert hasattr(pyngb.__main__, "main")
        assert pyngb.__main__.main is mock_main

    def test_main_module_exit_code_propagation(self):
        """Test that __main__ module propagates exit codes."""
        # This is covered by pragma: no cover in the actual code
        # Just test that the main guard exists
        import inspect

        import pyngb.__main__

        source = inspect.getsource(pyngb.__main__)
        assert 'if __name__ == "__main__"' in source
        assert "sys.exit(main())" in source

    def test_main_module_imports(self):
        """Test that __main__ module imports are correct."""
        import pyngb.__main__

        # Should have imported sys
        assert hasattr(pyngb.__main__, "sys")

        # Should have imported main from api.loaders
        assert hasattr(pyngb.__main__, "main")

        # main should be callable
        assert callable(pyngb.__main__.main)


class TestMainModuleStructure:
    """Test the structure of the __main__ module."""

    def test_main_module_structure(self):
        import pyngb.__main__

        # Should have docstring
        assert pyngb.__main__.__doc__ is not None
        assert "entry point" in pyngb.__main__.__doc__.lower()

        # Should import necessary components
        assert hasattr(pyngb.__main__, "sys")
        assert hasattr(pyngb.__main__, "main")


class TestErrorHandlingMainModule:
    """Test error handling signal through __main__ (import-level only)."""

    def test_main_guard_present(self):
        import inspect

        import pyngb.__main__

        source = inspect.getsource(pyngb.__main__)
        assert 'if __name__ == "__main__"' in source
        assert "sys.exit(main())" in source


class TestBackwardsCompatibility:
    """Test backwards compatibility aspects for __main__ only (cli removed)."""

    def test_main_module_interface_unchanged(self):
        """Test that __main__ module interface remains unchanged."""
        import pyngb.__main__

        # Should import main function
        assert hasattr(pyngb.__main__, "main")

        # Should import sys
        assert hasattr(pyngb.__main__, "sys")


class TestDocumentation:
    """Test documentation and help text."""

    def test_main_module_docstring(self):
        """Test that __main__ module has appropriate docstring."""
        import pyngb.__main__

        docstring = pyngb.__main__.__doc__
        assert docstring is not None
        assert "entry point" in docstring.lower()
        assert "python -m pyngb" in docstring
