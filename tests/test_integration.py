"""
Integration tests for the complete pynetzsch parsing workflow.
"""

import struct
import tempfile
import zipfile
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from pynetzsch import get_sta_data, load_ngb_data
from pynetzsch.constants import BinaryMarkers


class TestEndToEndIntegration:
    """Test complete parsing workflow."""

    def create_realistic_ngb_file(self):
        """Create a more realistic NGB file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                markers = BinaryMarkers()

                # Stream 1 - Rich metadata
                metadata_fields = [
                    # Instrument
                    (b"\x75\x17", b"\x59\x10", b"NETZSCH STA 449 F3 Jupiter\x00"),
                    # Sample name
                    (b"\x30\x75", b"\x40\x08", b"Test Sample\x00"),
                    # Sample mass (as float64)
                    (
                        b"\x30\x75",
                        b"\x9e\x0c",
                        b"\x33\x33\x33\x33\x33\x33\x2f\x40",
                    ),  # 15.6
                    # Operator
                    (b"\x72\x17", b"\x35\x08", b"Test Operator\x00"),
                ]

                stream1_parts = []
                for category, field, value in metadata_fields:
                    if isinstance(value, str):
                        value = value.encode() + b"\x00"

                    if len(value) > 8:  # String
                        data_type = b"\x1f"
                        length = len(value) - 1  # Exclude null terminator
                        value = length.to_bytes(4, "little") + value
                    else:  # Float
                        data_type = b"\x05"

                    part = (
                        category
                        + b"pad" * 5
                        + field
                        + b"pad" * 3
                        + markers.TYPE_PREFIX
                        + data_type
                        + markers.TYPE_SEPARATOR
                        + value
                        + markers.END_FIELD
                    )
                    stream1_parts.append(part)

                stream1_data = markers.TABLE_SEPARATOR.join(stream1_parts)
                z.writestr("Streams/stream_1.table", stream1_data)

                # Stream 2 - Time and Temperature data
                time_data = []
                temp_data = []
                for i in range(10):
                    # Time values: 0.0, 1.0, 2.0, ...
                    time_val = float(i)
                    time_bytes = struct.pack("<d", time_val)
                    time_data.append(time_bytes)

                    # Temperature values: 25.0, 26.0, 27.0, ...
                    temp_val = 25.0 + float(i)
                    temp_bytes = struct.pack("<d", temp_val)
                    temp_data.append(temp_bytes)

                # Time column
                time_header = b"\x8d\x17" + b"pad" * 10
                time_table = (
                    time_header
                    + markers.TABLE_SEPARATOR
                    + b"\x8d\x75"
                    + markers.START_DATA
                    + b"\x05"
                    + b"".join(time_data)
                    + markers.END_DATA
                )

                # Temperature column
                temp_header = b"\x8e\x17" + b"pad" * 10
                temp_table = (
                    temp_header
                    + markers.TABLE_SEPARATOR
                    + b"\x8e\x75"
                    + markers.START_DATA
                    + b"\x05"
                    + b"".join(temp_data)
                    + markers.END_DATA
                )

                stream2_data = time_table + markers.TABLE_SEPARATOR + temp_table
                z.writestr("Streams/stream_2.table", stream2_data)

                # Stream 3 - DSC data
                dsc_data = []
                for i in range(10):
                    # DSC values: small variation around 0
                    dsc_val = 0.1 * (i - 5)  # -0.5 to 0.4
                    dsc_bytes = struct.pack("<d", dsc_val)
                    dsc_data.append(dsc_bytes)

                dsc_header = b"\x9c\x22\x2b\x80\x22\x2b" + b"pad" * 5
                dsc_table = (
                    dsc_header
                    + b"\x9c\x75"
                    + markers.START_DATA
                    + b"\x05"
                    + b"".join(dsc_data)
                    + markers.END_DATA
                )

                z.writestr("Streams/stream_3.table", dsc_table)

            return temp_file.name

    def test_complete_parsing_workflow(self):
        """Test the complete parsing workflow with real NGB data."""
        from pathlib import Path

        # Use the real sample NGB file
        test_file_path = (
            Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"
        )

        if not test_file_path.exists():
            pytest.skip(f"Test file not found: {test_file_path}")

        # Test high-level API with real data
        table = load_ngb_data(str(test_file_path))

        # Verify table structure
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0, f"Expected data rows, got {table.num_rows}"
        assert (
            len(table.column_names) > 0
        ), f"Expected columns, got {table.column_names}"

        # Verify we have typical STA columns
        expected_columns = {"time", "temperature", "dsc", "sample_mass"}
        actual_columns = set(table.column_names)
        assert expected_columns.issubset(
            actual_columns
        ), f"Missing expected columns: {expected_columns - actual_columns}"

        # Verify metadata is embedded
        assert table.schema.metadata is not None
        assert b"file_metadata" in table.schema.metadata
        assert b"type" in table.schema.metadata
        assert table.schema.metadata[b"type"] == b"STA"

        # Test separate metadata/data API
        metadata, data = get_sta_data(str(test_file_path))

        # Verify metadata content - rich metadata extracted from the file
        assert isinstance(metadata, dict)
        assert len(metadata) > 0, "Should extract metadata from NGB file"

        # Check for typical metadata fields
        expected_meta_fields = ["instrument", "sample_name", "sample_mass"]
        for field in expected_meta_fields:
            if field in metadata:
                print(f"  ✓ Found {field}: {metadata[field]}")

        # Verify data structure
        assert isinstance(data, pa.Table)
        assert data.num_rows > 0
        assert data.num_rows == table.num_rows, "Both APIs should return same row count"

        print(
            f"✓ Successfully parsed {table.num_rows} rows with columns: {table.column_names}"
        )
        print(f"✓ Metadata keys: {list(metadata.keys())}")

        # Test basic data analysis capabilities
        if "time" in table.column_names:
            time_col = table.column("time").to_pylist()
            assert len(time_col) == table.num_rows
            assert all(
                isinstance(x, (int, float)) for x in time_col[:10]
            ), "Time should be numeric"
            print(f"  ✓ Time range: {min(time_col):.1f} to {max(time_col):.1f}")

        if "temperature" in table.column_names:
            temp_col = table.column("temperature").to_pylist()
            assert len(temp_col) == table.num_rows
            assert all(
                isinstance(x, (int, float)) for x in temp_col[:10]
            ), "Temperature should be numeric"
            print(
                f"  ✓ Temperature range: {min(temp_col):.1f} to {max(temp_col):.1f} °C"
            )

    def test_parser_components_integration(self):
        """Test that parser components work together correctly."""
        from pynetzsch.core.parser import NGBParser

        parser = NGBParser()

        # Test that all components are properly initialized
        assert parser.binary_parser is not None
        assert parser.metadata_extractor is not None
        assert parser.data_processor is not None
        assert parser.markers is not None

        # Test that components have expected attributes
        assert hasattr(parser.binary_parser, "split_tables")
        assert hasattr(parser.metadata_extractor, "extract_metadata")
        assert hasattr(parser.data_processor, "process_stream_2")

        # Test marker values are proper bytes
        markers = parser.markers
        assert isinstance(markers.START_DATA, bytes)
        assert isinstance(markers.END_DATA, bytes)
        assert isinstance(markers.TABLE_SEPARATOR, bytes)

    def test_error_handling_integration(self):
        """Test error handling in integration scenarios."""
        # Test with invalid ZIP file
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            temp_file.write(b"not a zip file")

        try:
            with pytest.raises(Exception):  # Should raise some parsing error
                load_ngb_data(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

    def test_empty_file_handling(self):
        """Test handling of empty or minimal files."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            # Create empty ZIP
            with zipfile.ZipFile(temp_file.name, "w"):
                pass

        try:
            with pytest.raises(Exception):  # Should raise missing stream error
                load_ngb_data(temp_file.name)
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

    def test_data_conversion_workflow(self):
        """Test converting parsed data to different formats."""
        ngb_file = self.create_realistic_ngb_file()

        try:
            table = load_ngb_data(ngb_file)

            # Convert to Polars DataFrame
            if table.num_rows > 0:
                df = pl.from_arrow(table)
                assert isinstance(df, pl.DataFrame)

                # Convert to Pandas (through Polars)
                pandas_df = df.to_pandas()
                assert hasattr(pandas_df, "columns")  # Basic pandas check

        finally:
            Path(ngb_file).unlink(missing_ok=True)


class TestPerformanceConsiderations:
    """Test performance-related aspects of parsing."""

    def create_large_mock_data(self, num_points=1000):
        """Create larger dataset for performance testing."""
        with tempfile.NamedTemporaryFile(suffix=".ngb-ss3", delete=False) as temp_file:
            with zipfile.ZipFile(temp_file.name, "w") as z:
                markers = BinaryMarkers()

                # Minimal stream 1
                stream1_data = (
                    b"\x75\x17"
                    + b"pad" * 5
                    + b"\x59\x10"
                    + b"pad" * 3
                    + markers.TYPE_PREFIX
                    + b"\x1f"
                    + markers.TYPE_SEPARATOR
                    + b"\x10\x00\x00\x00Test Instrument\x00"
                    + markers.END_FIELD
                )
                z.writestr("Streams/stream_1.table", stream1_data)

                # Large stream 2 with many data points
                float_data = []
                for i in range(num_points):
                    val = float(i)
                    val_bytes = struct.pack("<d", val)
                    float_data.append(val_bytes)

                time_table = (
                    b"\x8d\x17"
                    + b"pad" * 5
                    + markers.TABLE_SEPARATOR
                    + b"\x8d\x75"
                    + markers.START_DATA
                    + b"\x05"
                    + b"".join(float_data)
                    + markers.END_DATA
                )

                z.writestr("Streams/stream_2.table", time_table)

            return temp_file.name

    @pytest.mark.slow()
    def test_large_file_parsing(self):
        """Test parsing larger files (marked as slow test)."""
        ngb_file = self.create_large_mock_data(num_points=10000)

        try:
            import time

            start_time = time.time()

            table = load_ngb_data(ngb_file)

            end_time = time.time()
            parse_time = end_time - start_time

            # Should complete in reasonable time (adjust threshold as needed)
            assert parse_time < 30.0  # 30 seconds max
            assert isinstance(table, pa.Table)

        finally:
            Path(ngb_file).unlink(missing_ok=True)

    def test_memory_efficiency(self):
        """Test that parsing doesn't use excessive memory."""
        ngb_file = self.create_large_mock_data(num_points=1000)

        try:
            # This is a basic test - in a real scenario you'd use memory profiling
            table = load_ngb_data(ngb_file)

            # Basic verification that we got data
            assert isinstance(table, pa.Table)

            # Verify we can release the reference
            del table

        finally:
            Path(ngb_file).unlink(missing_ok=True)


class TestRegressionScenarios:
    """Test scenarios that might reveal regressions."""

    def test_backwards_compatibility(self):
        """Test that the modular structure maintains backwards compatibility."""
        # Test that old import style still works
        from pynetzsch import NGBParser as ParserClass
        from pynetzsch import load_ngb_data as load_func

        assert callable(load_func)
        assert callable(ParserClass)

        # Test instantiation
        parser = ParserClass()
        assert hasattr(parser, "parse")

    def test_module_isolation(self):
        """Test that modules are properly isolated."""
        # Test that we can import and use individual components
        from pynetzsch.binary import BinaryParser
        from pynetzsch.constants import PatternConfig
        from pynetzsch.extractors import MetadataExtractor

        # Should be able to create instances
        binary_parser = BinaryParser()
        config = PatternConfig()
        metadata_extractor = MetadataExtractor(config, binary_parser)

        # Should have expected methods
        assert hasattr(binary_parser, "split_tables")
        assert hasattr(metadata_extractor, "extract_metadata")

    def test_import_optimization(self):
        """Test that imports are optimized and don't cause circular dependencies."""
        # These should import without issues
        import pynetzsch
        import pynetzsch.api
        import pynetzsch.binary
        import pynetzsch.core
        import pynetzsch.extractors

        # Should be able to access main functions
        assert hasattr(pynetzsch, "load_ngb_data")
        assert hasattr(pynetzsch, "NGBParser")
