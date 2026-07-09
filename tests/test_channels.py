"""Channel assembly: type-ref state machine over streams 2 and 3.

Golden parity first: on every fixture the assembled frame must match the C0
parity golden's column names, order, row count, and per-column SHA-256 over
canonical little-endian float64 bytes. Synthetic cases pin the machine's
rules — time x60 exactly once, header-identity attribution (a missing MFC
channel yields an absent column, never zeros), and the strict corruption
policy for data streams.
"""

import hashlib
import json
import logging
from pathlib import Path

import numpy as np
import pytest

from pyngb.exceptions import NGBCorruptedFileError
from pyngb.format import DType, build_dataframe, load_document
from support.ngb_builder import (
    build_array,
    build_scalar,
    build_section,
    build_stream,
    build_table,
    write_ngb,
)

FIXTURE_DIR = Path(__file__).parent / "test_files"
GOLDEN_DIR = Path(__file__).parent / "goldens"
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*")) if FIXTURE_DIR.exists() else []

CHANNEL_HEADER_TYPE = 0x2B22
SEGMENT_VALUES_TYPE = 0x2B23


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_build_dataframe_matches_the_parity_golden(fixture: Path) -> None:
    """Column names, order, row count, and bytes all match the legacy stack."""
    golden_path = GOLDEN_DIR / f"{fixture.name}.parity.json"
    assert golden_path.exists(), f"missing parity golden for {fixture.name}"
    golden = json.loads(golden_path.read_text(encoding="utf-8"))

    frame = build_dataframe(load_document(fixture, streams=[2, 3]))
    assert frame.columns == golden["columns"]
    assert frame.height == golden["num_rows"]
    for name in frame.columns:
        digest = hashlib.sha256(
            np.asarray(frame[name].to_numpy(), dtype="<f8").tobytes()
        ).hexdigest()
        assert digest == golden["column_sha256"][name], f"column {name} differs"


def header(channel_id: int, *, high_byte: int = 0x17, class_def: bool = False) -> bytes:
    return build_table(
        (high_byte << 8) | channel_id,
        [build_scalar(0x0FDD, DType.U16, 3)],
        type_ref=CHANNEL_HEADER_TYPE,
        class_def=class_def,
    )


def segment(
    values, *, field_id: int = 0x0F40, dtype: DType = DType.F64, category: int = 0x7530
) -> bytes:
    return build_table(
        category, [build_array(field_id, dtype, values)], type_ref=SEGMENT_VALUES_TYPE
    )


def dataframe_of(tmp_path: Path, tables: list[bytes], stream_id: int = 2):
    path = write_ngb(
        tmp_path / "synth.ngb-ss3",
        {stream_id: build_stream(stream_id, body=build_section(tables))},
    )
    return build_dataframe(load_document(path))


class TestAssemblyRules:
    def test_time_is_converted_to_seconds_exactly_once(self, tmp_path: Path) -> None:
        frame = dataframe_of(
            tmp_path,
            [header(0x8C, class_def=True), segment([0.0, 0.5, 1.0])],
        )
        assert frame["time"].to_list() == [0.0, 30.0, 60.0]

    def test_f32_channel_uses_field_0f3d(self, tmp_path: Path) -> None:
        frame = dataframe_of(
            tmp_path,
            [
                header(0x8D, class_def=True),
                segment([25.5, 26.0], field_id=0x0F3D, dtype=DType.F32),
            ],
        )
        values = frame["sample_temperature"].to_numpy()
        assert values.dtype == np.float64
        assert values.tolist() == [
            float(np.float32(25.5)),
            float(np.float32(26.0)),
        ]

    def test_segments_concatenate_in_stream_order(self, tmp_path: Path) -> None:
        frame = dataframe_of(
            tmp_path,
            [
                header(0x90, class_def=True),
                segment([1.0, 2.0], category=0x7530),
                segment([3.0], category=0x7531),
            ],
        )
        assert frame["mass"].to_list() == [1.0, 2.0, 3.0]

    def test_unmapped_channel_passes_through_as_hex(self, tmp_path: Path) -> None:
        frame = dataframe_of(
            tmp_path,
            [header(0x31, high_byte=0x75, class_def=True), segment([0.0, 0.0])],
        )
        assert frame.columns == ["31"]

    def test_missing_channel_yields_absent_column_not_zeros(
        self, tmp_path: Path
    ) -> None:
        """Attribution is by header identity, never position (2025 files
        lack the purge-2 MFC: purge_flow_2 must be absent)."""
        frame = dataframe_of(
            tmp_path,
            [
                header(0x9C, class_def=True),
                segment([5.0, 5.0]),
                header(0x9E),
                segment([20.0, 20.0]),
            ],
        )
        assert frame.columns == ["purge_flow_1", "protective_flow"]
        assert "purge_flow_2" not in frame.columns

    def test_dataless_trailing_header_is_harmless(self, tmp_path: Path) -> None:
        frame = dataframe_of(
            tmp_path,
            [header(0x90, class_def=True), segment([1.0]), header(0x87)],
        )
        assert frame.columns == ["mass"]

    def test_structural_tables_do_not_flush(self, tmp_path: Path) -> None:
        other = build_table(
            0x1787, [build_scalar(0x0FDD, DType.U16, 1)], type_ref=0x0BB9
        )
        frame = dataframe_of(
            tmp_path,
            [header(0x90, class_def=True), segment([1.0]), other, segment([2.0])],
        )
        assert frame["mass"].to_list() == [1.0, 2.0]

    def test_non_data_arrays_in_segment_tables_are_ignored(
        self, tmp_path: Path
    ) -> None:
        stray = build_table(
            0x7530,
            [build_array(0x0999, DType.F64, [9.0, 9.0])],
            type_ref=SEGMENT_VALUES_TYPE,
        )
        frame = dataframe_of(
            tmp_path, [header(0x90, class_def=True), stray, segment([1.0])]
        )
        assert frame["mass"].to_list() == [1.0]

    def test_duplicate_channel_warns_and_overwrites(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        tables = [
            header(0x90, class_def=True),
            segment([1.0]),
            header(0x90),
            segment([2.0]),
        ]
        with caplog.at_level(logging.WARNING):
            frame = dataframe_of(tmp_path, tables)
        assert frame["mass"].to_list() == [2.0]
        assert "more than once" in caplog.text

    def test_headers_only_yields_empty_frame(self, tmp_path: Path) -> None:
        frame = dataframe_of(tmp_path, [header(0x90, class_def=True)])
        assert frame.is_empty()


class TestCorruptionPolicy:
    def test_data_before_any_header(self, tmp_path: Path) -> None:
        tables = [
            build_table(
                0x7530,
                [build_array(0x0F40, DType.F64, [1.0])],
                type_ref=SEGMENT_VALUES_TYPE,
                class_def=True,
            )
        ]
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            dataframe_of(tmp_path, tables)
        assert excinfo.value.stream == 2

    def test_channel_length_mismatch(self, tmp_path: Path) -> None:
        tables = [
            header(0x90, class_def=True),
            segment([1.0, 2.0]),
            header(0x8C),
            segment([1.0]),
        ]
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            dataframe_of(tmp_path, tables)
        assert excinfo.value.stream == 2
        assert excinfo.value.declared == 1
        assert excinfo.value.available == 2

    def test_malformed_span_in_a_data_stream_is_fatal(self, tmp_path: Path) -> None:
        body = (
            build_section([header(0x90, class_def=True), segment([1.0])])
            + b"\xde\xad\xbe\xef" * 3
        )
        path = write_ngb(tmp_path / "bad.ngb-ss3", {2: build_stream(2, body=body)})
        doc = load_document(path)
        assert doc.has_defect(2)  # precondition: the corruption is visible
        with pytest.raises(NGBCorruptedFileError) as excinfo:
            build_dataframe(doc)
        assert excinfo.value.stream == 2
        assert excinfo.value.offset is not None

    def test_pristine_input_parses(self, tmp_path: Path) -> None:
        """Every corruption case above starts from this passing baseline."""
        frame = dataframe_of(
            tmp_path, [header(0x90, class_def=True), segment([1.0, 2.0])]
        )
        assert frame["mass"].to_list() == [1.0, 2.0]
