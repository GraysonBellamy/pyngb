"""The document layer: token stream -> tables -> NGBDocument.

Synthetic cases run on the builder (the tokenizer's dual); the real fixtures
provide the smoke check that every stream of every vintage assembles with no
defects and no orphan fields.
"""

import logging
from pathlib import Path

import numpy as np
import pytest

from pyngb.config import ParsingConfig
from pyngb.exceptions import NGBResourceLimitError
from pyngb.format import DType, load_document
from support.ngb_builder import (
    build_scalar,
    build_section,
    build_stream,
    build_table,
    minimal_ngb,
    write_ngb,
)

FIXTURE_DIR = Path(__file__).parent / "test_files"
ALL_FIXTURES = sorted(FIXTURE_DIR.glob("*.ngb-*")) if FIXTURE_DIR.exists() else []


def one_stream_ngb(path: Path, tables: list[bytes], stream_id: int = 1) -> Path:
    return write_ngb(
        path, {stream_id: build_stream(stream_id, body=build_section(tables))}
    )


class TestAssembly:
    def test_minimal_ngb_assembles(self, tmp_path: Path) -> None:
        doc = load_document(minimal_ngb(tmp_path / "min.ngb-ss3"))
        assert sorted(doc.streams) == [1, 2]

        s1 = doc.tables_of(1)
        assert [t.category for t in s1] == [0x1772, 0x7530]
        assert [t.type_ref for t in s1] == [0x2AFA, 0x2B0C]
        assert [t.index for t in s1] == [0, 1]
        assert s1[0].class_name == "CDbTable"
        assert s1[1].class_name is None
        assert all(t.preamble for t in s1)
        assert not doc.orphans[1]
        assert not doc.orphans[2]
        assert not doc.has_defect(1)
        assert not doc.has_defect(2)

    def test_field_decoding_and_order(self, tmp_path: Path) -> None:
        doc = load_document(minimal_ngb(tmp_path / "min.ngb-ss3"))
        table = doc.tables_of(1)[1]
        # Insertion order == record order.
        assert list(table.fields) == [
            0x0840,
            0x0C9E,
            0x0999,
            0x0998,
            0x0997,
            0x0996,
            0x0995,
            0x04BE,
            0x0994,
        ]
        assert table.value(0x0840) == "Sample A"
        assert table.value(0x0C9E) == 5.25
        assert table.value(0x0998) == 42
        assert table.value(0x0996) == bytes(range(8))
        assert table.value(0x0994) is None  # arrays have no scalar value
        array_field = table.get(0x0994)
        assert array_field is not None
        assert np.array_equal(array_field.array(), np.array([1.0, -2.0, 3.0]))

    def test_multi_section_stream_concatenates_in_directory_order(
        self, tmp_path: Path
    ) -> None:
        doc = load_document(minimal_ngb(tmp_path / "min.ngb-ss3"))
        s2 = doc.tables_of(2)
        # Main section's three tables, then the TOC section's table.
        assert [t.category for t in s2] == [0x178C, 0x7530, 0x7531, 0x0323]
        assert [t.index for t in s2] == [0, 1, 2, 3]

    def test_array_on_scalar_field_raises(self, tmp_path: Path) -> None:
        doc = load_document(minimal_ngb(tmp_path / "min.ngb-ss3"))
        scalar = doc.tables_of(1)[1].get(0x0C9E)
        assert scalar is not None
        with pytest.raises(ValueError, match="scalar"):
            scalar.array()

    def test_duplicate_field_id_keeps_first(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        table = build_table(
            0x1772,
            [
                build_scalar(0x0834, DType.STRING, "first"),
                build_scalar(0x0834, DType.STRING, "second"),
            ],
            class_def=True,
        )
        path = one_stream_ngb(tmp_path / "dup.ngb-ss3", [table])
        with caplog.at_level(logging.WARNING):
            doc = load_document(path)
        assert doc.tables_of(1)[0].value(0x0834) == "first"
        assert "duplicate field 0x0834" in caplog.text

    def test_orphan_field_before_any_open_is_collected(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        body = build_section([]) + build_scalar(0x0834, DType.U16, 7)
        path = write_ngb(tmp_path / "orphan.ngb-ss3", {1: build_stream(1, body=body)})
        with caplog.at_level(logging.WARNING):
            doc = load_document(path)
        assert not doc.tables_of(1)
        assert [f.field_id for f in doc.orphans[1]] == [0x0834]
        assert "precedes any table open" in caplog.text

    def test_table_count_limit(self, tmp_path: Path) -> None:
        tables = [
            build_table(0x1772, [build_scalar(0x0834, DType.U16, i)], class_def=i == 0)
            for i in range(3)
        ]
        path = one_stream_ngb(tmp_path / "many.ngb-ss3", tables)
        assert len(load_document(path).tables_of(1)) == 3  # pristine parses
        with pytest.raises(NGBResourceLimitError) as excinfo:
            load_document(path, limits=ParsingConfig(max_tables_per_stream=2))
        assert excinfo.value.stream == 1
        assert excinfo.value.limit == 2

    def test_malformed_bytes_surface_as_defect(self, tmp_path: Path) -> None:
        table = build_table(
            0x1772, [build_scalar(0x0834, DType.U16, 7)], class_def=True
        )
        body = build_section([table]) + b"\xde\xad\xbe\xef\xde\xad\xbe\xef"
        path = write_ngb(tmp_path / "bad.ngb-ss3", {1: build_stream(1, body=body)})
        doc = load_document(path)
        assert doc.has_defect(1)
        assert doc.defects(1)[0].kind == "malformed"
        # The table before the garbage still assembled.
        assert doc.tables_of(1)[0].value(0x0834) == 7


class TestQueryHelpers:
    @pytest.fixture()
    def doc(self, tmp_path: Path):
        tables = [
            build_table(
                0x1772,
                [build_scalar(0x0834, DType.STRING, "lab A")],
                type_ref=0x0BB9,
                class_def=True,
            ),
            build_table(
                0x7530,
                [
                    build_scalar(0x0840, DType.STRING, "name"),
                    build_scalar(0x0898, DType.STRING, "id"),
                ],
                type_ref=0x0BC6,
            ),
            build_table(
                0x7530, [build_scalar(0x0840, DType.STRING, "other")], type_ref=0x0BC7
            ),
        ]
        return load_document(one_stream_ngb(tmp_path / "q.ngb-ss3", tables))

    def test_by_category(self, doc) -> None:
        assert [t.index for t in doc.by_category(1, 0x7530)] == [1, 2]

    def test_find_with_fields(self, doc) -> None:
        assert [t.index for t in doc.find(1, with_fields=(0x0840,))] == [1, 2]
        assert [t.index for t in doc.find(1, with_fields=(0x0840, 0x0898))] == [1]

    def test_find_type_ref(self, doc) -> None:
        assert [t.index for t in doc.find(1, type_ref=0x0BC7)] == [2]

    def test_first_returns_none_when_absent(self, doc) -> None:
        assert doc.first(1, category=0x9999) is None
        assert doc.first(2) is None  # stream not loaded

    def test_strings(self, doc) -> None:
        assert doc.tables_of(1)[1].strings() == ["name", "id"]

    def test_unknown_fields_excludes_mapped_ids(self, doc) -> None:
        unknown = doc.unknown_fields()
        # 0x0834/0x0840/0x0898 are all in the field map -> nothing unknown.
        assert unknown[1] == []


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=lambda p: p.name)
def test_every_fixture_stream_assembles_cleanly(fixture: Path) -> None:
    """No defects, no orphans, and tables in every stream of every vintage."""
    doc = load_document(fixture)
    assert sorted(doc.streams) == [1, 2, 3, 4, 5, 6]
    for stream_id in doc.streams:
        assert doc.tables_of(stream_id), f"stream {stream_id} has no tables"
        assert not doc.has_defect(stream_id)
        assert not doc.orphans[stream_id]
        # Every table opened with a type ref and unique field ids by
        # construction; spot the invariant that indexes are stream ordinals.
        indexes = [t.index for t in doc.tables_of(stream_id)]
        assert indexes == list(range(len(indexes)))
