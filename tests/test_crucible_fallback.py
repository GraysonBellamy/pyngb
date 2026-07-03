"""Crucible-mass extraction: signature classification and its fallbacks.

The MassExtractor distinguishes sample from reference crucible masses by the
binary signature fragments that precede each occurrence. When the sample
signature is absent, extraction must degrade to the first structural
occurrence rather than dropping the field (see
MassExtractor._extract_crucible_masses_structural).
"""

import zipfile
from pathlib import Path

import pytest

from pyngb.binary import BinaryParser
from pyngb.constants import (
    REF_CRUCIBLE_SIG_FRAGMENT,
    SAMPLE_CRUCIBLE_SIG_FRAGMENT,
    PatternConfig,
)
from pyngb.extractors.base import StreamTables
from pyngb.extractors.mass import MassExtractor

FIXTURE = Path(__file__).parent / "test_files" / "Red_Oak_STA_10K_250731_R7.ngb-ss3"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason="real fixture not available"
)


@pytest.fixture(scope="module")
def stream_1_tables() -> list[bytes]:
    with zipfile.ZipFile(FIXTURE) as z:
        data = z.read("Streams/stream_1.table")
    return BinaryParser().split_tables(data)


def extract_masses(tables: list[bytes]) -> dict:
    metadata: dict = {}
    extractor = MassExtractor(PatternConfig(), BinaryParser())
    extractor.extract(StreamTables.wrap(tables), metadata)
    return metadata


def test_signature_classification_on_pristine_stream(stream_1_tables) -> None:
    """With both signatures present, sample and reference are distinguished."""
    metadata = extract_masses(stream_1_tables)

    assert metadata["crucible_mass"] == pytest.approx(253.516, abs=1e-3)
    assert metadata["reference_crucible_mass"] == pytest.approx(256.298, abs=1e-3)
    assert metadata["sample_mass"] == pytest.approx(4.044, abs=1e-3)


def test_fallback_when_sample_signature_absent(stream_1_tables) -> None:
    """Without the sample signature, the first occurrence still fills the field."""
    junk = b"\x00" * len(SAMPLE_CRUCIBLE_SIG_FRAGMENT)
    mutated = [t.replace(SAMPLE_CRUCIBLE_SIG_FRAGMENT, junk) for t in stream_1_tables]

    metadata = extract_masses(mutated)

    # Classification can no longer identify the sample occurrence, so the
    # fallback assigns the first occurrence by byte position (the reference
    # crucible on this fixture) instead of dropping the field.
    assert metadata["crucible_mass"] == pytest.approx(256.298, abs=1e-3)


def test_fallback_when_all_signatures_absent(stream_1_tables) -> None:
    """With no signatures at all, crucible_mass must still be present."""
    mutated = [
        t.replace(
            SAMPLE_CRUCIBLE_SIG_FRAGMENT, b"\x00" * len(SAMPLE_CRUCIBLE_SIG_FRAGMENT)
        ).replace(REF_CRUCIBLE_SIG_FRAGMENT, b"\x00" * len(REF_CRUCIBLE_SIG_FRAGMENT))
        for t in stream_1_tables
    ]

    metadata = extract_masses(mutated)

    assert metadata["crucible_mass"] == pytest.approx(256.298, abs=1e-3)
    assert "reference_crucible_mass" not in metadata
