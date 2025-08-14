from pathlib import Path

from pyngb.core.parser import NGBParser


def test_reference_mass_extraction():
    sample_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
    parser = NGBParser()
    metadata, _ = parser.parse(str(sample_file))
    assert "reference_crucible_mass" in metadata
    # reference_mass may be zero but should exist structurally if reference_crucible_mass found
    assert (
        "reference_mass" in metadata or metadata.get("reference_crucible_mass") is None
    )
