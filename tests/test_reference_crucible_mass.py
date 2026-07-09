from pathlib import Path
from typing import Any

from pyngb import read_ngb_metadata


def test_crucible_and_reference_crucible_mass_extraction() -> Any:
    sample_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
    metadata = read_ngb_metadata(str(sample_file))
    assert "crucible_mass" in metadata
    assert "reference_crucible_mass" in metadata
    # Values should differ
    assert metadata["crucible_mass"] != metadata["reference_crucible_mass"]
    # Reference mass expected to be larger (tare + lid, etc.)
    assert metadata["reference_crucible_mass"] > metadata["crucible_mass"]
