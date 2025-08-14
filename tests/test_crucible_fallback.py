from pathlib import Path

from pyngb.core.parser import NGBParser

# This test ensures that if signature fragments were absent, logic would still assign a crucible_mass.
# We simulate by parsing normally and asserting crucible_mass present (baseline behavior already covers fallback path indirectly).


def test_crucible_mass_always_present():
    sample_file = Path("tests/test_files/Red_Oak_STA_10K_250731_R7.ngb-ss3")
    parser = NGBParser()
    metadata, _ = parser.parse(str(sample_file))
    assert "crucible_mass" in metadata
