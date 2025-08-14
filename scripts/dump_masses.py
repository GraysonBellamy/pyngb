from pathlib import Path

from pyngb.core.parser import NGBParser

TEST_DIR = Path("tests/test_files")

FILES = [
    "DF_FILED_STA_21O2_10K_220222_R1.ngb-ss3",
    "Red_Oak_STA_10K_250731_R7.ngb-ss3",
    "RO_FILED_STA_N2_10K_250129_R29.ngb-ss3",
]


def main():
    parser = NGBParser()
    for fname in FILES:
        p = TEST_DIR / fname
        meta, _ = parser.parse(str(p))
        print(
            f"{fname} -> sample_mass={meta.get('sample_mass')} crucible_mass={meta.get('crucible_mass')} reference_mass={meta.get('reference_mass')} reference_crucible_mass={meta.get('reference_crucible_mass')}"
        )


if __name__ == "__main__":
    main()
