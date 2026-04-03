"""Build the processed profile metadata table."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .common import PROCESSED_DIR, RAW_DIR, find_input_file, relative_to_root, setup_logging
from .profile_workbook import iter_profile_blocks, summarize_profile_block

OUTPUT_COLUMNS = [
    "profile_id",
    "site_id",
    "profile_num",
    "profile_name",
    "sheet_name_raw",
    "start_date",
    "end_date",
    "n_observations",
    "source_file",
]


def build_profiles(output_path: Path | None = None) -> Path:
    """Build `profiles.csv` from the legacy shoreline workbook."""

    source_path = find_input_file(RAW_DIR / "profiles", "*БРОВКИ*.xls")
    output_path = output_path or PROCESSED_DIR / "profiles.csv"
    logger = setup_logging("build_profiles")

    records = [summarize_profile_block(source_path, block) for block in iter_profile_blocks(source_path)]
    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS).sort_values(["site_id", "profile_id"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("Built %s with %s rows from %s", relative_to_root(output_path), len(result), relative_to_root(source_path))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "profiles.csv")
    args = parser.parse_args()
    build_profiles(output_path=args.output)


if __name__ == "__main__":
    main()
