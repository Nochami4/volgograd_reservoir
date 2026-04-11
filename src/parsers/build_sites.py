"""Build the processed site metadata table."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .common import (
    PROCESSED_DIR,
    RAW_DIR,
    clean_text,
    find_input_file,
    normalize_site_name,
    normalize_site_id,
    orientation_to_deg,
    relative_to_root,
    setup_logging,
)

OUTPUT_COLUMNS = [
    "site_id",
    "site_name",
    "shore_type",
    "shore_orientation_text",
    "shore_orientation_deg",
    "exposure_sectors_text",
    "lithology_text",
    "lithology_class",
    "notes",
    "source_file",
    "source_sheet",
]


def build_sites(output_path: Path | None = None) -> Path:
    """Build `sites.csv` from the site metadata workbook."""

    source_path = find_input_file(RAW_DIR / "site_metadata", "Литология*берега*.xlsx")
    output_path = output_path or PROCESSED_DIR / "sites.csv"
    logger = setup_logging("build_sites")

    workbook = pd.ExcelFile(source_path, engine="openpyxl")
    records: list[dict[str, object]] = []

    for sheet_name in workbook.sheet_names:
        df = pd.read_excel(source_path, sheet_name=sheet_name, header=None, dtype=object, engine="openpyxl")
        if df.empty:
            continue

        header_idx = None
        for idx, row in df.iterrows():
            row_text = " | ".join(clean_text(value) for value in row.tolist())
            if "Название участка" in row_text:
                header_idx = idx
                break
        if header_idx is None:
            continue

        for _, row in df.iloc[header_idx + 1 :].iterrows():
            site_name = clean_text(row.iloc[1] if len(row) > 1 else "")
            if not site_name:
                continue
            canonical_site_name = normalize_site_name(site_name)
            notes = " | ".join(clean_text(value) for value in row.iloc[7:].tolist() if clean_text(value))
            record = {
                "site_id": normalize_site_id(canonical_site_name),
                "site_name": canonical_site_name,
                "shore_type": clean_text(row.iloc[2] if len(row) > 2 else ""),
                "shore_orientation_text": clean_text(row.iloc[3] if len(row) > 3 else ""),
                "shore_orientation_deg": orientation_to_deg(row.iloc[3] if len(row) > 3 else ""),
                "exposure_sectors_text": clean_text(row.iloc[4] if len(row) > 4 else ""),
                "lithology_text": clean_text(row.iloc[5] if len(row) > 5 else ""),
                "lithology_class": clean_text(row.iloc[6] if len(row) > 6 else ""),
                "notes": notes or None,
                "source_file": relative_to_root(source_path),
                "source_sheet": sheet_name,
            }
            records.append(record)

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS).drop_duplicates(subset=["site_id"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("Built %s with %s rows", relative_to_root(output_path), len(result))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "sites.csv")
    args = parser.parse_args()
    build_sites(output_path=args.output)


if __name__ == "__main__":
    main()
