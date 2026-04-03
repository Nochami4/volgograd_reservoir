"""Build a transparent raw water-level table from shoreline workbooks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from .common import (
    INTERIM_DIR,
    PROCESSED_DIR,
    RAW_DIR,
    clean_text,
    find_input_file,
    normalize_site_id,
    normalize_site_name,
    parse_number,
    relative_to_root,
    setup_logging,
)

OUTPUT_COLUMNS = [
    "water_obs_id",
    "site_id",
    "site_name",
    "obs_date",
    "year",
    "level_col_1_m",
    "level_col_2_m",
    "source_file",
    "source_sheet",
    "source_row",
    "is_missing",
    "missing_reason",
    "qc_flag",
    "qc_note",
    "preferred_level_col",
]


def find_water_block(rows: list[tuple[object, ...]]) -> tuple[int, int] | None:
    """Return the row and column where the water block begins."""

    for row_idx, row_values in enumerate(rows[:25], start=1):
        for col_idx, value in enumerate(row_values, start=1):
            cell_value = clean_text(value)
            if cell_value == "Годы":
                return row_idx, col_idx
    return None


def detect_preferred_level_col(rows: list[dict[str, object]]) -> str | None:
    """Choose the technically more complete neutral level column without semantic naming."""

    count_1 = sum(1 for row in rows if row["level_col_1_m"] is not None)
    count_2 = sum(1 for row in rows if row["level_col_2_m"] is not None)
    if count_1 == count_2 == 0:
        return None
    return "level_col_1_m" if count_1 >= count_2 else "level_col_2_m"


def build_water_levels_raw(output_path: Path | None = None) -> Path:
    """Build `water_levels_raw.csv` and a sheet-level profile report."""

    source_path = find_input_file(RAW_DIR / "shoreline", "Скорость*в год.xlsx")
    output_path = output_path or PROCESSED_DIR / "water_levels_raw.csv"
    profile_path = INTERIM_DIR / "water_levels_profile.json"
    logger = setup_logging("build_water_levels_raw")

    workbook = load_workbook(source_path, read_only=True, data_only=True)
    records: list[dict[str, object]] = []
    profile_rows: list[dict[str, object]] = []

    for sheet_name in workbook.sheetnames:
        if sheet_name.startswith("Сводная") or sheet_name == "Лист1":
            continue
        ws = workbook[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        block = find_water_block(rows)
        if block is None:
            continue

        header_row, year_col = block
        sheet_records: list[dict[str, object]] = []
        sheet_stats = {
            "sheet_name": sheet_name,
            "rows_scanned": max(len(rows) - header_row, 0),
            "recognized_years": 0,
            "rows_with_1_numeric_col": 0,
            "rows_with_2_numeric_cols": 0,
            "problem_rows": 0,
        }

        for row_idx, row_values in enumerate(rows[header_row:], start=header_row + 1):
            row_values = list(row_values)
            year_raw = row_values[year_col - 1] if year_col - 1 < len(row_values) else None
            year = parse_number(year_raw)
            level_raw_1 = row_values[year_col] if year_col < len(row_values) else None
            level_raw_2 = row_values[year_col + 1] if year_col + 1 < len(row_values) else None
            level_1 = parse_number(level_raw_1)
            level_2 = parse_number(level_raw_2)

            nonempty_row = any(clean_text(value) for value in row_values)
            if year is None and not nonempty_row:
                continue

            qc_flags: list[str] = []
            qc_notes: list[str] = []

            if year is None:
                if nonempty_row:
                    sheet_stats["problem_rows"] += 1
                continue

            sheet_stats["recognized_years"] += 1
            numeric_count = int(level_1 is not None) + int(level_2 is not None)
            if numeric_count == 1:
                sheet_stats["rows_with_1_numeric_col"] += 1
            elif numeric_count >= 2:
                sheet_stats["rows_with_2_numeric_cols"] += 1
                qc_flags.append("AMBIGUOUS_LEVEL_COLUMNS")
                qc_notes.append("Two neutral numeric level columns were retained without assigning semantics.")
            else:
                qc_flags.append("MISSING_LEVEL_VALUES")
                qc_notes.append("Year recognized but both neutral level columns were empty or non-numeric.")

            raw_text_candidates = [clean_text(level_raw_1), clean_text(level_raw_2)]
            if any(value and parse_number(value) is None for value in raw_text_candidates):
                qc_flags.append("NON_NUMERIC_LEVEL_TEXT")
                qc_notes.append("At least one level cell contained non-numeric text or symbols.")
                sheet_stats["problem_rows"] += 1

            extra_nonempty = [
                clean_text(value)
                for col_idx, value in enumerate(row_values, start=1)
                if col_idx not in {year_col, year_col + 1, year_col + 2} and clean_text(value)
            ]
            if extra_nonempty:
                qc_flags.append("EXTRA_ROW_CONTENT")
                qc_notes.append(f"Extra non-empty cells outside the neutral water columns: {' | '.join(extra_nonempty[:4])}")
                sheet_stats["problem_rows"] += 1

            sheet_records.append(
                {
                    "water_obs_id": f"{normalize_site_id(sheet_name)}_{int(year)}_{row_idx}",
                    "site_id": normalize_site_id(sheet_name),
                    "site_name": normalize_site_name(sheet_name),
                    "obs_date": None,
                    "year": int(year),
                    "level_col_1_m": level_1,
                    "level_col_2_m": level_2,
                    "source_file": relative_to_root(source_path),
                    "source_sheet": sheet_name,
                    "source_row": row_idx,
                    "is_missing": numeric_count == 0,
                    "missing_reason": "all_level_columns_missing" if numeric_count == 0 else None,
                    "qc_flag": ";".join(dict.fromkeys(qc_flags)) if qc_flags else None,
                    "qc_note": " ".join(dict.fromkeys(qc_notes)) if qc_notes else None,
                    "preferred_level_col": None,
                }
            )

        preferred_col = detect_preferred_level_col(sheet_records)
        for record in sheet_records:
            record["preferred_level_col"] = preferred_col
            if preferred_col is not None and record["qc_note"]:
                record["qc_note"] = f"{record['qc_note']} Preferred neutral column by completeness: {preferred_col}."
            elif preferred_col is not None:
                record["qc_note"] = f"Preferred neutral column by completeness: {preferred_col}."

        records.extend(sheet_records)
        profile_rows.append(
            {
                **sheet_stats,
                "preferred_level_col": preferred_col,
            }
        )

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS).sort_values(["site_id", "year", "source_row"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps(profile_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Built %s with %s rows", relative_to_root(output_path), len(result))
    logger.info("Wrote water-level profile report to %s", relative_to_root(profile_path))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "water_levels_raw.csv")
    args = parser.parse_args()
    build_water_levels_raw(output_path=args.output)


if __name__ == "__main__":
    main()
