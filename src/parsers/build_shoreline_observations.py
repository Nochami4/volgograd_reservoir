"""Build the main raw-derived shoreline observation table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .common import (
    INTERIM_DIR,
    PROCESSED_DIR,
    RAW_DIR,
    clean_text,
    find_input_file,
    parse_number,
    relative_to_root,
    safe_parse_date,
    setup_logging,
)
from .profile_workbook import block_row_to_numeric_fields, iter_block_rows

OUTPUT_COLUMNS = [
    "obs_id",
    "site_id",
    "profile_id",
    "obs_date",
    "survey_year",
    "measured_point_name",
    "pn_name",
    "raw_measured_distance_m",
    "gp_to_pn_offset_m",
    "brow_position_pn_m",
    "brow_position_raw_m",
    "raw_value_text",
    "is_missing",
    "missing_reason",
    "qc_flag",
    "qc_note",
    "source_file",
    "source_sheet",
    "source_row",
]


def build_shoreline_observations(output_path: Path | None = None) -> Path:
    """Build `shoreline_observations.csv` and an interim parsing log."""

    source_path = find_input_file(RAW_DIR / "profiles", "*БРОВКИ*.xls")
    output_path = output_path or PROCESSED_DIR / "shoreline_observations.csv"
    log_path = INTERIM_DIR / "shoreline_observations_log.json"
    logger = setup_logging("build_shoreline_observations", INTERIM_DIR / "shoreline_observations.log")

    records: list[dict[str, object]] = []
    counters = {
        "rows_read": 0,
        "rows_missing_numeric": 0,
        "rows_with_qc_flags": 0,
        "rows_invalid_date": 0,
        "rows_invalid_numeric_text": 0,
    }

    for row in iter_block_rows(source_path):
        counters["rows_read"] += 1
        obs_date = safe_parse_date(row.obs_date)
        numeric_fields = block_row_to_numeric_fields(row)

        raw_value_bits = []
        qc_flags: list[str] = []
        qc_notes: list[str] = []

        if clean_text(row.obs_date) and obs_date is None:
            qc_flags.append("INVALID_DATE")
            raw_value_bits.append(clean_text(row.obs_date))
            counters["rows_invalid_date"] += 1

        for raw_value in [row.raw_measured_distance, row.gp_to_pn_offset, row.brow_position_pn]:
            if clean_text(raw_value) and parse_number(raw_value) is None:
                raw_value_bits.append(clean_text(raw_value))
                if "INVALID_NUMERIC" not in qc_flags:
                    qc_flags.append("INVALID_NUMERIC")
                counters["rows_invalid_numeric_text"] += 1

        if row.note_text:
            qc_notes.append(row.note_text)
            if "ROW_NOTE" not in qc_flags:
                qc_flags.append("ROW_NOTE")

        numeric_values = [
            numeric_fields["raw_measured_distance_m"],
            numeric_fields["gp_to_pn_offset_m"],
            numeric_fields["brow_position_pn_m"],
            numeric_fields["brow_position_raw_m"],
        ]
        is_missing = all(value is None for value in numeric_values)
        missing_reason = "all_numeric_fields_missing" if is_missing else None
        if is_missing:
            counters["rows_missing_numeric"] += 1
            if "MISSING_NUMERIC" not in qc_flags:
                qc_flags.append("MISSING_NUMERIC")

        if qc_flags:
            counters["rows_with_qc_flags"] += 1

        record = {
            "obs_id": f"{row.block.profile_id}_{row.source_row}",
            "site_id": row.block.site_id,
            "profile_id": row.block.profile_id,
            "obs_date": obs_date.isoformat() if obs_date else None,
            "survey_year": obs_date.year if obs_date else None,
            "measured_point_name": clean_text(row.measured_point_name) or None,
            "pn_name": clean_text(row.pn_name) or None,
            **numeric_fields,
            "raw_value_text": " | ".join(dict.fromkeys(bit for bit in raw_value_bits if bit)) or None,
            "is_missing": is_missing,
            "missing_reason": missing_reason,
            "qc_flag": ";".join(qc_flags) if qc_flags else None,
            "qc_note": " ".join(qc_notes) if qc_notes else None,
            "source_file": row.block.source_file,
            "source_sheet": row.block.sheet_name,
            "source_row": row.source_row,
        }
        records.append(record)

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    log_payload = {
        "source_file": relative_to_root(source_path),
        "output_file": relative_to_root(output_path),
        **counters,
    }
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(json.dumps(log_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Built %s with %s rows", relative_to_root(output_path), len(result))
    logger.info("Wrote shoreline parsing log to %s", relative_to_root(log_path))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "shoreline_observations.csv")
    args = parser.parse_args()
    build_shoreline_observations(output_path=args.output)


if __name__ == "__main__":
    main()
