"""Build hourly or sub-daily wind observations from the meteorological workbook."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .common import (
    INTERIM_DIR,
    MAX_REASONABLE_WIND_YEAR,
    MIN_REASONABLE_WIND_YEAR,
    PROCESSED_DIR,
    RAW_DIR,
    clean_text,
    combine_date_and_hour,
    find_input_file,
    is_reasonable_wind_year,
    normalize_wind_direction,
    parse_number,
    relative_to_root,
    safe_parse_date,
    setup_logging,
)

OUTPUT_COLUMNS = [
    "wind_obs_id",
    "station_id",
    "station_name",
    "obs_datetime",
    "obs_date",
    "year",
    "month",
    "day",
    "hour",
    "wind_dir_text",
    "wind_dir_deg",
    "wind_speed_ms",
    "wind_gust_ms",
    "source_file",
    "source_sheet",
    "source_row",
    "is_missing",
    "missing_reason",
    "qc_flag",
    "qc_note",
]

WIND_DATE_MIN = date(MIN_REASONABLE_WIND_YEAR, 1, 1)
WIND_DATE_MAX = date(MAX_REASONABLE_WIND_YEAR, 12, 31)


def parse_sheet_year(sheet_name: str) -> int | None:
    """Extract a year from the sheet name."""

    match = re.fullmatch(r"(20\d{2})", clean_text(sheet_name))
    return int(match.group(1)) if match else None


def parse_explicit_wind_date(value: object) -> tuple[date | None, str | None]:
    """Parse a date value only from explicit date-like cells."""

    if isinstance(value, datetime):
        return value.date(), None
    if isinstance(value, date):
        return value, None

    text = clean_text(value)
    if not text:
        return None, None

    # Do not accept arbitrary numbers from non-date columns as dates.
    if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{2,4}", text) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        parsed = safe_parse_date(text)
        if parsed is not None:
            return parsed, None
        return None, f"Explicit date token {text!r} could not be parsed safely."
    if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{5,}", text):
        return None, f"Explicit date token {text!r} has too many year digits and was rejected."

    if isinstance(value, (int, float)):
        parsed = safe_parse_date(value)
        if parsed is not None:
            return parsed, None

    return None, None


def parse_partial_day_month(value: object, year_hint: int | None) -> tuple[date | None, str | None]:
    """Parse day.month values using the sheet year only when the year is absent."""

    if year_hint is None:
        return None, None

    if isinstance(value, datetime):
        if value.year == 1900:
            try:
                return date(year_hint, value.month, value.day), None
            except ValueError:
                return None, f"Excel partial date {value.isoformat()} was invalid after substituting the sheet year."
        return None, None
    if isinstance(value, date):
        if value.year == 1900:
            try:
                return date(year_hint, value.month, value.day), None
            except ValueError:
                return None, f"Excel partial date {value.isoformat()} was invalid after substituting the sheet year."
        return None, None

    text = clean_text(value)
    if not text:
        return None, None

    if isinstance(value, (int, float)) and parse_number(value) is not None:
        numeric = float(value)
        day = int(numeric)
        month = int(round((numeric - day) * 100))
        try:
            return date(year_hint, month, day), None
        except ValueError:
            return None, f"Partial numeric day.month token {text!r} was invalid."

    match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.?", text)
    if not match:
        return None, None
    day = int(match.group(1))
    month = int(match.group(2))
    try:
        return date(year_hint, month, day), None
    except ValueError:
        return None, f"Partial text day.month token {text!r} was invalid."


def is_reasonable_wind_date(obs_date: date | None) -> bool:
    """Check whether a parsed date falls in the accepted range."""

    return obs_date is not None and WIND_DATE_MIN <= obs_date <= WIND_DATE_MAX


def parse_regular_sheet_row(row_values: list[object], year_hint: int | None) -> dict[str, object]:
    """Parse rows from sheets that store hour/date in the first two columns."""

    hour = parse_number(row_values[0]) if row_values else None
    hour = int(hour) if hour is not None and float(hour).is_integer() and 0 <= int(hour) <= 23 else None
    date_value = row_values[1] if len(row_values) > 1 else None
    explicit_date, date_note = parse_explicit_wind_date(date_value)
    if explicit_date is None:
        partial_date, partial_note = parse_partial_day_month(date_value, year_hint=year_hint)
        explicit_date = partial_date
        date_note = date_note or partial_note
    wind_dir_text, wind_dir_deg = normalize_wind_direction(row_values[2] if len(row_values) > 2 else None)
    if wind_dir_deg is None:
        wind_dir_text = ""
    wind_speed_ms = parse_number(row_values[3] if len(row_values) > 3 else None)
    wind_gust_ms = parse_number(row_values[4] if len(row_values) > 4 else None)
    return {
        "hour": hour,
        "obs_date": explicit_date,
        "wind_dir_text": wind_dir_text,
        "wind_dir_deg": wind_dir_deg,
        "wind_speed_ms": wind_speed_ms,
        "wind_gust_ms": wind_gust_ms,
        "date_note": date_note,
        "year_hint": year_hint,
    }


def parse_2025_sheet_row(row_values: list[object], year_hint: int | None) -> dict[str, object]:
    """Parse rows from the 2025 sheet with day.month values and repeated directions."""

    hour = parse_number(row_values[0]) if row_values else None
    hour = int(hour) if hour is not None and float(hour).is_integer() and 0 <= int(hour) <= 23 else None
    obs_date, date_note = parse_partial_day_month(row_values[1] if len(row_values) > 1 else None, year_hint=year_hint)

    wind_dir_text = ""
    wind_dir_deg = None
    for value in row_values[2:14]:
        wind_dir_text, wind_dir_deg = normalize_wind_direction(value)
        if wind_dir_deg is not None:
            break
    if wind_dir_deg is None:
        wind_dir_text = ""

    wind_speed_ms = None
    wind_gust_ms = None
    for value in row_values[14:]:
        numeric = parse_number(value)
        if numeric is not None:
            if wind_speed_ms is None:
                wind_speed_ms = numeric
            elif wind_gust_ms is None:
                wind_gust_ms = numeric
                break

    return {
        "hour": hour,
        "obs_date": obs_date,
        "wind_dir_text": wind_dir_text,
        "wind_dir_deg": wind_dir_deg,
        "wind_speed_ms": wind_speed_ms,
        "wind_gust_ms": wind_gust_ms,
        "date_note": date_note,
        "year_hint": year_hint,
    }


def looks_like_observation_row(sheet_name: str, row_values: list[object]) -> bool:
    """Filter out continuation and summary rows that are not standalone observations."""

    hour_candidate = parse_number(row_values[0] if row_values else None)
    has_hour = hour_candidate is not None and float(hour_candidate).is_integer() and 0 <= int(hour_candidate) <= 23
    has_date_cell = bool(clean_text(row_values[1] if len(row_values) > 1 else ""))
    if not (has_hour or has_date_cell):
        return False

    if sheet_name == "2025":
        return True
    return True


def is_direction_grid_row(row_values: list[object]) -> bool:
    """Detect non-observation rows that repeat the same direction across many columns."""

    normalized_directions = []
    for value in row_values[2:14]:
        text, degrees = normalize_wind_direction(value)
        if degrees is not None:
            normalized_directions.append(text)
    return len(normalized_directions) >= 4 and len(set(normalized_directions)) == 1


def build_wind_obs_hourly(output_path: Path | None = None) -> Path:
    """Build `wind_obs_hourly.csv` from the meteorological workbook."""

    source_path = find_input_file(RAW_DIR / "meteo", "*ветра.xlsx")
    output_path = output_path or PROCESSED_DIR / "wind_obs_hourly.csv"
    log_path = INTERIM_DIR / "wind_obs_hourly_log.json"
    logger = setup_logging("build_wind_obs_hourly", INTERIM_DIR / "wind_obs_hourly.log")

    workbook = pd.ExcelFile(source_path, engine="openpyxl")
    records: list[dict[str, object]] = []
    counters = {
        "rows_read": 0,
        "valid_observations": 0,
        "rows_with_date_error": 0,
        "rows_with_missing_wind_speed": 0,
        "rows_with_missing_wind_direction": 0,
    }

    for sheet_name in workbook.sheet_names:
        year_hint = parse_sheet_year(sheet_name)
        if year_hint is None:
            continue
        df = pd.read_excel(source_path, sheet_name=sheet_name, header=None, dtype=object, engine="openpyxl")
        if df.empty:
            continue

        for row_idx, row in df.iloc[2:].iterrows():
            row_values = row.tolist()
            counters["rows_read"] += 1
            if not looks_like_observation_row(sheet_name, row_values):
                continue
            if sheet_name != "2025" and is_direction_grid_row(row_values):
                continue

            parsed = (
                parse_2025_sheet_row(row_values, year_hint)
                if sheet_name == "2025" or is_direction_grid_row(row_values)
                else parse_regular_sheet_row(row_values, year_hint)
            )

            qc_flags: list[str] = []
            qc_notes: list[str] = []
            obs_date = parsed["obs_date"]

            if parsed["date_note"]:
                qc_notes.append(parsed["date_note"])

            if obs_date is None:
                qc_flags.append("invalid_datetime")
                if not parsed["date_note"]:
                    qc_notes.append("Observation-like row was retained, but no reliable date could be reconstructed from the date cell.")
                counters["rows_with_date_error"] += 1
            elif not is_reasonable_wind_date(obs_date):
                qc_flags.append("invalid_datetime")
                qc_notes.append(f"Parsed date {obs_date.isoformat()} is outside the accepted range {WIND_DATE_MIN.isoformat()}..{WIND_DATE_MAX.isoformat()}.")
                obs_date = None
                counters["rows_with_date_error"] += 1

            year = obs_date.year if obs_date is not None else None
            month = obs_date.month if obs_date is not None else None
            day = obs_date.day if obs_date is not None else None

            if parsed["wind_speed_ms"] is None:
                qc_flags.append("missing_wind_speed")
                counters["rows_with_missing_wind_speed"] += 1
            if not parsed["wind_dir_text"]:
                qc_flags.append("missing_wind_direction")
                counters["rows_with_missing_wind_direction"] += 1

            obs_datetime = combine_date_and_hour(obs_date, parsed["hour"]) if obs_date is not None else None
            if obs_datetime is not None and not is_reasonable_wind_year(obs_datetime.year):
                qc_flags.append("invalid_datetime")
                qc_notes.append(f"Parsed datetime {obs_datetime.isoformat()} is outside the accepted range.")
                obs_datetime = None

            is_missing = parsed["wind_speed_ms"] is None and not parsed["wind_dir_text"]
            missing_reason = "wind_direction_and_speed_missing" if is_missing else None

            if obs_date is not None and parsed["hour"] is not None and obs_datetime is not None:
                counters["valid_observations"] += 1

            # Keep rows that look like observations even when the datetime is invalid,
            # but clear the derived temporal fields so invalid years never enter the processed layer.
            record = {
                "wind_obs_id": f"kamyshin_{sheet_name}_{row_idx + 1}",
                "station_id": "kamyshin",
                "station_name": "Камышин",
                "obs_datetime": obs_datetime.isoformat() if obs_datetime else None,
                "obs_date": obs_date.isoformat() if obs_date else None,
                "year": year,
                "month": month,
                "day": day,
                "hour": parsed["hour"],
                "wind_dir_text": parsed["wind_dir_text"] or None,
                "wind_dir_deg": parsed["wind_dir_deg"],
                "wind_speed_ms": parsed["wind_speed_ms"],
                "wind_gust_ms": parsed["wind_gust_ms"],
                "source_file": relative_to_root(source_path),
                "source_sheet": sheet_name,
                "source_row": row_idx + 1,
                "is_missing": is_missing,
                "missing_reason": missing_reason,
                "qc_flag": ";".join(dict.fromkeys(qc_flags)) if qc_flags else None,
                "qc_note": " ".join(dict.fromkeys(note for note in qc_notes if note)) if qc_notes else None,
            }

            if any(
                [
                    record["obs_date"],
                    record["hour"] is not None,
                    record["wind_dir_text"],
                    record["wind_speed_ms"] is not None,
                    record["qc_flag"],
                ]
            ):
                records.append(record)

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(json.dumps({"source_file": relative_to_root(source_path), **counters}, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Built %s with %s rows", relative_to_root(output_path), len(result))
    logger.info("Wrote wind parsing log to %s", relative_to_root(log_path))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "wind_obs_hourly.csv")
    args = parser.parse_args()
    build_wind_obs_hourly(output_path=args.output)


if __name__ == "__main__":
    main()
