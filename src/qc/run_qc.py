"""Run basic QC checks on generated datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.parsers.common import INTERIM_DIR, MAX_REASONABLE_WIND_YEAR, MIN_REASONABLE_WIND_YEAR, PROCESSED_DIR, REPORTS_DIR, relative_to_root, setup_logging

KEY_COLUMNS = {
    "analysis_ready.csv": ["interval_id"],
    "base_points.csv": ["base_point_id"],
    "interval_metrics.csv": ["interval_id"],
    "profiles.csv": ["profile_id"],
    "shoreline_observations.csv": ["obs_id"],
    "sites.csv": ["site_id"],
    "water_levels_raw.csv": ["water_obs_id"],
    "wind_obs_hourly.csv": ["wind_obs_id"],
}


def qc_for_file(path: Path) -> dict[str, object]:
    """Compute QC metrics for one CSV file."""

    df = pd.read_csv(path)
    key_columns = KEY_COLUMNS.get(path.name, [])
    duplicates = int(df.duplicated().sum())
    duplicate_keys = int(df.duplicated(subset=key_columns).sum()) if key_columns else None
    missing_share = {
        column: (float(df[column].isna().mean()) if len(df) else None)
        for column in df.columns
    }
    extra_checks: list[dict[str, object]] = []
    blockers: list[str] = []

    if path.name == "wind_obs_hourly.csv" and not df.empty:
        year_numeric = pd.to_numeric(df["year"], errors="coerce")
        bad_year_mask = year_numeric.notna() & ((year_numeric < MIN_REASONABLE_WIND_YEAR) | (year_numeric > MAX_REASONABLE_WIND_YEAR))
        extra_checks.append(
            {
                "name": "wind_year_out_of_range",
                "count": int(bad_year_mask.sum()),
                "details": f"Accepted range: {MIN_REASONABLE_WIND_YEAR}..{MAX_REASONABLE_WIND_YEAR}.",
            }
        )
        mandatory_dt_mask = year_numeric.notna() & pd.to_numeric(df["hour"], errors="coerce").notna() & df["obs_datetime"].isna()
        extra_checks.append(
            {
                "name": "missing_obs_datetime_when_date_and_hour_present",
                "count": int(mandatory_dt_mask.sum()),
                "details": "Rows with parsed year and hour should carry obs_datetime unless flagged invalid upstream.",
            }
        )
        if int(bad_year_mask.sum()) > 0:
            blockers.append("Wind layer still contains years outside the accepted range.")

    if path.name == "base_points.csv":
        extra_checks.append(
            {
                "name": "base_points_empty",
                "count": int(len(df) == 0),
                "details": "An empty base_points layer means shoreline geometry remains only partially anchored.",
            }
        )
        if len(df) == 0:
            blockers.append("base_points.csv is empty.")
        unresolved_site_share = float(df["site_id"].isna().mean()) if len(df) else 1.0
        extra_checks.append(
            {
                "name": "base_points_missing_site_id_share",
                "count": None,
                "details": f"Share of base-point rows without resolved site_id: {unresolved_site_share:.3f}.",
            }
        )

    if path.name == "water_levels_raw.csv" and not df.empty:
        ambiguous_share = float(df["qc_flag"].fillna("").str.contains("AMBIGUOUS_LEVEL_COLUMNS", regex=False).mean())
        extra_checks.append(
            {
                "name": "ambiguous_level_columns_share",
                "count": None,
                "details": f"Share of rows with ambiguous neutral level columns: {ambiguous_share:.3f}.",
            }
        )
        if ambiguous_share > 0:
            blockers.append("Water level columns remain semantically ambiguous; only neutral technical aggregation is currently safe.")

    if path.name == "analysis_ready.csv" and not df.empty:
        low_wind_share = float(df["qc_flag_analysis"].fillna("").str.contains("LOW_COVERAGE_WIND", regex=False).mean())
        low_water_share = float(df["qc_flag_analysis"].fillna("").str.contains("LOW_COVERAGE_WATER", regex=False).mean())
        extra_checks.append(
            {
                "name": "analysis_low_coverage_wind_share",
                "count": None,
                "details": f"Share of intervals flagged LOW_COVERAGE_WIND: {low_wind_share:.3f}.",
            }
        )
        extra_checks.append(
            {
                "name": "analysis_low_coverage_water_share",
                "count": None,
                "details": f"Share of intervals flagged LOW_COVERAGE_WATER: {low_water_share:.3f}.",
            }
        )
        if low_wind_share > 0.5:
            blockers.append("Wind coverage is below the 0.8 threshold for many shoreline intervals.")

    return {
        "file": relative_to_root(path),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "key_columns": key_columns,
        "duplicate_rows": duplicates,
        "duplicate_keys": duplicate_keys,
        "missing_share": missing_share,
        "extra_checks": extra_checks,
        "critical_blockers": blockers,
    }


def build_markdown_report(results: list[dict[str, object]]) -> str:
    """Render a markdown QC summary."""

    all_blockers = []
    for result in results:
        for blocker in result["critical_blockers"]:
            all_blockers.append((result["file"], blocker))

    lines = [
        "# QC Summary",
        "",
        "## Critical blockers before scientific analysis",
        "",
    ]
    if all_blockers:
        for file_name, blocker in all_blockers:
            lines.append(f"- `{file_name}`: {blocker}")
    else:
        lines.append("- No critical blockers detected by automated QC.")

    lines.extend(
        [
            "",
            "| File | Rows | Columns | Duplicate rows | Duplicate keys | Key columns |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for result in results:
        lines.append(
            f"| `{result['file']}` | {result['rows']} | {result['columns']} | {result['duplicate_rows']} | "
            f"{result['duplicate_keys'] if result['duplicate_keys'] is not None else 'n/a'} | "
            f"{', '.join(result['key_columns']) if result['key_columns'] else 'n/a'} |"
        )

    lines.extend(["", "## Additional Targeted Checks", ""])
    for result in results:
        if not result["extra_checks"]:
            continue
        lines.append(f"### `{result['file']}`")
        lines.append("")
        for check in result["extra_checks"]:
            rendered_count = check["count"] if check["count"] is not None else "n/a"
            lines.append(f"- `{check['name']}`: {rendered_count}. {check['details']}")
        lines.append("")

    lines.extend(["## Missingness By Column", ""])
    for result in results:
        lines.append(f"### `{result['file']}`")
        lines.append("")
        lines.append("| Column | Missing share |")
        lines.append("| --- | ---: |")
        for column, share in sorted(result["missing_share"].items()):
            rendered = f"{share:.3f}" if share is not None else "n/a"
            lines.append(f"| `{column}` | {rendered} |")
        lines.append("")
    return "\n".join(lines)


def run_qc(output_json: Path | None = None, output_md: Path | None = None) -> tuple[Path, Path]:
    """Run QC over all generated processed CSVs."""

    output_json = output_json or INTERIM_DIR / "qc_summary.json"
    output_md = output_md or REPORTS_DIR / "tables" / "qc_summary.md"
    logger = setup_logging("run_qc")

    paths = sorted(PROCESSED_DIR.glob("*.csv"))
    results = [qc_for_file(path) for path in paths]

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(build_markdown_report(results), encoding="utf-8")

    logger.info("Wrote machine-readable QC report to %s", relative_to_root(output_json))
    logger.info("Wrote markdown QC report to %s", relative_to_root(output_md))
    return output_json, output_md


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, default=INTERIM_DIR / "qc_summary.json")
    parser.add_argument("--output-md", type=Path, default=REPORTS_DIR / "tables" / "qc_summary.md")
    args = parser.parse_args()
    run_qc(output_json=args.output_json, output_md=args.output_md)


if __name__ == "__main__":
    main()
