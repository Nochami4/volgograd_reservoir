"""Build a safe first-pass analysis-ready table."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.parsers.common import PROCESSED_DIR, relative_to_root, setup_logging

LOW_COVERAGE_THRESHOLD = 0.8


def prepare_water_levels(water_df: pd.DataFrame) -> pd.DataFrame:
    """Select a first available numeric level column without guessing source semantics."""

    df = water_df.copy()
    level_columns = [column for column in df.columns if column.startswith("level_col_") and column.endswith("_m")]
    if not level_columns:
        df["analysis_level_m"] = pd.NA
        df["water_variable_is_ambiguous"] = False
        return df

    sorted_level_columns = sorted(level_columns)
    df[sorted_level_columns] = df[sorted_level_columns].apply(pd.to_numeric, errors="coerce")
    df["analysis_level_m"] = df[sorted_level_columns].bfill(axis=1).iloc[:, 0]
    df["water_variable_is_ambiguous"] = True
    return df


def aggregate_wind_for_interval(interval_row: pd.Series, wind_df: pd.DataFrame) -> dict[str, object]:
    """Aggregate wind observations over an interval."""

    subset = wind_df[
        (wind_df["obs_date"] >= interval_row["date_start_dt"])
        & (wind_df["obs_date"] <= interval_row["date_end_dt"])
    ].copy()
    n_days = max((interval_row["date_end_dt"] - interval_row["date_start_dt"]).days + 1, 1)
    coverage = subset["obs_date"].nunique() / n_days if not subset.empty else 0.0
    return {
        "n_wind_obs": int(len(subset)),
        "mean_wind_speed_ms": subset["wind_speed_ms"].mean() if not subset.empty else None,
        "max_wind_speed_ms": subset["wind_speed_ms"].max() if not subset.empty else None,
        "coverage_wind": coverage,
    }


def aggregate_water_for_interval(interval_row: pd.Series, water_df: pd.DataFrame) -> dict[str, object]:
    """Aggregate water observations over an interval using inclusive calendar years."""

    subset = water_df[
        (water_df["site_id"] == interval_row["site_id"])
        & (water_df["year"] >= interval_row["date_start_dt"].year)
        & (water_df["year"] <= interval_row["date_end_dt"].year)
    ].copy()
    years_spanned = max(interval_row["date_end_dt"].year - interval_row["date_start_dt"].year + 1, 1)
    coverage = subset["year"].nunique() / years_spanned if not subset.empty else 0.0
    return {
        "n_water_obs": int(len(subset)),
        "mean_level": subset["analysis_level_m"].mean() if not subset.empty else None,
        "max_level": subset["analysis_level_m"].max() if not subset.empty else None,
        "min_level": subset["analysis_level_m"].min() if not subset.empty else None,
        "range_level": (subset["analysis_level_m"].max() - subset["analysis_level_m"].min()) if not subset.empty else None,
        "coverage_water": coverage,
        "water_qc_note": "Water aggregation uses neutral level_col_* fields without semantic relabeling; interpret only as a draft technical proxy." if not subset.empty else None,
        "water_variable_is_ambiguous": bool(subset["water_variable_is_ambiguous"].any()) if not subset.empty else False,
    }


def build_analysis_ready(output_path: Path | None = None) -> Path:
    """Build `analysis_ready.csv`."""

    output_path = output_path or PROCESSED_DIR / "analysis_ready.csv"
    logger = setup_logging("build_analysis_ready")

    interval_df = pd.read_csv(PROCESSED_DIR / "interval_metrics.csv")
    sites_df = pd.read_csv(PROCESSED_DIR / "sites.csv")
    profiles_df = pd.read_csv(PROCESSED_DIR / "profiles.csv")
    wind_df = pd.read_csv(PROCESSED_DIR / "wind_obs_hourly.csv")
    water_df = prepare_water_levels(pd.read_csv(PROCESSED_DIR / "water_levels_raw.csv"))

    interval_df["date_start_dt"] = pd.to_datetime(interval_df["date_start"])
    interval_df["date_end_dt"] = pd.to_datetime(interval_df["date_end"])
    wind_df["obs_date"] = pd.to_datetime(wind_df["obs_date"], errors="coerce")
    wind_df["wind_speed_ms"] = pd.to_numeric(wind_df["wind_speed_ms"], errors="coerce")
    water_df["year"] = pd.to_numeric(water_df["year"], errors="coerce")
    water_df["analysis_level_m"] = pd.to_numeric(water_df["analysis_level_m"], errors="coerce")

    merged = (
        interval_df.merge(sites_df, on="site_id", how="left", suffixes=("", "_site"))
        .merge(profiles_df, on=["site_id", "profile_id"], how="left", suffixes=("", "_profile"))
    )

    wind_aggregates = merged.apply(lambda row: aggregate_wind_for_interval(row, wind_df), axis=1, result_type="expand")
    water_aggregates = merged.apply(lambda row: aggregate_water_for_interval(row, water_df), axis=1, result_type="expand")
    merged = pd.concat([merged, wind_aggregates, water_aggregates], axis=1)

    qc_flags = []
    qc_notes = []
    for _, row in merged.iterrows():
        row_flags = []
        row_notes = []
        if pd.notna(row["coverage_wind"]) and row["coverage_wind"] < LOW_COVERAGE_THRESHOLD:
            row_flags.append("LOW_COVERAGE_WIND")
            row_notes.append(f"Wind coverage is {row['coverage_wind']:.2f}, below the {LOW_COVERAGE_THRESHOLD:.1f} threshold.")
        if pd.notna(row["coverage_water"]) and row["coverage_water"] < LOW_COVERAGE_THRESHOLD:
            row_flags.append("LOW_COVERAGE_WATER")
            row_notes.append(f"Water coverage is {row['coverage_water']:.2f}, below the {LOW_COVERAGE_THRESHOLD:.1f} threshold.")
        if bool(row.get("water_variable_is_ambiguous")):
            row_flags.append("AMBIGUOUS_WATER_VARIABLE")
        if pd.notna(row.get("water_qc_note")) and row["water_qc_note"]:
            row_notes.append(row["water_qc_note"])
        qc_flags.append(";".join(row_flags) if row_flags else None)
        qc_notes.append(" ".join(row_notes) if row_notes else None)

    merged["qc_flag_analysis"] = qc_flags
    merged["qc_note_analysis"] = qc_notes
    merged = merged.drop(columns=["date_start_dt", "date_end_dt", "water_qc_note"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    logger.info("Built %s with %s rows", relative_to_root(output_path), len(merged))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "analysis_ready.csv")
    args = parser.parse_args()
    build_analysis_ready(output_path=args.output)


if __name__ == "__main__":
    main()
