"""Build a safe first-pass analysis-ready table."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.parsers.common import INTERIM_DIR, PROCESSED_DIR, merge_with_checks, relative_to_root, setup_logging

LOW_COVERAGE_THRESHOLD = 0.8
SITE_SCOPE_COLUMNS = ["site_id", "site_name", "in_project_scope", "scope_status", "scope_note"]
PROJECT_SCOPE_DEFAULTS = {
    "pichuga_yuzhny": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "novonikolskoe": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "proleyskiy": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "nizhniy_balykley": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "burty": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "nizhniy_urakov": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "urakov_bugor": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "molchanovka": (True, "reviewed", "Listed on the project map as Молчановский and matched to site Молчановка."),
    "berezhnovka": (True, "reviewed", "Listed on the project map and in the source-document scope."),
    "suvodskaya": (False, "reviewed", "Present in the metadata workbook but not listed among the nine scoped map/source-document sites."),
}


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


def ensure_site_scope_review(sites_df: pd.DataFrame) -> pd.DataFrame:
    """Create or refresh a manual scope-review file without filtering sites silently."""

    scope_path = INTERIM_DIR / "site_scope_review.csv"
    base_scope = sites_df[["site_id", "site_name"]].drop_duplicates().copy()
    base_scope["in_project_scope"] = base_scope["site_id"].map(lambda site_id: PROJECT_SCOPE_DEFAULTS.get(site_id, (pd.NA, "needs_review", "Scope status still needs review."))[0])
    base_scope["scope_status"] = base_scope["site_id"].map(lambda site_id: PROJECT_SCOPE_DEFAULTS.get(site_id, (pd.NA, "needs_review", "Scope status still needs review."))[1])
    base_scope["scope_note"] = base_scope["site_id"].map(lambda site_id: PROJECT_SCOPE_DEFAULTS.get(site_id, (pd.NA, "needs_review", "Scope status still needs review."))[2])

    if scope_path.exists():
        existing = pd.read_csv(scope_path, dtype=object)
        for column in SITE_SCOPE_COLUMNS:
            if column not in existing.columns:
                existing[column] = pd.NA
        scope_df = merge_with_checks(
            base_scope,
            existing[SITE_SCOPE_COLUMNS],
            on=["site_id", "site_name"],
            how="left",
            validate="one_to_one",
            relationship_name="site_scope_review refresh",
            require_all_left=False,
            suffixes=("_default", ""),
        )
        stale_existing_mask = scope_df["scope_note"].fillna("").astype(str).str.startswith("Auto-initialized from sites.csv")
        scope_df.loc[stale_existing_mask, "in_project_scope"] = pd.NA
        scope_df.loc[stale_existing_mask, "scope_status"] = pd.NA
        scope_df.loc[stale_existing_mask, "scope_note"] = pd.NA
        scope_df["in_project_scope"] = scope_df["in_project_scope"].combine_first(scope_df["in_project_scope_default"])
        scope_df["scope_status"] = scope_df["scope_status"].combine_first(scope_df["scope_status_default"])
        scope_df["scope_note"] = scope_df["scope_note"].combine_first(scope_df["scope_note_default"])
        scope_df = scope_df[SITE_SCOPE_COLUMNS]
    else:
        scope_df = base_scope[SITE_SCOPE_COLUMNS]

    scope_path.parent.mkdir(parents=True, exist_ok=True)
    scope_df.to_csv(scope_path, index=False)
    return scope_df


def build_analysis_ready(output_path: Path | None = None) -> Path:
    """Build `analysis_ready.csv`."""

    output_path = output_path or PROCESSED_DIR / "analysis_ready.csv"
    logger = setup_logging("build_analysis_ready")

    interval_df = pd.read_csv(PROCESSED_DIR / "interval_metrics.csv")
    sites_df = pd.read_csv(PROCESSED_DIR / "sites.csv")
    profiles_df = pd.read_csv(PROCESSED_DIR / "profiles.csv")
    wind_df = pd.read_csv(PROCESSED_DIR / "wind_obs_hourly.csv")
    water_df = prepare_water_levels(pd.read_csv(PROCESSED_DIR / "water_levels_raw.csv"))
    scope_df = ensure_site_scope_review(sites_df)

    interval_df["date_start_dt"] = pd.to_datetime(interval_df["date_start"])
    interval_df["date_end_dt"] = pd.to_datetime(interval_df["date_end"])
    wind_df["obs_date"] = pd.to_datetime(wind_df["obs_date"], errors="coerce")
    wind_df["wind_speed_ms"] = pd.to_numeric(wind_df["wind_speed_ms"], errors="coerce")
    water_df["year"] = pd.to_numeric(water_df["year"], errors="coerce")
    water_df["analysis_level_m"] = pd.to_numeric(water_df["analysis_level_m"], errors="coerce")

    merged = merge_with_checks(
        interval_df,
        sites_df,
        on="site_id",
        how="left",
        validate="many_to_one",
        relationship_name="interval_metrics -> sites",
        suffixes=("", "_site"),
    )
    merged = merge_with_checks(
        merged,
        profiles_df,
        on=["site_id", "profile_id"],
        how="left",
        validate="many_to_one",
        relationship_name="interval_metrics/sites -> profiles",
        suffixes=("", "_profile"),
    )
    merged = merge_with_checks(
        merged,
        scope_df,
        on=["site_id", "site_name"],
        how="left",
        validate="many_to_one",
        relationship_name="interval_metrics/sites/profiles -> site_scope_review",
    )

    wind_aggregates = merged.apply(lambda row: aggregate_wind_for_interval(row, wind_df), axis=1, result_type="expand")
    water_aggregates = merged.apply(lambda row: aggregate_water_for_interval(row, water_df), axis=1, result_type="expand")
    merged = pd.concat([merged, wind_aggregates, water_aggregates], axis=1)

    wind_coverage = merged[
        ["interval_id", "site_id", "profile_id", "date_start", "date_end", "n_wind_obs", "coverage_wind"]
    ].copy()
    wind_coverage["coverage_wind_flag"] = wind_coverage["coverage_wind"].map(
        lambda value: "NO_WIND_OBS"
        if pd.isna(value) or value == 0
        else ("LOW_COVERAGE_WIND" if value < LOW_COVERAGE_THRESHOLD else "SUFFICIENT_COVERAGE_WIND")
    )
    wind_coverage_path = INTERIM_DIR / "wind_coverage_by_interval.csv"
    wind_coverage_path.parent.mkdir(parents=True, exist_ok=True)
    wind_coverage.to_csv(wind_coverage_path, index=False)

    qc_flags = []
    qc_notes = []
    for _, row in merged.iterrows():
        row_flags = []
        row_notes = []
        if pd.notna(row["coverage_wind"]) and row["coverage_wind"] < LOW_COVERAGE_THRESHOLD:
            row_flags.append("LOW_COVERAGE_WIND")
            if row["n_wind_obs"] == 0:
                row_notes.append("No wind observations fall inside this shoreline interval in the currently available local meteorological workbook.")
            else:
                row_notes.append(
                    f"Wind coverage is {row['coverage_wind']:.2f}, below the {LOW_COVERAGE_THRESHOLD:.1f} threshold; interval summaries should be treated as screening-only."
                )
        if pd.notna(row["coverage_water"]) and row["coverage_water"] < LOW_COVERAGE_THRESHOLD:
            row_flags.append("LOW_COVERAGE_WATER")
            row_notes.append(f"Water coverage is {row['coverage_water']:.2f}, below the {LOW_COVERAGE_THRESHOLD:.1f} threshold.")
        if bool(row.get("water_variable_is_ambiguous")):
            row_flags.append("AMBIGUOUS_WATER_VARIABLE")
            row_notes.append("Water metrics use neutral `level_col_*` fields whose physical meaning is still unresolved.")
        if str(row.get("scope_status") or "") == "needs_review":
            row_flags.append("SITE_SCOPE_NEEDS_REVIEW")
            row_notes.append("This site has not yet been explicitly confirmed in `data/interim/site_scope_review.csv`.")
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
