"""Build retreat interval metrics from shoreline observations."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.parsers.common import PROCESSED_DIR, relative_to_root, setup_logging

OUTPUT_COLUMNS = [
    "interval_id",
    "site_id",
    "profile_id",
    "date_start",
    "date_end",
    "days_between",
    "years_between",
    "brow_pos_start_m",
    "brow_pos_end_m",
    "retreat_m",
    "retreat_rate_m_per_year",
    "retreat_abs_m",
    "retreat_rate_abs_m_per_year",
    "n_raw_points_used",
    "calc_method",
    "qc_flag",
    "qc_note",
]

CONFLICT_COMPARE_COLUMNS = [
    "measured_point_name",
    "pn_name",
    "raw_measured_distance_m",
    "gp_to_pn_offset_m",
    "brow_position_pn_m",
    "brow_position_raw_m",
    "raw_value_text",
    "is_missing",
    "missing_reason",
]


def compute_interval_metrics(observations: pd.DataFrame) -> pd.DataFrame:
    """Compute interval metrics from a shoreline observation dataframe."""

    df = observations.copy()
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
    df["brow_position_pn_m"] = pd.to_numeric(df["brow_position_pn_m"], errors="coerce")
    duplicate_summary: dict[tuple[str, str], int] = {}
    grouped_keys = df.groupby(["site_id", "profile_id", "obs_date"], dropna=False)
    conflicting_date_keys: set[tuple[str, str, pd.Timestamp]] = set()
    for (site_id, profile_id, obs_date), group in grouped_keys:
        if len(group) <= 1 or pd.isna(obs_date):
            continue
        differing_fields = [
            column
            for column in CONFLICT_COMPARE_COLUMNS
            if column in group.columns and group[column].fillna("__NA__").astype(str).nunique(dropna=False) > 1
        ]
        usable_brow_count = pd.to_numeric(group["brow_position_pn_m"], errors="coerce").notna().sum()
        if differing_fields and usable_brow_count <= 1:
            conflicting_date_keys.add((site_id, profile_id, obs_date))
            duplicate_summary[(site_id, profile_id)] = duplicate_summary.get((site_id, profile_id), 0) + 1

    if conflicting_date_keys:
        conflict_mask = df.apply(
            lambda row: (row["site_id"], row["profile_id"], row["obs_date"]) in conflicting_date_keys,
            axis=1,
        )
        df = df.loc[~conflict_mask].copy()

    df = df.dropna(subset=["obs_date", "brow_position_pn_m"])
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    grouped = (
        df.groupby(["site_id", "profile_id", "obs_date"], dropna=False)
        .agg(
            brow_position_pn_m=("brow_position_pn_m", "mean"),
            n_raw_points_used=("obs_id", "count"),
            source_qc=("qc_flag", lambda series: ";".join(sorted({str(item) for item in series if pd.notna(item) and str(item)}))),
        )
        .reset_index()
        .sort_values(["site_id", "profile_id", "obs_date"])
    )

    records: list[dict[str, object]] = []
    for (site_id, profile_id), group in grouped.groupby(["site_id", "profile_id"], dropna=False):
        group = group.sort_values("obs_date").reset_index(drop=True)
        for idx in range(len(group) - 1):
            start = group.iloc[idx]
            end = group.iloc[idx + 1]
            days_between = int((end["obs_date"] - start["obs_date"]).days)
            if days_between <= 0:
                continue

            years_between = days_between / 365.25
            retreat_m = end["brow_position_pn_m"] - start["brow_position_pn_m"]
            rate = retreat_m / years_between if years_between > 0 else None
            retreat_abs_m = abs(retreat_m)
            retreat_rate_abs = abs(rate) if rate is not None else None

            qc_flags: list[str] = []
            qc_notes: list[str] = []
            if start["n_raw_points_used"] > 1 or end["n_raw_points_used"] > 1:
                qc_flags.append("AGGREGATED_RAW_POINTS")
                qc_notes.append("Brow positions were averaged within each observation date before interval differencing.")
            if start["source_qc"] or end["source_qc"]:
                qc_flags.append("SOURCE_QC_PRESENT")
            excluded_duplicate_dates = duplicate_summary.get((site_id, profile_id), 0)
            if excluded_duplicate_dates > 0:
                qc_flags.append("DUPLICATE_KEY_DATES_EXCLUDED")
                qc_notes.append(
                    f"{excluded_duplicate_dates} conflicting duplicate observation-date key(s) were excluded before interval construction; see data/interim/shoreline_duplicate_report.csv."
                )

            records.append(
                {
                    "interval_id": f"{profile_id}_{start['obs_date'].date().isoformat()}_{end['obs_date'].date().isoformat()}",
                    "site_id": site_id,
                    "profile_id": profile_id,
                    "date_start": start["obs_date"].date().isoformat(),
                    "date_end": end["obs_date"].date().isoformat(),
                    "days_between": days_between,
                    "years_between": years_between,
                    "brow_pos_start_m": start["brow_position_pn_m"],
                    "brow_pos_end_m": end["brow_position_pn_m"],
                    "retreat_m": retreat_m,
                    "retreat_rate_m_per_year": rate,
                    "retreat_abs_m": retreat_abs_m,
                    "retreat_rate_abs_m_per_year": retreat_rate_abs,
                    "n_raw_points_used": int(start["n_raw_points_used"] + end["n_raw_points_used"]),
                    "calc_method": "mean_brow_position_pn_by_date_then_difference",
                    "qc_flag": ";".join(dict.fromkeys(qc_flags)) if qc_flags else None,
                    "qc_note": " ".join(qc_notes) if qc_notes else None,
                }
            )

    return pd.DataFrame(records, columns=OUTPUT_COLUMNS)


def build_interval_metrics(
    observations_path: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """Build `interval_metrics.csv`."""

    observations_path = observations_path or PROCESSED_DIR / "shoreline_observations.csv"
    output_path = output_path or PROCESSED_DIR / "interval_metrics.csv"
    logger = setup_logging("build_interval_metrics")

    observations = pd.read_csv(observations_path)
    result = compute_interval_metrics(observations)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("Built %s with %s rows", relative_to_root(output_path), len(result))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=PROCESSED_DIR / "shoreline_observations.csv")
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "interval_metrics.csv")
    args = parser.parse_args()
    build_interval_metrics(observations_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
