"""First-stage analysis for tasks 1-2: periods and within-site profile correlation."""

from __future__ import annotations

import argparse
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator

from src.parsers.common import INTERIM_DIR, PROCESSED_DIR, REPORTS_DIR, merge_with_checks, relative_to_root, setup_logging

ANALYSIS_SAFE_COLUMNS = [
    "interval_id",
    "site_id",
    "site_name",
    "profile_id",
    "profile_num",
    "profile_name",
    "date_start",
    "date_end",
    "days_between",
    "years_between",
    "retreat_m",
    "retreat_rate_m_per_year",
    "retreat_abs_m",
    "retreat_rate_abs_m_per_year",
    "n_raw_points_used",
    "calc_method",
    "shore_type",
    "shore_orientation_text",
    "shore_orientation_deg",
    "exposure_sectors_text",
    "lithology_text",
    "lithology_class",
    "notes",
    "start_date",
    "end_date",
    "n_observations",
    "in_project_scope",
    "scope_status",
    "scope_note",
    "history_start_year",
    "history_start_group",
    "has_conflicting_shoreline_duplicates",
    "conflicting_duplicate_group_count",
    "duplicate_conflict_obs_dates",
    "duplicate_conflict_note",
    "qc_flag",
    "qc_note",
    "qc_flag_analysis",
    "qc_note_analysis",
    "qc_flag_analysis_safe",
    "qc_note_analysis_safe",
]

DISPLAY_LABELS = {
    "context": {
        "standard": "Обычный контекст наблюдений",
        "conflict": "Контекст с конфликтующими дублями\nв исходном shoreline-слое",
    },
    "overlap_caution_flag": {
        "unstable_small_sample": "Очень мало общих интервалов",
        "limited_overlap": "Общих интервалов мало",
        "adequate_overlap": "Общих интервалов достаточно",
    },
    "correlation_strength": {
        "unstable_small_sample": "Оценка нестабильна из-за малого числа интервалов",
        "insufficient_variation": "Недостаточно вариации для оценки связи",
        "high_but_limited_overlap": "Высокая, но при малом числе общих интервалов",
        "moderate_but_limited_overlap": "Умеренная, но при малом числе общих интервалов",
        "weak_but_limited_overlap": "Слабая, но при малом числе общих интервалов",
        "very_high": "Очень высокая",
        "high": "Высокая",
        "moderate": "Умеренная",
        "weak": "Слабая",
    },
}

FIGURE_STYLE = {
    "font.family": "DejaVu Sans",
    "axes.titlesize": 13,
    "axes.labelsize": 11,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "legend.fontsize": 9.5,
    "figure.titlesize": 15,
    "axes.titleweight": "semibold",
    "axes.edgecolor": "#6F7680",
    "axes.linewidth": 0.8,
    "grid.color": "#CFD6DE",
    "grid.linewidth": 0.7,
    "grid.alpha": 0.7,
    "axes.grid": False,
    "savefig.facecolor": "white",
    "figure.facecolor": "white",
}

PALETTE = {
    "primary": "#456D9C",
    "primary_light": "#8FA7C4",
    "accent": "#B85C5C",
    "neutral": "#7D8792",
    "grid": "#D7DDE5",
    "hist_signed": "#5B7EA4",
    "hist_rate": "#C46C5E",
    "box_fill": "#7EA0BF",
    "box_fill_alt": "#D9B38C",
    "abs_fill": "#AAB7C4",
}

EXPORT_DPI = 260
EXPORT_SIDE_FORMATS = (".svg", ".pdf")


def classify_history_start(year: float | int | None) -> str:
    """Group sites into an early-history and late-history block for task 1."""

    if pd.isna(year):
        return "не определено"
    year_int = int(year)
    if year_int < 1986:
        return "ранний блок (до 1986; в данных старт 1958-1977)"
    return "поздний блок (с 1986/1987)"


def wrap_site_label(label: str, width: int = 12) -> str:
    """Wrap long site labels for figure readability."""

    return "\n".join(textwrap.wrap(str(label), width=width, break_long_words=False, break_on_hyphens=True))


def translate_display_label(group: str, value: str | None) -> str:
    """Translate service labels into human-readable Russian strings."""

    if value is None or pd.isna(value):
        return "Не указано"
    return DISPLAY_LABELS.get(group, {}).get(str(value), str(value))


def normalize_profile_label(profile_name: str | None, profile_id: str | None = None) -> str:
    """Convert raw profile names to compact display labels."""

    value = str(profile_name or profile_id or "Профиль без названия").strip()
    value = value.replace("ПРОФИЛЬ", "Профиль").replace("№ ", "№ ").replace("№", "№ ")
    value = " ".join(value.split())
    return value


def wrap_display_label(label: str, width: int = 18, max_lines: int = 3) -> str:
    """Wrap long labels and keep them compact."""

    wrapped = textwrap.wrap(str(label), width=width, break_long_words=False, break_on_hyphens=True)
    if len(wrapped) <= max_lines:
        return "\n".join(wrapped)
    trimmed = wrapped[: max_lines - 1]
    remainder = " ".join(wrapped[max_lines - 1 :])
    trimmed.append(textwrap.shorten(remainder, width=width, placeholder="…"))
    return "\n".join(trimmed)


def format_site_profile_label(site_name: str, profile_name: str | None, profile_id: str | None = None) -> str:
    """Build a readable combined label for timelines."""

    site_label = wrap_display_label(str(site_name), width=18, max_lines=2)
    profile_label = wrap_display_label(normalize_profile_label(profile_name, profile_id), width=18, max_lines=2)
    return f"{site_label}\n{profile_label}"


def profile_sort_key(profile_num: object, profile_name: object, profile_id: object) -> tuple[float, str]:
    """Return a stable sort key for profiles without changing analytical meaning."""

    numeric = pd.to_numeric(pd.Series([profile_num]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return float(numeric), str(profile_name or profile_id or "")
    return float("inf"), str(profile_name or profile_id or "")


def set_academic_style() -> None:
    """Apply a restrained, report-friendly matplotlib style."""

    plt.rcParams.update(FIGURE_STYLE)


def style_axes(ax: plt.Axes, grid_axis: str = "y") -> None:
    """Apply a consistent axis appearance."""

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis=grid_axis, color=PALETTE["grid"], linewidth=0.8, alpha=0.7)
    ax.set_axisbelow(True)


def export_figure(fig: plt.Figure, output_path: Path) -> None:
    """Save figure with safe margins and archival vector copies."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=EXPORT_DPI, bbox_inches="tight", pad_inches=0.18, facecolor="white")
    for suffix in EXPORT_SIDE_FORMATS:
        fig.savefig(output_path.with_suffix(suffix), bbox_inches="tight", pad_inches=0.18, facecolor="white")


def classify_overlap_caution(n_overlap: int) -> str:
    """Classify caution level for profile-pair overlap."""

    if n_overlap < 3:
        return "unstable_small_sample"
    if n_overlap < 10:
        return "limited_overlap"
    return "adequate_overlap"


def interpret_correlation_strength(value: float | None, n_overlap: int) -> str:
    """Return a restrained qualitative interpretation of correlation strength."""

    caution_flag = classify_overlap_caution(n_overlap)
    if caution_flag == "unstable_small_sample":
        return "unstable_small_sample"
    if value is None or pd.isna(value):
        return "insufficient_variation"
    abs_value = abs(float(value))
    if caution_flag == "limited_overlap":
        if abs_value >= 0.7:
            return "high_but_limited_overlap"
        if abs_value >= 0.4:
            return "moderate_but_limited_overlap"
        return "weak_but_limited_overlap"
    if abs_value >= 0.9:
        return "very_high"
    if abs_value >= 0.7:
        return "high"
    if abs_value >= 0.4:
        return "moderate"
    return "weak"


def load_analysis_safe_subset() -> pd.DataFrame:
    """Build an analysis-safe subset for tasks 1-2 from processed tables."""

    analysis_ready = pd.read_csv(PROCESSED_DIR / "analysis_ready.csv")
    duplicate_report_path = INTERIM_DIR / "shoreline_duplicate_report.csv"
    duplicate_report = pd.read_csv(duplicate_report_path) if duplicate_report_path.exists() else pd.DataFrame()

    subset = analysis_ready.loc[analysis_ready["in_project_scope"].fillna(False).astype(bool)].copy()
    subset["date_start"] = pd.to_datetime(subset["date_start"], errors="coerce")
    subset["date_end"] = pd.to_datetime(subset["date_end"], errors="coerce")

    site_history = (
        subset.groupby(["site_id", "site_name"], dropna=False)["date_start"]
        .min()
        .dt.year.rename("history_start_year")
        .reset_index()
    )
    site_history["history_start_group"] = site_history["history_start_year"].map(classify_history_start)
    subset = merge_with_checks(
        subset,
        site_history,
        on=["site_id", "site_name"],
        how="left",
        validate="many_to_one",
        relationship_name="analysis_ready -> site history start",
    )

    if duplicate_report.empty:
        duplicate_flags = pd.DataFrame(
            columns=[
                "site_id",
                "profile_id",
                "has_conflicting_shoreline_duplicates",
                "conflicting_duplicate_group_count",
                "duplicate_conflict_obs_dates",
                "duplicate_conflict_note",
            ]
        )
    else:
        duplicate_flags = duplicate_report.loc[duplicate_report["resolution"].eq("keep_and_flag_conflict")].copy()
        duplicate_flags["duplicate_conflict_obs_dates"] = duplicate_flags["obs_date"].fillna("missing_obs_date").astype(str)
        duplicate_flags = (
            duplicate_flags.groupby(["site_id", "profile_id"], dropna=False)
            .agg(
                conflicting_duplicate_group_count=("resolution", "size"),
                duplicate_conflict_obs_dates=("duplicate_conflict_obs_dates", lambda values: ";".join(sorted(set(values)))),
            )
            .reset_index()
        )
        duplicate_flags["has_conflicting_shoreline_duplicates"] = True
        duplicate_flags["duplicate_conflict_note"] = duplicate_flags.apply(
            lambda row: (
                f"В исходном shoreline-слое для профиля есть {int(row['conflicting_duplicate_group_count'])} конфликтующих duplicate-групп(ы); "
                f"даты ключей: {row['duplicate_conflict_obs_dates']}. Эти строки не удалялись молча и должны учитываться как источник осторожности."
            ),
            axis=1,
        )

    subset = merge_with_checks(
        subset,
        duplicate_flags,
        on=["site_id", "profile_id"],
        how="left",
        validate="many_to_one",
        relationship_name="analysis_ready -> shoreline duplicate flags",
        require_all_left=False,
    )
    subset["has_conflicting_shoreline_duplicates"] = subset["has_conflicting_shoreline_duplicates"].eq(True)
    subset["conflicting_duplicate_group_count"] = subset["conflicting_duplicate_group_count"].fillna(0).astype(int)

    subset["qc_flag_analysis_safe"] = subset.apply(
        lambda row: ";".join(
            [
                part
                for part in [
                    row.get("qc_flag"),
                    row.get("qc_flag_analysis"),
                    "CONFLICTING_SHORELINE_DUPLICATES_IN_PROFILE_CONTEXT" if row["has_conflicting_shoreline_duplicates"] else None,
                ]
                if isinstance(part, str) and part
            ]
        )
        or None,
        axis=1,
    )
    subset["qc_note_analysis_safe"] = subset.apply(
        lambda row: " ".join(
            [
                part
                for part in [
                    row.get("qc_note"),
                    row.get("qc_note_analysis"),
                    row.get("duplicate_conflict_note") if row["has_conflicting_shoreline_duplicates"] else None,
                ]
                if isinstance(part, str) and part
            ]
        )
        or None,
        axis=1,
    )

    subset["date_start"] = subset["date_start"].dt.date.astype(str)
    subset["date_end"] = subset["date_end"].dt.date.astype(str)
    return subset[ANALYSIS_SAFE_COLUMNS].sort_values(["site_name", "profile_id", "date_start"]).reset_index(drop=True)


def build_periods_summary(subset: pd.DataFrame) -> pd.DataFrame:
    """Build site- and profile-level period summaries."""

    summary_specs = [
        (["site_id", "site_name", "history_start_group"], "site"),
        (["site_id", "site_name", "profile_id", "profile_num", "profile_name", "history_start_group"], "profile"),
    ]
    rows: list[pd.DataFrame] = []
    for group_columns, level in summary_specs:
        grouped = (
            subset.groupby(group_columns, dropna=False)
            .agg(
                n_intervals=("interval_id", "size"),
                first_interval_start=("date_start", "min"),
                last_interval_end=("date_end", "max"),
                median_days_between=("days_between", "median"),
                mean_years_between=("years_between", "mean"),
                mean_retreat_m=("retreat_m", "mean"),
                median_retreat_m=("retreat_m", "median"),
                mean_retreat_rate_m_per_year=("retreat_rate_m_per_year", "mean"),
                median_retreat_rate_m_per_year=("retreat_rate_m_per_year", "median"),
                std_retreat_rate_m_per_year=("retreat_rate_m_per_year", "std"),
                mean_retreat_abs_m=("retreat_abs_m", "mean"),
                mean_retreat_rate_abs_m_per_year=("retreat_rate_abs_m_per_year", "mean"),
                n_duplicate_context_intervals=("has_conflicting_shoreline_duplicates", "sum"),
            )
            .reset_index()
        )
        grouped.insert(0, "summary_level", level)
        rows.append(grouped)

    summary = pd.concat(rows, ignore_index=True)
    return summary.sort_values(["summary_level", "site_name", "profile_id"], na_position="last").reset_index(drop=True)


def build_profile_correlation_tables(subset: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build comparable profile-pair rows and correlation summary."""

    pair_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    for (site_id, site_name), site_df in subset.groupby(["site_id", "site_name"], dropna=False):
        profiles = sorted(site_df["profile_id"].dropna().unique())
        if len(profiles) < 2:
            continue

        for idx, profile_id_a in enumerate(profiles):
            for profile_id_b in profiles[idx + 1 :]:
                left = site_df.loc[site_df["profile_id"].eq(profile_id_a)].copy()
                right = site_df.loc[site_df["profile_id"].eq(profile_id_b)].copy()
                merged = left.merge(
                    right,
                    on=["site_id", "site_name", "date_start", "date_end"],
                    how="inner",
                    suffixes=("_a", "_b"),
                    validate="one_to_one",
                )

                if not merged.empty:
                    for _, row in merged.iterrows():
                        pair_rows.append(
                            {
                                "site_id": site_id,
                                "site_name": site_name,
                                "profile_id_a": profile_id_a,
                                "profile_id_b": profile_id_b,
                                "profile_name_a": row["profile_name_a"],
                                "profile_name_b": row["profile_name_b"],
                                "date_start": row["date_start"],
                                "date_end": row["date_end"],
                                "retreat_m_a": row["retreat_m_a"],
                                "retreat_m_b": row["retreat_m_b"],
                                "retreat_rate_m_per_year_a": row["retreat_rate_m_per_year_a"],
                                "retreat_rate_m_per_year_b": row["retreat_rate_m_per_year_b"],
                                "retreat_abs_m_a": row["retreat_abs_m_a"],
                                "retreat_abs_m_b": row["retreat_abs_m_b"],
                            }
                        )

                n_overlap = int(len(merged))
                note_bits: list[str] = []
                if n_overlap == 0:
                    note_bits.append("Нет полностью сопоставимых интервалов наблюдений между профилями.")
                elif n_overlap < 3:
                    note_bits.append("Сопоставимых интервалов мало; коэффициенты корреляции нестабильны.")
                elif n_overlap < 10:
                    note_bits.append("Сопоставимых интервалов меньше 10; интерпретация силы связи должна быть осторожной.")

                def corr_or_nan(column_a: str, column_b: str, method: str) -> float | None:
                    if n_overlap < 2:
                        return None
                    series_a = pd.to_numeric(merged[column_a], errors="coerce")
                    series_b = pd.to_numeric(merged[column_b], errors="coerce")
                    valid_mask = series_a.notna() & series_b.notna()
                    if int(valid_mask.sum()) < 2:
                        return None
                    series_a = series_a.loc[valid_mask]
                    series_b = series_b.loc[valid_mask]
                    if method == "spearman":
                        value = series_a.rank(method="average").corr(series_b.rank(method="average"), method="pearson")
                    else:
                        value = series_a.corr(series_b, method="pearson")
                    return None if pd.isna(value) else float(value)

                pearson_retreat = corr_or_nan("retreat_m_a", "retreat_m_b", "pearson")
                spearman_retreat = corr_or_nan("retreat_m_a", "retreat_m_b", "spearman")
                pearson_rate = corr_or_nan("retreat_rate_m_per_year_a", "retreat_rate_m_per_year_b", "pearson")
                spearman_rate = corr_or_nan("retreat_rate_m_per_year_a", "retreat_rate_m_per_year_b", "spearman")
                overlap_caution_flag = classify_overlap_caution(n_overlap)

                if n_overlap >= 2 and all(value is None for value in [pearson_retreat, spearman_retreat, pearson_rate, spearman_rate]):
                    note_bits.append("Один из рядов почти константен или содержит недостаточно вариации для оценки корреляции.")

                summary_rows.append(
                    {
                        "site_id": site_id,
                        "site_name": site_name,
                        "profile_id_a": profile_id_a,
                        "profile_id_b": profile_id_b,
                        "profile_name_a": merged["profile_name_a"].dropna().iloc[0] if not merged["profile_name_a"].dropna().empty else None,
                        "profile_name_b": merged["profile_name_b"].dropna().iloc[0] if not merged["profile_name_b"].dropna().empty else None,
                        "n_overlap_intervals": n_overlap,
                        "pearson_retreat_m": pearson_retreat,
                        "spearman_retreat_m": spearman_retreat,
                        "pearson_retreat_rate_m_per_year": pearson_rate,
                        "spearman_retreat_rate_m_per_year": spearman_rate,
                        "overlap_caution_flag": overlap_caution_flag,
                        "pearson_rate_strength": interpret_correlation_strength(pearson_rate, n_overlap),
                        "spearman_rate_strength": interpret_correlation_strength(spearman_rate, n_overlap),
                        "is_low_sample": n_overlap < 3,
                        "note": " ".join(note_bits) if note_bits else None,
                    }
                )

    pairs_df = pd.DataFrame(pair_rows)
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df = summary_df.sort_values(["site_name", "profile_id_a", "profile_id_b"]).reset_index(drop=True)
    if not pairs_df.empty:
        pairs_df = pairs_df.sort_values(["site_name", "profile_id_a", "profile_id_b", "date_start"]).reset_index(drop=True)
    return summary_df, pairs_df


def build_profile_correlation_presentation(summary_df: pd.DataFrame, subset: pd.DataFrame) -> pd.DataFrame:
    """Build a compact site-level presentation table for profile agreement."""

    if summary_df.empty:
        return pd.DataFrame(
            columns=[
                "site_id",
                "site_name",
                "n_profiles",
                "overlap_range",
                "mean_pearson_retreat_rate_m_per_year",
                "mean_spearman_retreat_rate_m_per_year",
                "profile_agreement_summary",
                "has_duplicate_conflict_context",
            ]
        )

    profile_counts = (
        subset.groupby(["site_id", "site_name"], dropna=False)["profile_id"]
        .nunique()
        .rename("n_profiles")
        .reset_index()
    )
    duplicate_flags = (
        subset.groupby(["site_id", "site_name"], dropna=False)["has_conflicting_shoreline_duplicates"]
        .any()
        .rename("has_duplicate_conflict_context")
        .reset_index()
    )

    presentation = (
        summary_df.groupby(["site_id", "site_name"], dropna=False)
        .agg(
            min_overlap=("n_overlap_intervals", "min"),
            max_overlap=("n_overlap_intervals", "max"),
            mean_pearson_retreat_rate_m_per_year=("pearson_retreat_rate_m_per_year", "mean"),
            mean_spearman_retreat_rate_m_per_year=("spearman_retreat_rate_m_per_year", "mean"),
        )
        .reset_index()
    )
    presentation["overlap_range"] = presentation.apply(
        lambda row: f"{int(row['min_overlap'])}–{int(row['max_overlap'])}",
        axis=1,
    )
    presentation["profile_agreement_summary"] = presentation.apply(
        lambda row: interpret_correlation_strength(row["mean_pearson_retreat_rate_m_per_year"], int(row["max_overlap"])),
        axis=1,
    )
    presentation = merge_with_checks(
        presentation,
        profile_counts,
        on=["site_id", "site_name"],
        how="left",
        validate="one_to_one",
        relationship_name="correlation presentation -> profile counts",
    )
    presentation = merge_with_checks(
        presentation,
        duplicate_flags,
        on=["site_id", "site_name"],
        how="left",
        validate="one_to_one",
        relationship_name="correlation presentation -> duplicate flags",
    )
    presentation["has_duplicate_conflict_context"] = presentation["has_duplicate_conflict_context"].eq(True)
    return presentation[
        [
            "site_id",
            "site_name",
            "n_profiles",
            "overlap_range",
            "mean_pearson_retreat_rate_m_per_year",
            "mean_spearman_retreat_rate_m_per_year",
            "profile_agreement_summary",
            "has_duplicate_conflict_context",
        ]
    ].sort_values("site_name").reset_index(drop=True)


def plot_retreat_distributions(subset: pd.DataFrame, output_path: Path) -> None:
    """Plot first-stage retreat distributions."""

    set_academic_style()
    plot_df = subset.copy()
    ordered_sites = (
        plot_df.groupby("site_name")["retreat_rate_abs_m_per_year"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    fig, axes = plt.subplots(2, 2, figsize=(17, 11.5))
    axes = axes.ravel()
    wrapped_sites = [wrap_display_label(site, width=12, max_lines=2) for site in ordered_sites]

    axes[0].hist(plot_df["retreat_m"].dropna(), bins=24, color=PALETTE["hist_signed"], edgecolor="white", alpha=0.95)
    axes[0].axvline(0, color=PALETTE["neutral"], linewidth=1.2, linestyle="--")
    axes[0].set_title("Смещение бровки за интервал")
    axes[0].set_xlabel("Смещение бровки, м")
    axes[0].set_ylabel("Частота")
    style_axes(axes[0], grid_axis="y")
    axes[0].xaxis.set_major_locator(MaxNLocator(nbins=7))

    axes[1].hist(plot_df["retreat_rate_m_per_year"].dropna(), bins=24, color=PALETTE["hist_rate"], edgecolor="white", alpha=0.95)
    axes[1].axvline(0, color=PALETTE["neutral"], linewidth=1.2, linestyle="--")
    axes[1].set_title("Скорость смещения бровки за интервал")
    axes[1].set_xlabel("Скорость смещения, м/год")
    axes[1].set_ylabel("Частота")
    style_axes(axes[1], grid_axis="y")
    axes[1].xaxis.set_major_locator(MaxNLocator(nbins=7))

    rate_data = [plot_df.loc[plot_df["site_name"].eq(site), "retreat_rate_m_per_year"].dropna().to_numpy() for site in ordered_sites]
    axes[2].boxplot(rate_data, tick_labels=wrapped_sites, patch_artist=True)
    for box in axes[2].artists:
        box.set(facecolor=PALETTE["box_fill"], edgecolor=PALETTE["primary"], alpha=0.85)
    for patch in axes[2].patches:
        patch.set(facecolor=PALETTE["box_fill"], edgecolor=PALETTE["primary"], alpha=0.9)
    axes[2].axhline(0, color=PALETTE["neutral"], linewidth=1.0, linestyle="--")
    axes[2].set_title("Скорость смещения по участкам")
    axes[2].set_ylabel("Скорость смещения, м/год")
    axes[2].tick_params(axis="x", rotation=0, labelsize=9)
    style_axes(axes[2], grid_axis="y")

    abs_data = [plot_df.loc[plot_df["site_name"].eq(site), "retreat_abs_m"].dropna().to_numpy() for site in ordered_sites]
    axes[3].boxplot(abs_data, tick_labels=wrapped_sites, patch_artist=True)
    for patch in axes[3].patches:
        patch.set(facecolor=PALETTE["abs_fill"], edgecolor=PALETTE["neutral"], alpha=0.95)
    axes[3].set_title("Абсолютная величина смещения по участкам")
    axes[3].set_ylabel("Абсолютная величина смещения, м")
    axes[3].tick_params(axis="x", rotation=0, labelsize=9)
    style_axes(axes[3], grid_axis="y")

    fig.suptitle(
        "Распределения показателей переформирования береговой бровки",
        y=0.995,
    )
    fig.text(
        0.015,
        0.018,
        "Примечание. Знак смещения и скорости показывает направление изменения в нормированной профильной системе координат.\n"
        "Для интерпретации величины изменения без учёта знака используйте абсолютные показатели на нижней правой панели.",
        ha="left",
        va="bottom",
        fontsize=9.5,
        color="#3D4752",
    )
    fig.tight_layout(rect=[0.0, 0.08, 1.0, 0.95])
    export_figure(fig, output_path)
    plt.close(fig)


def plot_site_interval_timelines(subset: pd.DataFrame, output_path: Path) -> None:
    """Plot observation intervals by site/profile."""

    set_academic_style()
    plot_df = subset.copy()
    plot_df["date_start_dt"] = pd.to_datetime(plot_df["date_start"])
    plot_df["date_end_dt"] = pd.to_datetime(plot_df["date_end"])
    profile_order = (
        plot_df[["site_name", "profile_num", "profile_name", "profile_id"]]
        .drop_duplicates()
        .sort_values(
            by=["site_name", "profile_num", "profile_name", "profile_id"],
            key=lambda col: col.map(str) if col.name == "site_name" else col,
            na_position="last",
        )
    )
    profile_order["label"] = profile_order.apply(
        lambda row: format_site_profile_label(row["site_name"], row["profile_name"], row["profile_id"]),
        axis=1,
    )
    labels = profile_order["label"].tolist()
    y_lookup = {label: idx for idx, label in enumerate(labels)}
    plot_df = plot_df.merge(
        profile_order[["site_name", "profile_num", "profile_name", "profile_id", "label"]],
        on=["site_name", "profile_num", "profile_name", "profile_id"],
        how="left",
        validate="many_to_one",
    )

    fig_height = max(9.0, 0.46 * len(labels) + 1.8)
    fig, ax = plt.subplots(figsize=(17.5, fig_height), constrained_layout=True)
    for _, row in plot_df.iterrows():
        ax.hlines(
            y=y_lookup[row["label"]],
            xmin=row["date_start_dt"],
            xmax=row["date_end_dt"],
            color=PALETTE["primary"] if not row["has_conflicting_shoreline_duplicates"] else PALETTE["accent"],
            linewidth=3.0,
            alpha=0.92,
            zorder=3,
        )
        ax.scatter(
            [row["date_start_dt"], row["date_end_dt"]],
            [y_lookup[row["label"]], y_lookup[row["label"]]],
            color=PALETTE["primary"] if not row["has_conflicting_shoreline_duplicates"] else PALETTE["accent"],
            s=14,
            zorder=4,
        )

    ax.set_yticks(list(y_lookup.values()))
    ax.set_yticklabels(labels)
    ax.set_title("Интервалы наблюдений по участкам и профилям")
    ax.set_xlabel("Годы наблюдений")
    ax.set_ylabel("Участок и профиль")
    style_axes(ax, grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.invert_yaxis()

    site_breaks = (
        profile_order.groupby("site_name", dropna=False)
        .agg(y_min=("label", lambda values: min(y_lookup[value] for value in values)), y_max=("label", lambda values: max(y_lookup[value] for value in values)))
        .reset_index()
    )
    for idx, row in site_breaks.iterrows():
        if idx % 2 == 0:
            ax.axhspan(row["y_min"] - 0.45, row["y_max"] + 0.45, color="#F5F7FA", zorder=0)
        ax.axhline(row["y_max"] + 0.5, color=PALETTE["grid"], linewidth=0.8, zorder=1)

    legend_handles = [
        Line2D([0], [0], color=PALETTE["primary"], lw=3, label=DISPLAY_LABELS["context"]["standard"]),
        Line2D([0], [0], color=PALETTE["accent"], lw=3, label=DISPLAY_LABELS["context"]["conflict"]),
    ]
    ax.legend(handles=legend_handles, loc="upper right", frameon=True, facecolor="white", edgecolor=PALETTE["grid"])
    ax.text(
        0.0,
        1.02,
        "Красным показаны интервалы, для которых в профильном контексте сохранён признак конфликтующих дублей исходной береговой линии.",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=9.2,
        color="#46505B",
    )
    export_figure(fig, output_path)
    plt.close(fig)


def _build_heatmap_matrix(site_summary: pd.DataFrame, value_column: str) -> tuple[np.ndarray, list[str]]:
    """Create a square matrix for one correlation metric."""

    profile_ids = sorted(set(site_summary["profile_id_a"]).union(site_summary["profile_id_b"]))
    matrix = np.full((len(profile_ids), len(profile_ids)), np.nan)
    index = {profile_id: idx for idx, profile_id in enumerate(profile_ids)}
    label_lookup: dict[str, str] = {}
    for _, row in site_summary.iterrows():
        label_lookup.setdefault(row["profile_id_a"], normalize_profile_label(row.get("profile_name_a"), row["profile_id_a"]))
        label_lookup.setdefault(row["profile_id_b"], normalize_profile_label(row.get("profile_name_b"), row["profile_id_b"]))
    for profile_id, idx in index.items():
        matrix[idx, idx] = 1.0
    for _, row in site_summary.iterrows():
        i = index[row["profile_id_a"]]
        j = index[row["profile_id_b"]]
        value = row[value_column]
        if pd.notna(value):
            matrix[i, j] = float(value)
            matrix[j, i] = float(value)
    return matrix, [wrap_display_label(label_lookup.get(profile_id, str(profile_id)), width=12, max_lines=2) for profile_id in profile_ids]


def _heatmap_text_color(value: float) -> str:
    """Choose contrasting annotation color for heatmap cells."""

    return "white" if abs(value) >= 0.55 else "#20262D"


def plot_profile_correlation_heatmaps(summary_df: pd.DataFrame, output_path: Path) -> None:
    """Plot Pearson and Spearman heatmaps of within-site profile correlations."""

    if summary_df.empty:
        return

    set_academic_style()
    sites = sorted(summary_df["site_name"].dropna().unique())
    ncols = 2
    nrows = int(np.ceil(len(sites) / ncols))
    fig = plt.figure(figsize=(18.0, max(3.5 * nrows + 1.5, 10.5)))
    outer = fig.add_gridspec(nrows=nrows, ncols=ncols, wspace=0.12, hspace=0.34)
    image = None

    for site_idx, site_name in enumerate(sites):
        site_summary = summary_df.loc[summary_df["site_name"].eq(site_name)].copy()
        site_stats = {
            "min_overlap": int(site_summary["n_overlap_intervals"].min()),
            "max_overlap": int(site_summary["n_overlap_intervals"].max()),
            "has_limited_overlap": site_summary["overlap_caution_flag"].isin(["unstable_small_sample", "limited_overlap"]).any(),
        }
        row_idx = site_idx // ncols
        col_group_idx = site_idx % ncols
        inner = outer[row_idx, col_group_idx].subgridspec(1, 2, wspace=0.12)

        for col_idx, (metric, title) in enumerate(
            [
                ("pearson_retreat_rate_m_per_year", "Пирсон"),
                ("spearman_retreat_rate_m_per_year", "Спирмен"),
            ]
        ):
            ax = fig.add_subplot(inner[0, col_idx])
            matrix, labels = _build_heatmap_matrix(site_summary, metric)
            image = ax.imshow(matrix, vmin=-1, vmax=1, cmap="coolwarm")
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=32, ha="right", rotation_mode="anchor", fontsize=9)
            ax.set_yticks(range(len(labels)))
            ax.set_yticklabels(labels, fontsize=9)
            if col_idx == 0:
                overlap_note = (
                    f"Общие интервалы: {site_stats['min_overlap']}-{site_stats['max_overlap']}, осторожно"
                    if site_stats["has_limited_overlap"]
                    else f"Общие интервалы: {site_stats['min_overlap']}-{site_stats['max_overlap']}"
                )
                ax.set_title(
                    f"{site_name}\n{overlap_note}\n{title}",
                    fontsize=9.7,
                    pad=7,
                )
            else:
                ax.set_title(title, fontsize=11, pad=10)
            ax.set_xticks(np.arange(-0.5, len(labels), 1), minor=True)
            ax.set_yticks(np.arange(-0.5, len(labels), 1), minor=True)
            ax.grid(which="minor", color="white", linewidth=1.2)
            ax.tick_params(which="minor", bottom=False, left=False)
            for i in range(matrix.shape[0]):
                for j in range(matrix.shape[1]):
                    if pd.notna(matrix[i, j]):
                        ax.text(
                            j,
                            i,
                            f"{matrix[i, j]:.2f}",
                            ha="center",
                            va="center",
                            fontsize=8.5,
                            color=_heatmap_text_color(float(matrix[i, j])),
                        )
                    else:
                        ax.text(j, i, "н/д", ha="center", va="center", fontsize=7.5, color="#5E6771")
    fig.suptitle(
        "Внутриучастковые корреляции скоростей смещения между профилями",
        y=0.99,
        fontsize=14.5,
    )
    fig.text(
        0.013,
        0.014,
        "Примечание. Коэффициенты рассчитаны только по полностью сопоставимым интервалам наблюдений. "
        "Если общих интервалов мало, значения следует трактовать как ориентировочные.",
        ha="left",
        va="bottom",
        fontsize=9.5,
        color="#46505B",
    )
    fig.subplots_adjust(left=0.055, right=0.91, top=0.93, bottom=0.055)
    cbar = fig.colorbar(image, ax=fig.axes, shrink=0.82, pad=0.01, fraction=0.025)
    cbar.set_label("Коэффициент корреляции", fontsize=11)
    cbar.ax.tick_params(labelsize=9)
    export_figure(fig, output_path)
    plt.close(fig)


def write_first_stage_outputs(subset: pd.DataFrame) -> dict[str, Path]:
    """Write CSV and figure outputs for tasks 1-2."""

    safe_subset_path = PROCESSED_DIR / "analysis_safe_subset.csv"
    periods_summary_path = REPORTS_DIR / "tables" / "01_periods_summary.csv"
    corr_summary_path = REPORTS_DIR / "tables" / "02_profile_correlation_summary.csv"
    corr_pairs_path = REPORTS_DIR / "tables" / "02_profile_correlation_pairs.csv"
    corr_presentation_path = REPORTS_DIR / "tables" / "02_profile_correlation_presentation.csv"
    distributions_path = REPORTS_DIR / "figures" / "01_retreat_distributions.png"
    timelines_path = REPORTS_DIR / "figures" / "01_site_interval_timelines.png"
    heatmaps_path = REPORTS_DIR / "figures" / "02_profile_correlation_heatmaps.png"

    safe_subset_path.parent.mkdir(parents=True, exist_ok=True)
    periods_summary_path.parent.mkdir(parents=True, exist_ok=True)
    distributions_path.parent.mkdir(parents=True, exist_ok=True)

    subset.to_csv(safe_subset_path, index=False)
    periods_summary = build_periods_summary(subset)
    corr_summary, corr_pairs = build_profile_correlation_tables(subset)
    corr_presentation = build_profile_correlation_presentation(corr_summary, subset)
    periods_summary.to_csv(periods_summary_path, index=False)
    corr_summary.to_csv(corr_summary_path, index=False)
    corr_pairs.to_csv(corr_pairs_path, index=False)
    corr_presentation.to_csv(corr_presentation_path, index=False)

    plot_retreat_distributions(subset, distributions_path)
    plot_site_interval_timelines(subset, timelines_path)
    plot_profile_correlation_heatmaps(corr_summary, heatmaps_path)

    return {
        "analysis_safe_subset": safe_subset_path,
        "periods_summary": periods_summary_path,
        "profile_correlation_summary": corr_summary_path,
        "profile_correlation_pairs": corr_pairs_path,
        "profile_correlation_presentation": corr_presentation_path,
        "retreat_distributions_figure": distributions_path,
        "site_interval_timelines_figure": timelines_path,
        "profile_correlation_heatmaps_figure": heatmaps_path,
    }


def run_first_stage_analysis() -> dict[str, Path]:
    """Run the first-stage analysis workflow and return output paths."""

    logger = setup_logging("first_stage_analysis")
    subset = load_analysis_safe_subset()
    outputs = write_first_stage_outputs(subset)
    logger.info("Built %s with %s rows", relative_to_root(outputs["analysis_safe_subset"]), len(subset))
    for key, path in outputs.items():
        logger.info("%s -> %s", key, relative_to_root(path))
    return outputs


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run_first_stage_analysis()


if __name__ == "__main__":
    main()
