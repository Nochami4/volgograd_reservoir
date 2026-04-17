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
    "n_water_obs",
    "coverage_water",
    "mean_water_level_mean_annual_m_abs",
    "max_water_level_mean_annual_m_abs",
    "min_water_level_mean_annual_m_abs",
    "range_water_level_mean_annual_m_abs",
    "mean_water_level_max_annual_m_abs",
    "max_water_level_max_annual_m_abs",
    "min_water_level_max_annual_m_abs",
    "range_water_level_max_annual_m_abs",
    "water_context_scope",
    "water_time_resolution",
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
FINAL_MODELING_DROP_COLUMNS = [
    "notes",
    "qc_note",
    "mean_level",
    "max_level",
    "min_level",
    "range_level",
    "calc_method",
    "n_raw_points_used",
    "in_project_scope",
    "scope_status",
    "scope_note",
    "start_date",
    "end_date",
    "n_observations",
    "duplicate_conflict_obs_dates",
    "duplicate_conflict_note",
    "qc_flag",
    "qc_flag_analysis",
    "qc_note_analysis",
]

DISPLAY_LABELS = {
    "context": {
        "standard": "Обычный контекст наблюдений",
        "conflict": "Профиль с конфликтующими дублями\nв исходном shoreline-слое",
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
    "metrics": {
        "retreat_m": "Смещение бровки за интервал, м",
        "retreat_rate_m_per_year": "Скорость смещения бровки, м/год",
        "retreat_abs_m": "Абсолютная величина смещения, м",
        "retreat_rate_abs_m_per_year": "Абсолютная интенсивность смещения, м/год",
    },
    "notes": {
        "signed_metric": "Знак смещения и скорости показывает направление изменения в профильной системе координат и не должен автоматически трактоваться как готовая физическая категория без полевой верификации.",
        "absolute_metric": "Беззнаковый показатель интенсивности удобен для межучасткового сравнения, потому что отражает масштаб изменения без смешения с направлением.",
        "timeline_reading": "Линия показывает профиль; точки показывают даты наблюдений; длина линии отражает период от первого до последнего наблюдения.",
        "duplicate_context": "Конфликтующие shoreline duplicates не удаляются молча: такие профили сохраняются в выборке и помечаются как контекст, требующий осторожности.",
        "correlation_reading": "Корреляции рассчитаны только внутри одного участка, только по полностью совпадающим `date_start/date_end`, без импутации и по signed `retreat_rate_m_per_year`; Pearson показывает линейную связь, Spearman показывает монотонную связь, а интерпретация зависит от `n_overlap`.",
        "correlation_negative": "Отрицательная корреляция здесь означает противоположное поведение signed скоростей в принятой профильной конвенции, а не готовую физическую категорию; при малом overlap и/или конфликтном duplicate-context нужна дополнительная осторожность.",
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
OUTPUT_FORMATS = ("png",)
LEGACY_EXPORT_SUFFIXES = (".svg", ".pdf")
ZERO_NEAR_ZERO_RATE_THRESHOLD = 0.1


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
    """Save figure in the configured output formats and clear legacy duplicates."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_path = output_path.with_suffix("")
    for fmt in OUTPUT_FORMATS:
        fig.savefig(base_path.with_suffix(f".{fmt}"), dpi=EXPORT_DPI, bbox_inches="tight", pad_inches=0.18, facecolor="white")
    for suffix in LEGACY_EXPORT_SUFFIXES:
        legacy_path = base_path.with_suffix(suffix)
        if legacy_path.exists():
            legacy_path.unlink()


def save_alias_copy(source_path: Path, alias_path: Path) -> None:
    """Save a filesystem-level alias copy for compatibility output names."""

    if source_path.resolve() == alias_path.resolve():
        return
    alias_path.parent.mkdir(parents=True, exist_ok=True)
    alias_path.write_bytes(source_path.read_bytes())
    for suffix in LEGACY_EXPORT_SUFFIXES:
        alias_variant = alias_path.with_suffix(suffix)
        if alias_variant.exists():
            alias_variant.unlink()


def format_overlap_caption(min_overlap: int, max_overlap: int) -> str:
    """Format overlap range in Russian."""

    if min_overlap == max_overlap:
        return f"Общие интервалы: {min_overlap}"
    return f"Общие интервалы: {min_overlap}-{max_overlap}"


def compute_robust_limits(values: pd.Series, low_q: float = 0.01, high_q: float = 0.99) -> tuple[float, float, int]:
    """Compute readable axis limits without altering data."""

    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return -1.0, 1.0, 0
    low = float(clean.quantile(low_q))
    high = float(clean.quantile(high_q))
    if low == high:
        span = abs(low) * 0.15 or 1.0
        return low - span, high + span, 0
    span = high - low
    padding = max(span * 0.08, 0.5)
    outside = int(((clean < low) | (clean > high)).sum())
    return low - padding, high + padding, outside


def annotate_outlier_note(ax: plt.Axes, n_outliers: int, x: float = 0.02, y: float = 0.95) -> None:
    """Annotate that rare extreme values exist outside the main display range."""

    if n_outliers <= 0:
        return
    ax.text(
        x,
        y,
        f"За пределами основного диапазона остаются\nредкие экстремумы: {n_outliers}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8.8,
        color="#49525C",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": PALETTE["grid"], "alpha": 0.95},
    )


def add_figure_footer(fig: plt.Figure, text: str, y: float = 0.012, fontsize: float = 9.2) -> None:
    """Add a compact footer outside the plotting area."""

    fig.text(0.013, y, text, ha="left", va="bottom", fontsize=fontsize, color="#46505B")


def add_figure_subtitle(fig: plt.Figure, text: str, y: float = 0.935, fontsize: float = 9.4) -> None:
    """Add a compact subtitle below the figure title."""

    fig.text(0.013, y, text, ha="left", va="top", fontsize=fontsize, color="#46505B")


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


def classify_direction_bucket(rate_a: float, rate_b: float, threshold: float = ZERO_NEAR_ZERO_RATE_THRESHOLD) -> str:
    """Classify one overlapping interval pair by signed-direction context."""

    if abs(rate_a) <= threshold or abs(rate_b) <= threshold:
        return "zero_or_near_zero"
    return "same_sign" if np.sign(rate_a) == np.sign(rate_b) else "opposite_sign"


def build_direction_shares(rates_a: pd.Series, rates_b: pd.Series) -> tuple[float | None, float | None, float | None]:
    """Summarize how often overlapping rate pairs move in the same, opposite, or near-zero direction."""

    valid = pd.to_numeric(rates_a, errors="coerce").notna() & pd.to_numeric(rates_b, errors="coerce").notna()
    if int(valid.sum()) == 0:
        return None, None, None
    buckets = [
        classify_direction_bucket(float(rate_a), float(rate_b))
        for rate_a, rate_b in zip(rates_a.loc[valid], rates_b.loc[valid])
    ]
    total = len(buckets)
    same = buckets.count("same_sign") / total
    opposite = buckets.count("opposite_sign") / total
    zero = buckets.count("zero_or_near_zero") / total
    return same, opposite, zero


def build_correlation_short_note(
    *,
    n_overlap: int,
    pearson_rate: float | None,
    spearman_rate: float | None,
    same_sign_share: float | None,
    opposite_sign_share: float | None,
    zero_or_near_zero_share: float | None,
    duplicate_conflict_context_any: bool,
    std_rate_a: float | None,
    std_rate_b: float | None,
) -> str | None:
    """Build a compact diagnostic tag string for one profile pair."""

    tags: list[str] = []
    negative_by_coefficients = (
        pearson_rate is not None
        and spearman_rate is not None
        and not pd.isna(pearson_rate)
        and not pd.isna(spearman_rate)
        and float(pearson_rate) <= -0.30
        and float(spearman_rate) <= -0.30
    )
    negative_by_direction = (
        opposite_sign_share is not None
        and same_sign_share is not None
        and float(opposite_sign_share) >= 0.40
        and float(same_sign_share) < 0.40
    )
    suppress_negative_tag = same_sign_share is not None and float(same_sign_share) >= 0.60
    if not suppress_negative_tag and (negative_by_coefficients or negative_by_direction):
        tags.append("negative_signed_relation")
    if classify_overlap_caution(n_overlap) != "adequate_overlap":
        tags.append("low_overlap_read_with_caution")
    if duplicate_conflict_context_any:
        tags.append("conflict_context_read_with_caution")
    if opposite_sign_share is not None and opposite_sign_share >= 0.6:
        tags.append("mostly_opposite_direction")
    elif same_sign_share is not None and same_sign_share >= 0.6:
        tags.append("mostly_same_direction")
    if zero_or_near_zero_share is not None and zero_or_near_zero_share >= 0.4:
        tags.append("many_zero_or_near_zero_intervals")
    if (
        (std_rate_a is not None and not pd.isna(std_rate_a) and float(std_rate_a) <= ZERO_NEAR_ZERO_RATE_THRESHOLD)
        or (std_rate_b is not None and not pd.isna(std_rate_b) and float(std_rate_b) <= ZERO_NEAR_ZERO_RATE_THRESHOLD)
    ):
        tags.append("low_variation_read_with_caution")
    return "; ".join(dict.fromkeys(tags)) if tags else None


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
                                "has_conflicting_shoreline_duplicates_a": row.get("has_conflicting_shoreline_duplicates_a"),
                                "has_conflicting_shoreline_duplicates_b": row.get("has_conflicting_shoreline_duplicates_b"),
                            }
                        )

                n_overlap = int(len(merged))
                note_bits: list[str] = []
                note_bits.append("Сопоставление выполнено только внутри одного участка и только по полностью совпадающим interval `date_start/date_end` без импутации.")
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
                rates_a = pd.to_numeric(merged["retreat_rate_m_per_year_a"], errors="coerce")
                rates_b = pd.to_numeric(merged["retreat_rate_m_per_year_b"], errors="coerce")
                mean_rate_a = None if merged.empty else float(rates_a.mean()) if pd.notna(rates_a.mean()) else None
                mean_rate_b = None if merged.empty else float(rates_b.mean()) if pd.notna(rates_b.mean()) else None
                std_rate_a = None if merged.empty else float(rates_a.std()) if pd.notna(rates_a.std()) else None
                std_rate_b = None if merged.empty else float(rates_b.std()) if pd.notna(rates_b.std()) else None
                same_sign_share, opposite_sign_share, zero_or_near_zero_share = build_direction_shares(rates_a, rates_b)
                duplicate_conflict_context_any = bool(
                    left["has_conflicting_shoreline_duplicates"].fillna(False).astype(bool).any()
                    or right["has_conflicting_shoreline_duplicates"].fillna(False).astype(bool).any()
                )
                short_note = build_correlation_short_note(
                    n_overlap=n_overlap,
                    pearson_rate=pearson_rate,
                    spearman_rate=spearman_rate,
                    same_sign_share=same_sign_share,
                    opposite_sign_share=opposite_sign_share,
                    zero_or_near_zero_share=zero_or_near_zero_share,
                    duplicate_conflict_context_any=duplicate_conflict_context_any,
                    std_rate_a=std_rate_a,
                    std_rate_b=std_rate_b,
                )

                if n_overlap >= 2 and all(value is None for value in [pearson_retreat, spearman_retreat, pearson_rate, spearman_rate]):
                    note_bits.append("Один из рядов почти константен или содержит недостаточно вариации для оценки корреляции.")
                if any(value is not None and value < 0 for value in [pearson_rate, spearman_rate]):
                    note_bits.append(
                        "Отрицательная signed-корреляция здесь означает противоположное поведение `retreat_rate_m_per_year` в принятой профильной конвенции, а не готовую физическую категорию."
                    )
                if duplicate_conflict_context_any:
                    note_bits.append("Для одной или обеих профильных серий есть конфликтующий duplicate-context; интерпретация требует дополнительной осторожности.")
                if zero_or_near_zero_share is not None and zero_or_near_zero_share >= 0.4:
                    note_bits.append("Заметная доля overlap-интервалов имеет нулевые или близкие к нулю signed-скорости.")

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
                        "mean_rate_a": mean_rate_a,
                        "mean_rate_b": mean_rate_b,
                        "std_rate_a": std_rate_a,
                        "std_rate_b": std_rate_b,
                        "same_sign_share": same_sign_share,
                        "opposite_sign_share": opposite_sign_share,
                        "zero_or_near_zero_share": zero_or_near_zero_share,
                        "duplicate_conflict_context_any": duplicate_conflict_context_any,
                        "short_note": short_note,
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


def build_profile_correlation_diagnostics(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Build an explainability-first diagnostics table for task 2."""

    if summary_df.empty:
        return pd.DataFrame(
            columns=[
                "site_id",
                "site_name",
                "profile_id_a",
                "profile_id_b",
                "n_overlap",
                "pearson_rate",
                "spearman_rate",
                "overlap_caution_flag",
                "mean_rate_a",
                "mean_rate_b",
                "std_rate_a",
                "std_rate_b",
                "same_sign_share",
                "opposite_sign_share",
                "zero_or_near_zero_share",
                "duplicate_conflict_context_any",
                "short_note",
                "correlation_basis",
                "interpretation_note",
            ]
        )

    diagnostics = summary_df.copy()
    diagnostics["correlation_basis"] = (
        "within_site_only; exact_date_start_date_end_match_only; no_imputation; signed_retreat_rate_m_per_year; "
        "pearson_linear; spearman_monotonic; interpret_with_n_overlap"
    )
    diagnostics["interpretation_note"] = (
        "Negative coefficients indicate opposite signed behavior in the adopted profile convention; read with extra caution when overlap is small or duplicate-conflict context is present."
    )
    diagnostics = diagnostics.rename(
        columns={
            "n_overlap_intervals": "n_overlap",
            "pearson_retreat_rate_m_per_year": "pearson_rate",
            "spearman_retreat_rate_m_per_year": "spearman_rate",
        }
    )
    return diagnostics[
        [
            "site_id",
            "site_name",
            "profile_id_a",
            "profile_id_b",
            "n_overlap",
            "pearson_rate",
            "spearman_rate",
            "overlap_caution_flag",
            "mean_rate_a",
            "mean_rate_b",
            "std_rate_a",
            "std_rate_b",
            "same_sign_share",
            "opposite_sign_share",
            "zero_or_near_zero_share",
            "duplicate_conflict_context_any",
            "short_note",
            "correlation_basis",
            "interpretation_note",
        ]
    ].sort_values(["site_name", "profile_id_a", "profile_id_b"]).reset_index(drop=True)


def build_final_dataset_for_modeling(subset: pd.DataFrame, output_path: Path | None = None) -> Path:
    """Write a compact modeling dataset without altering the wider technical layers."""

    output_path = output_path or PROCESSED_DIR / "final_dataset_for_modeling.csv"
    final_df = subset.copy()
    drop_columns = [column for column in FINAL_MODELING_DROP_COLUMNS if column in final_df.columns]
    if drop_columns:
        final_df = final_df.drop(columns=drop_columns)

    empty_columns = [column for column in final_df.columns if final_df[column].isna().all()]
    if empty_columns:
        final_df = final_df.drop(columns=empty_columns)

    duplicate_columns_to_drop: list[str] = []
    remaining_columns = list(final_df.columns)
    for left_idx, left_column in enumerate(remaining_columns):
        if left_column in duplicate_columns_to_drop:
            continue
        for right_column in remaining_columns[left_idx + 1 :]:
            if right_column in duplicate_columns_to_drop:
                continue
            if final_df[left_column].equals(final_df[right_column]):
                duplicate_columns_to_drop.append(right_column)
    if duplicate_columns_to_drop:
        final_df = final_df.drop(columns=duplicate_columns_to_drop)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False)
    return output_path


def plot_retreat_histogram(
    plot_df: pd.DataFrame,
    column: str,
    output_path: Path,
    title: str,
    color: str,
    explanation: str,
) -> None:
    """Plot a single retreat distribution with readable central range."""

    set_academic_style()
    values = pd.to_numeric(plot_df[column], errors="coerce").dropna()
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    ax.hist(values, bins=28, color=color, edgecolor="white", alpha=0.95)
    ax.axvline(0, color=PALETTE["neutral"], linewidth=1.15, linestyle="--")
    x_min, x_max, n_outliers = compute_robust_limits(values)
    ax.set_xlim(x_min, x_max)
    ax.set_title(title)
    ax.set_xlabel(translate_display_label("metrics", column))
    ax.set_ylabel("Частота интервалов")
    style_axes(ax, grid_axis="y")
    ax.xaxis.set_major_locator(MaxNLocator(nbins=7))
    annotate_outlier_note(ax, n_outliers)
    add_figure_footer(fig, explanation, y=0.01, fontsize=9.25)
    fig.tight_layout(rect=[0.0, 0.06, 1.0, 0.98])
    export_figure(fig, output_path)
    plt.close(fig)


def plot_site_intensity_summary(plot_df: pd.DataFrame, output_path: Path) -> None:
    """Plot an intuitive site-level summary of absolute retreat intensity."""

    set_academic_style()
    summary = (
        plot_df.groupby("site_name", dropna=False)
        .agg(
            median_intensity=("retreat_rate_abs_m_per_year", "median"),
            q1_intensity=("retreat_rate_abs_m_per_year", lambda series: series.quantile(0.25)),
            q3_intensity=("retreat_rate_abs_m_per_year", lambda series: series.quantile(0.75)),
            n_intervals=("interval_id", "size"),
        )
        .reset_index()
        .sort_values("median_intensity", ascending=False)
        .reset_index(drop=True)
    )
    summary["site_label"] = summary["site_name"].map(lambda value: wrap_display_label(value, width=15, max_lines=2))
    y_positions = np.arange(len(summary))

    fig, ax = plt.subplots(figsize=(12.5, 6.8))
    left_err = summary["median_intensity"] - summary["q1_intensity"]
    right_err = summary["q3_intensity"] - summary["median_intensity"]
    ax.hlines(y_positions, summary["q1_intensity"], summary["q3_intensity"], color=PALETTE["primary_light"], linewidth=5, alpha=0.95)
    ax.scatter(summary["median_intensity"], y_positions, s=68, color=PALETTE["primary"], zorder=3)
    for idx, row in summary.iterrows():
        ax.text(
            row["q3_intensity"] + max(summary["q3_intensity"].max() * 0.015, 0.03),
            idx,
            f"n={int(row['n_intervals'])}",
            va="center",
            ha="left",
            fontsize=8.8,
            color="#5A6470",
        )
    ax.set_yticks(y_positions)
    ax.set_yticklabels(summary["site_label"])
    ax.invert_yaxis()
    ax.set_title("Сравнение участков по интенсивности переформирования")
    ax.set_xlabel("Абсолютная интенсивность смещения бровки, м/год")
    ax.set_ylabel("Участок")
    style_axes(ax, grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    fig.text(
        0.013,
        0.948,
        "Точка показывает медиану по участку, толстая линия показывает межквартильный диапазон.",
        ha="left",
        va="top",
        fontsize=9.4,
        color="#46505B",
    )
    fig.text(
        0.013,
        0.012,
        DISPLAY_LABELS["notes"]["absolute_metric"],
        ha="left",
        va="bottom",
        fontsize=9.4,
        color="#46505B",
    )
    fig.tight_layout(rect=[0.0, 0.05, 1.0, 0.94])
    export_figure(fig, output_path)
    plt.close(fig)


def plot_retreat_distributions(subset: pd.DataFrame, output_path: Path) -> dict[str, Path]:
    """Build separate and composite figures for task 1 retreat distributions."""

    plot_df = subset.copy()
    figures_dir = output_path.parent
    displacement_path = figures_dir / "01_retreat_displacement_hist.png"
    rate_path = figures_dir / "01_retreat_rate_hist.png"
    intensity_summary_path = figures_dir / "01_site_intensity_summary.png"
    composite_path = figures_dir / "01_retreat_distributions_composite.png"

    plot_retreat_histogram(
        plot_df,
        "retreat_m",
        displacement_path,
        "Интервальные смещения береговой бровки",
        PALETTE["hist_signed"],
        DISPLAY_LABELS["notes"]["signed_metric"],
    )
    plot_retreat_histogram(
        plot_df,
        "retreat_rate_m_per_year",
        rate_path,
        "Интервальные скорости смещения береговой бровки",
        PALETTE["hist_rate"],
        DISPLAY_LABELS["notes"]["signed_metric"],
    )
    plot_site_intensity_summary(plot_df, intensity_summary_path)

    set_academic_style()
    fig = plt.figure(figsize=(15.5, 11.5))
    grid = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.15], hspace=0.32, wspace=0.2)
    hist_specs = [
        ("retreat_m", "Интервальные смещения береговой бровки", PALETTE["hist_signed"], fig.add_subplot(grid[0, 0])),
        ("retreat_rate_m_per_year", "Интервальные скорости смещения береговой бровки", PALETTE["hist_rate"], fig.add_subplot(grid[0, 1])),
    ]
    for column, title, color, ax in hist_specs:
        values = pd.to_numeric(plot_df[column], errors="coerce").dropna()
        ax.hist(values, bins=26, color=color, edgecolor="white", alpha=0.95)
        ax.axvline(0, color=PALETTE["neutral"], linewidth=1.1, linestyle="--")
        x_min, x_max, n_outliers = compute_robust_limits(values)
        ax.set_xlim(x_min, x_max)
        ax.set_title(title)
        ax.set_xlabel(translate_display_label("metrics", column))
        ax.set_ylabel("Частота интервалов")
        style_axes(ax, grid_axis="y")
        annotate_outlier_note(ax, n_outliers)
    summary_ax = fig.add_subplot(grid[1, :])
    summary = (
        plot_df.groupby("site_name", dropna=False)
        .agg(
            median_intensity=("retreat_rate_abs_m_per_year", "median"),
            q1_intensity=("retreat_rate_abs_m_per_year", lambda series: series.quantile(0.25)),
            q3_intensity=("retreat_rate_abs_m_per_year", lambda series: series.quantile(0.75)),
        )
        .reset_index()
        .sort_values("median_intensity", ascending=False)
        .reset_index(drop=True)
    )
    y_positions = np.arange(len(summary))
    summary_ax.hlines(y_positions, summary["q1_intensity"], summary["q3_intensity"], color=PALETTE["primary_light"], linewidth=5)
    summary_ax.scatter(summary["median_intensity"], y_positions, s=62, color=PALETTE["primary"], zorder=3)
    summary_ax.set_yticks(y_positions)
    summary_ax.set_yticklabels(summary["site_name"].map(lambda value: wrap_display_label(value, width=16, max_lines=2)))
    summary_ax.invert_yaxis()
    summary_ax.set_title("Межучастковое сравнение по беззнаковой интенсивности")
    summary_ax.set_xlabel("Абсолютная интенсивность смещения бровки, м/год")
    summary_ax.set_ylabel("Участок")
    style_axes(summary_ax, grid_axis="x")
    summary_ax.spines["left"].set_visible(False)
    summary_ax.tick_params(axis="y", length=0)
    fig.suptitle("Задача 1. Периоды переформирования и интенсивность изменений", y=0.985)
    fig.text(
        0.013,
        0.012,
        "Слева и справа сверху показаны знаковые показатели направления изменения. "
        "Нижняя панель показывает беззнаковую интенсивность, более удобную для межучасткового сравнения. "
        + DISPLAY_LABELS["notes"]["signed_metric"],
        ha="left",
        va="bottom",
        fontsize=9.3,
        color="#46505B",
    )
    fig.subplots_adjust(left=0.07, right=0.98, top=0.93, bottom=0.08, hspace=0.35, wspace=0.22)
    export_figure(fig, composite_path)
    plt.close(fig)
    save_alias_copy(composite_path, output_path)
    return {
        "retreat_displacement_hist": displacement_path,
        "retreat_rate_hist": rate_path,
        "site_intensity_summary": intensity_summary_path,
        "retreat_distributions_composite": composite_path,
        "retreat_distributions_legacy": output_path,
    }


def _prepare_timeline_plot_df(subset: pd.DataFrame) -> pd.DataFrame:
    """Prepare timeline plotting frame with readable labels and stable ordering."""

    plot_df = subset.copy()
    plot_df["date_start_dt"] = pd.to_datetime(plot_df["date_start"])
    plot_df["date_end_dt"] = pd.to_datetime(plot_df["date_end"])
    profile_order = (
        plot_df[["site_name", "history_start_group", "profile_num", "profile_name", "profile_id", "has_conflicting_shoreline_duplicates"]]
        .drop_duplicates()
        .sort_values(["history_start_group", "site_name", "profile_num", "profile_name", "profile_id"], na_position="last")
        .reset_index(drop=True)
    )
    profile_order["label"] = profile_order.apply(
        lambda row: format_site_profile_label(row["site_name"], row["profile_name"], row["profile_id"]),
        axis=1,
    )
    return plot_df.merge(
        profile_order,
        on=["site_name", "history_start_group", "profile_num", "profile_name", "profile_id", "has_conflicting_shoreline_duplicates"],
        how="left",
        validate="many_to_one",
    )


def _draw_timeline_panel(ax: plt.Axes, panel_df: pd.DataFrame, panel_title: str) -> None:
    """Draw one timeline panel with calm flagging for duplicate conflicts."""

    if panel_df.empty:
        ax.axis("off")
        return

    profile_rows = panel_df[
        ["site_name", "profile_num", "profile_name", "profile_id", "label", "has_conflicting_shoreline_duplicates"]
    ].drop_duplicates().reset_index(drop=True)
    labels = profile_rows["label"].tolist()
    y_lookup = {label: idx for idx, label in enumerate(labels)}
    global_min = panel_df["date_start_dt"].min()
    marker_x = global_min - pd.Timedelta(days=900)

    site_breaks = (
        profile_rows.groupby("site_name", dropna=False)
        .agg(
            y_min=("label", lambda values: min(y_lookup[value] for value in values)),
            y_max=("label", lambda values: max(y_lookup[value] for value in values)),
        )
        .reset_index()
    )
    for idx, row in site_breaks.iterrows():
        if idx % 2 == 0:
            ax.axhspan(row["y_min"] - 0.45, row["y_max"] + 0.45, color="#F6F8FB", zorder=0)
        ax.axhline(row["y_max"] + 0.5, color=PALETTE["grid"], linewidth=0.8, zorder=1)

    for _, row in panel_df.iterrows():
        y_pos = y_lookup[row["label"]]
        ax.hlines(y=y_pos, xmin=row["date_start_dt"], xmax=row["date_end_dt"], color=PALETTE["primary"], linewidth=2.4, alpha=0.95, zorder=3)
        ax.scatter([row["date_start_dt"], row["date_end_dt"]], [y_pos, y_pos], color=PALETTE["primary"], s=14, zorder=4)

    flagged = profile_rows.loc[profile_rows["has_conflicting_shoreline_duplicates"]].copy()
    if not flagged.empty:
        ax.scatter(
            [marker_x] * len(flagged),
            [y_lookup[label] for label in flagged["label"]],
            color=PALETTE["accent"],
            s=28,
            marker="s",
            zorder=5,
        )

    ax.set_yticks(list(y_lookup.values()))
    ax.set_yticklabels(labels)
    ax.set_title(panel_title, loc="left", fontsize=12)
    ax.set_xlabel("Годы наблюдений")
    ax.set_ylabel("Участок и профиль")
    style_axes(ax, grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.invert_yaxis()
    ax.set_xlim(marker_x - pd.Timedelta(days=250), panel_df["date_end_dt"].max() + pd.Timedelta(days=450))


def plot_site_interval_timelines(subset: pd.DataFrame, output_path: Path) -> dict[str, Path]:
    """Build full and presentation timeline figures for task 1."""

    set_academic_style()
    plot_df = _prepare_timeline_plot_df(subset)
    figures_dir = output_path.parent
    full_path = figures_dir / "01_site_interval_timelines_full.png"
    presentation_path = figures_dir / "01_site_interval_timelines_presentation.png"

    labels = plot_df["label"].drop_duplicates().tolist()
    fig_height = max(10.5, 0.5 * len(labels) + 2.6)
    fig, ax = plt.subplots(figsize=(18.0, fig_height))
    _draw_timeline_panel(ax, plot_df, "Полная временная структура наблюдений")
    legend_handles = [
        Line2D([0], [0], color=PALETTE["primary"], lw=2.8, marker="o", markersize=4, label="Линия и точки: наблюдения одного профиля"),
        Line2D([0], [0], color=PALETTE["accent"], marker="s", linestyle="", markersize=6, label=DISPLAY_LABELS["context"]["conflict"]),
    ]
    ax.legend(handles=legend_handles, loc="upper right", frameon=True, facecolor="white", edgecolor=PALETTE["grid"])
    fig.suptitle("Задача 1. Реальные периоды наблюдений по участкам и профилям", y=0.99)
    fig.text(
        0.013,
        0.014,
        DISPLAY_LABELS["notes"]["timeline_reading"] + " " + DISPLAY_LABELS["notes"]["duplicate_context"],
        ha="left",
        va="bottom",
        fontsize=9.35,
        color="#46505B",
    )
    fig.tight_layout(rect=[0.0, 0.04, 1.0, 0.97])
    export_figure(fig, full_path)
    plt.close(fig)

    groups = [
        ("Ранний блок истории наблюдений", plot_df.loc[plot_df["history_start_group"].str.contains("ранний", na=False)].copy()),
        ("Поздний блок истории наблюдений", plot_df.loc[plot_df["history_start_group"].str.contains("поздний", na=False)].copy()),
    ]
    valid_groups = [(title, df) for title, df in groups if not df.empty]
    fig, axes = plt.subplots(len(valid_groups), 1, figsize=(18.0, max(11.0, 0.42 * len(labels) + 1.8 * len(valid_groups))), sharex=False)
    if len(valid_groups) == 1:
        axes = [axes]
    for ax, (title, panel_df) in zip(axes, valid_groups):
        _draw_timeline_panel(ax, panel_df, title)
    axes[0].legend(handles=legend_handles, loc="upper right", frameon=True, facecolor="white", edgecolor=PALETTE["grid"])
    fig.suptitle("Задача 1. Временная структура наблюдений в удобной версии для показа", y=0.995)
    fig.text(
        0.013,
        0.014,
        DISPLAY_LABELS["notes"]["timeline_reading"] + " " + DISPLAY_LABELS["notes"]["duplicate_context"],
        ha="left",
        va="bottom",
        fontsize=9.35,
        color="#46505B",
    )
    fig.tight_layout(rect=[0.0, 0.04, 1.0, 0.97])
    export_figure(fig, presentation_path)
    plt.close(fig)

    save_alias_copy(presentation_path, output_path)
    return {
        "site_interval_timelines_full": full_path,
        "site_interval_timelines_presentation": presentation_path,
        "site_interval_timelines_legacy": output_path,
    }


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


def _site_correlation_note(site_summary: pd.DataFrame, has_duplicate_context: bool) -> str:
    """Build a short Russian note for one site."""

    min_overlap = int(site_summary["n_overlap_intervals"].min())
    max_overlap = int(site_summary["n_overlap_intervals"].max())
    if site_summary["overlap_caution_flag"].isin(["unstable_small_sample"]).any():
        caution_flag = "unstable_small_sample"
    elif site_summary["overlap_caution_flag"].isin(["limited_overlap"]).any():
        caution_flag = "limited_overlap"
    else:
        caution_flag = "adequate_overlap"
    note = f"{format_overlap_caption(min_overlap, max_overlap)}. {translate_display_label('overlap_caution_flag', caution_flag)}."
    if has_duplicate_context:
        note += " Есть профили с пометкой о конфликтующих дублях."
    return note


def build_correlation_short_label(site_summary: pd.DataFrame, has_duplicate_context: bool) -> str:
    """Build a compact right-side label for overview figure."""

    min_overlap = int(site_summary["n_overlap_intervals"].min())
    max_overlap = int(site_summary["n_overlap_intervals"].max())
    parts = [f"{min_overlap}-{max_overlap}"]
    if site_summary["overlap_caution_flag"].isin(["unstable_small_sample", "limited_overlap"]).any():
        parts.append("осторожно")
    if has_duplicate_context:
        parts.append("дубли")
    return "; ".join(parts)


def _draw_correlation_heatmap_panel(
    ax: plt.Axes,
    site_summary: pd.DataFrame,
    metric: str,
    title: str,
    rotation_override: float | None = None,
) -> plt.AxesImage:
    """Draw one readable heatmap panel."""

    matrix, labels = _build_heatmap_matrix(site_summary, metric)
    image = ax.imshow(matrix, vmin=-1, vmax=1, cmap="coolwarm")
    rotation = rotation_override if rotation_override is not None else (0 if len(labels) <= 3 else 18)
    label_alignment = "center" if rotation == 0 else "right"
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=rotation, ha=label_alignment, rotation_mode="anchor", fontsize=9.5)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9.5)
    ax.set_title(title, fontsize=11.5, pad=9)
    ax.set_xticks(np.arange(-0.5, len(labels), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(labels), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=1.2)
    ax.tick_params(which="minor", bottom=False, left=False)
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if pd.notna(matrix[i, j]):
                ax.text(j, i, f"{matrix[i, j]:.2f}", ha="center", va="center", fontsize=8.3, color=_heatmap_text_color(float(matrix[i, j])))
            else:
                ax.text(j, i, "н/д", ha="center", va="center", fontsize=7.5, color="#5E6771")
    return image


def plot_profile_correlation_heatmaps(
    summary_df: pd.DataFrame,
    presentation_df: pd.DataFrame,
    output_path: Path,
) -> dict[str, Path]:
    """Build site-specific, overview and appendix figures for task 2."""

    if summary_df.empty:
        return {}

    set_academic_style()
    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}
    presentation_lookup = presentation_df.set_index("site_id").to_dict(orient="index") if not presentation_df.empty else {}
    site_records = summary_df[["site_id", "site_name"]].drop_duplicates().sort_values("site_name").to_dict(orient="records")

    for record in site_records:
        site_id = str(record["site_id"])
        site_name = str(record["site_name"])
        site_summary = summary_df.loc[summary_df["site_id"].eq(site_id)].copy()
        site_meta = presentation_lookup.get(site_id, {})
        has_duplicate_context = bool(site_meta.get("has_duplicate_conflict_context", False))
        strength_text = translate_display_label("correlation_strength", site_meta.get("profile_agreement_summary"))
        overlap_label = format_overlap_caption(int(site_summary["n_overlap_intervals"].min()), int(site_summary["n_overlap_intervals"].max()))
        caution_bits = []
        if site_summary["overlap_caution_flag"].isin(["unstable_small_sample", "limited_overlap"]).any():
            caution_bits.append("интерпретация требует осторожности")
        else:
            caution_bits.append("общих интервалов достаточно")
        if has_duplicate_context:
            caution_bits.append("в исходных данных есть конфликтующие дубли")
        subtitle = overlap_label + ". " + "; ".join(caution_bits).capitalize() + "."
        fig = plt.figure(figsize=(12.8, 6.6), layout="constrained")
        gs = fig.add_gridspec(nrows=3, ncols=2, height_ratios=[0.18, 1.0, 0.09], width_ratios=[1.0, 1.0])
        title_ax = fig.add_subplot(gs[0, :])
        left_ax = fig.add_subplot(gs[1, 0])
        right_ax = fig.add_subplot(gs[1, 1])
        footer_ax = fig.add_subplot(gs[2, :])
        title_ax.axis("off")
        footer_ax.axis("off")
        _draw_correlation_heatmap_panel(left_ax, site_summary, "pearson_retreat_rate_m_per_year", "Коэффициент Пирсона")
        _draw_correlation_heatmap_panel(right_ax, site_summary, "spearman_retreat_rate_m_per_year", "Коэффициент Спирмена")
        title_ax.text(
            0.5,
            0.98,
            f"Задача 2. Согласованность профилей внутри участка «{site_name}»",
            ha="center",
            va="top",
            fontsize=16,
            color="#20262D",
            transform=title_ax.transAxes,
        )
        title_ax.text(0.0, 0.18, subtitle, ha="left", va="bottom", fontsize=9.35, color="#46505B", transform=title_ax.transAxes)
        footer_ax.text(
            0.0,
            0.3,
            f"Средняя качественная оценка согласованности: {strength_text}. {DISPLAY_LABELS['notes']['correlation_reading']} {DISPLAY_LABELS['notes']['correlation_negative']}",
            ha="left",
            va="center",
            fontsize=9.0,
            color="#46505B",
            transform=footer_ax.transAxes,
        )
        site_path = output_dir / f"02_profile_correlation_{site_id}.png"
        export_figure(fig, site_path)
        plt.close(fig)
        outputs[f"profile_correlation_{site_id}"] = site_path

    overview = presentation_df.copy().sort_values("mean_pearson_retreat_rate_m_per_year", ascending=False).reset_index(drop=True)
    overview["site_label"] = overview["site_name"].map(lambda value: wrap_display_label(value, width=15, max_lines=2))
    fig = plt.figure(figsize=(12.8, 7.0), layout="constrained")
    gs = fig.add_gridspec(nrows=3, ncols=1, height_ratios=[0.16, 1.0, 0.08])
    title_ax = fig.add_subplot(gs[0, 0])
    ax = fig.add_subplot(gs[1, 0])
    footer_ax = fig.add_subplot(gs[2, 0])
    title_ax.axis("off")
    footer_ax.axis("off")
    y_pos = np.arange(len(overview))
    ax.hlines(y_pos, overview["mean_spearman_retreat_rate_m_per_year"], overview["mean_pearson_retreat_rate_m_per_year"], color=PALETTE["primary_light"], linewidth=2.4, alpha=0.95)
    ax.scatter(overview["mean_pearson_retreat_rate_m_per_year"], y_pos, color=PALETTE["primary"], s=64, label="Средний Пирсон", zorder=3)
    ax.scatter(overview["mean_spearman_retreat_rate_m_per_year"], y_pos, color=PALETTE["accent"], s=64, label="Средний Спирмен", zorder=3)
    for idx, row in overview.iterrows():
        site_summary = summary_df.loc[summary_df["site_id"].eq(row["site_id"])]
        caution_text = build_correlation_short_label(site_summary, bool(row["has_duplicate_conflict_context"]))
        ax.text(1.03, idx, caution_text, va="center", ha="left", fontsize=8.45, color="#59626D")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(overview["site_label"])
    ax.invert_yaxis()
    ax.set_xlim(-1.0, 1.22)
    ax.set_xlabel("Средний коэффициент корреляции по скоростям смещения")
    ax.set_ylabel("Участок")
    style_axes(ax, grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.legend(loc="upper left", frameon=True, facecolor="white", edgecolor=PALETTE["grid"])
    title_ax.text(
        0.5,
        0.98,
        "Задача 2. Сводное сравнение согласованности профилей по участкам",
        ha="center",
        va="top",
        fontsize=16,
        color="#20262D",
        transform=title_ax.transAxes,
    )
    title_ax.text(
        0.0,
        0.18,
        "Справа указано число общих интервалов; коэффициенты получены только по точным совпадениям `date_start/date_end` внутри участка без импутации.",
        ha="left",
        va="bottom",
        fontsize=9.15,
        color="#46505B",
        transform=title_ax.transAxes,
    )
    footer_ax.text(
        0.0,
        0.3,
        "Это основной рисунок задачи 2: он показывает, где профили внутри участка ведут себя более согласованно, а где интерпретация должна быть осторожной. "
        + DISPLAY_LABELS["notes"]["correlation_negative"],
        ha="left",
        va="center",
        fontsize=9.05,
        color="#46505B",
        transform=footer_ax.transAxes,
    )
    overview_path = output_dir / "02_profile_correlation_overview.png"
    export_figure(fig, overview_path)
    plt.close(fig)
    outputs["profile_correlation_overview"] = overview_path

    appendix_groups = [site_records[:4], site_records[4:]]
    appendix_paths: list[Path] = []
    for page_idx, page_records in enumerate([group for group in appendix_groups if group], start=1):
        ncols = 2
        nrows = int(np.ceil(len(page_records) / ncols))
        fig = plt.figure(figsize=(15.5, max(4.2 * nrows, 9.8)), layout="constrained")
        outer = fig.add_gridspec(nrows=nrows, ncols=ncols, wspace=0.18, hspace=0.42)
        for idx, record in enumerate(page_records):
            site_id = str(record["site_id"])
            site_name = str(record["site_name"])
            site_summary = summary_df.loc[summary_df["site_id"].eq(site_id)].copy()
            inner = outer[idx // ncols, idx % ncols].subgridspec(1, 2, wspace=0.1)
            left_ax = fig.add_subplot(inner[0, 0])
            right_ax = fig.add_subplot(inner[0, 1])
            _draw_correlation_heatmap_panel(
                left_ax,
                site_summary,
                "pearson_retreat_rate_m_per_year",
                f"{site_name}\nПирсон",
                rotation_override=30,
            )
            _draw_correlation_heatmap_panel(
                right_ax,
                site_summary,
                "spearman_retreat_rate_m_per_year",
                "Спирмен",
                rotation_override=30,
            )
        fig.suptitle(f"Приложение к задаче 2. Корреляции по участкам, стр. {page_idx}", y=0.99)
        add_figure_footer(
            fig,
            "Справочная мозаика для приложения. Для обсуждения используйте overview и отдельные figures по участкам. "
            + DISPLAY_LABELS["notes"]["correlation_negative"],
            y=0.014,
            fontsize=8.95,
        )
        appendix_page_path = output_dir / f"02_profile_correlation_appendix_page_{page_idx}.png"
        export_figure(fig, appendix_page_path)
        plt.close(fig)
        outputs[f"profile_correlation_appendix_page_{page_idx}"] = appendix_page_path
        appendix_paths.append(appendix_page_path)

    appendix_path = output_dir / "02_profile_correlation_appendix.png"
    save_alias_copy(appendix_paths[0], appendix_path)
    outputs["profile_correlation_appendix"] = appendix_path
    save_alias_copy(appendix_paths[0], output_path)
    return outputs


def write_first_stage_outputs(subset: pd.DataFrame) -> dict[str, Path]:
    """Write CSV and figure outputs for tasks 1-2."""

    safe_subset_path = PROCESSED_DIR / "analysis_safe_subset.csv"
    final_dataset_path = PROCESSED_DIR / "final_dataset_for_modeling.csv"
    periods_summary_path = REPORTS_DIR / "tables" / "01_periods_summary.csv"
    corr_summary_path = REPORTS_DIR / "tables" / "02_profile_correlation_summary.csv"
    corr_pairs_path = REPORTS_DIR / "tables" / "02_profile_correlation_pairs.csv"
    corr_diagnostics_path = REPORTS_DIR / "tables" / "02_profile_correlation_diagnostics.csv"
    corr_presentation_path = REPORTS_DIR / "tables" / "02_profile_correlation_presentation.csv"
    distributions_path = REPORTS_DIR / "figures" / "01_retreat_distributions.png"
    timelines_path = REPORTS_DIR / "figures" / "01_site_interval_timelines.png"
    heatmaps_path = REPORTS_DIR / "figures" / "02_profile_correlation_heatmaps.png"

    safe_subset_path.parent.mkdir(parents=True, exist_ok=True)
    periods_summary_path.parent.mkdir(parents=True, exist_ok=True)
    distributions_path.parent.mkdir(parents=True, exist_ok=True)

    subset.to_csv(safe_subset_path, index=False)
    build_final_dataset_for_modeling(subset, output_path=final_dataset_path)
    periods_summary = build_periods_summary(subset)
    corr_summary, corr_pairs = build_profile_correlation_tables(subset)
    corr_diagnostics = build_profile_correlation_diagnostics(corr_summary)
    corr_presentation = build_profile_correlation_presentation(corr_summary, subset)
    periods_summary.to_csv(periods_summary_path, index=False)
    corr_summary.to_csv(corr_summary_path, index=False)
    corr_pairs.to_csv(corr_pairs_path, index=False)
    corr_diagnostics.to_csv(corr_diagnostics_path, index=False)
    corr_presentation.to_csv(corr_presentation_path, index=False)

    figure_outputs: dict[str, Path] = {}
    figure_outputs.update(plot_retreat_distributions(subset, distributions_path))
    figure_outputs.update(plot_site_interval_timelines(subset, timelines_path))
    figure_outputs.update(plot_profile_correlation_heatmaps(corr_summary, corr_presentation, heatmaps_path))

    outputs = {
        "analysis_safe_subset": safe_subset_path,
        "final_dataset_for_modeling": final_dataset_path,
        "periods_summary": periods_summary_path,
        "profile_correlation_summary": corr_summary_path,
        "profile_correlation_pairs": corr_pairs_path,
        "profile_correlation_diagnostics": corr_diagnostics_path,
        "profile_correlation_presentation": corr_presentation_path,
        "retreat_distributions_figure": distributions_path,
        "site_interval_timelines_figure": timelines_path,
        "profile_correlation_heatmaps_figure": heatmaps_path,
    }
    outputs.update(figure_outputs)
    return outputs


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
