"""Build a human-readable delivery package from processed project tables."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.export.delivery_labels import (
    COLUMN_LABELS,
    COLUMN_METADATA,
    clean_optional_text,
    humanize_base_point_date,
    humanize_base_point_note,
    humanize_base_point_status,
    join_nonempty_parts,
    translate_flag_codes,
    translate_missing_reason,
    translate_qc_note_text,
    translate_value_for_column,
)
from src.parsers.common import DATA_DIR, PROCESSED_DIR, relative_to_root, setup_logging

DELIVERY_DIR = DATA_DIR / "delivery"
README_PATH = DELIVERY_DIR / "README_delivery.md"
DATA_DICTIONARY_PATH = DELIVERY_DIR / "data_dictionary_delivery.csv"


@dataclass(frozen=True)
class DeliveryTableSpec:
    """Metadata for a delivery export table."""

    filename: str
    source_table: str
    description: str
    role: str
    main_dataset: bool = False


TABLE_SPECS = [
    DeliveryTableSpec(
        filename="sites_delivery.csv",
        source_table="data/processed/sites.csv",
        description="Справочник участков с основными береговыми характеристиками.",
        role="Справочник",
    ),
    DeliveryTableSpec(
        filename="profiles_delivery.csv",
        source_table="data/processed/profiles.csv",
        description="Справочник профилей и периода их наблюдений.",
        role="Справочник",
    ),
    DeliveryTableSpec(
        filename="shoreline_observations_delivery.csv",
        source_table="data/processed/shoreline_observations.csv",
        description="Исходные по смыслу наблюдения положения бровки в человеко-понятном виде.",
        role="Наблюдения",
    ),
    DeliveryTableSpec(
        filename="interval_metrics_delivery.csv",
        source_table="data/processed/interval_metrics.csv",
        description="Производные интервалы между соседними наблюдениями профиля.",
        role="Производные интервалы",
    ),
    DeliveryTableSpec(
        filename="base_points_delivery.csv",
        source_table="data/processed/base_points_current.csv",
        description="Текущий рабочий слой координат базисных точек без полной технической истории.",
        role="Справочник",
    ),
    DeliveryTableSpec(
        filename="analysis_ready_delivery.csv",
        source_table="data/processed/analysis_ready.csv",
        description="Широкий аналитический слой до безопасного сужения, с человеко-понятными QC-пояснениями.",
        role="Производный аналитический слой",
    ),
    DeliveryTableSpec(
        filename="analysis_safe_subset_delivery.csv",
        source_table="data/processed/analysis_safe_subset.csv",
        description="Расширенный аналитический слой для текущего этапа с человеко-понятными QC-пояснениями.",
        role="Производный аналитический слой",
    ),
    DeliveryTableSpec(
        filename="final_dataset_for_modeling_delivery.csv",
        source_table="data/processed/final_dataset_for_modeling.csv",
        description="Главный компактный набор для передачи, анализа и моделирования.",
        role="Главный датасет",
        main_dataset=True,
    ),
]


def apply_value_translations(df: pd.DataFrame) -> pd.DataFrame:
    """Translate configured value domains into Russian display values."""

    translated = df.copy()
    for column in translated.columns:
        translated[column] = translated[column].map(lambda value: translate_value_for_column(column, value))
    return translated


def drop_all_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that contain no meaningful values."""

    keep_columns = []
    for column in df.columns:
        series = df[column]
        if series.notna().any():
            if series.dtype == object:
                if series.map(clean_optional_text).astype(bool).any():
                    keep_columns.append(column)
            else:
                keep_columns.append(column)
    return df[keep_columns].copy()


def build_shoreline_record_note(row: pd.Series) -> str | None:
    """Compose a human-readable note for shoreline observations."""

    parts: list[str] = []
    qc_note = translate_qc_note_text(row.get("qc_note"))
    if qc_note:
        parts.append(qc_note)

    missing_reason = translate_missing_reason(row.get("missing_reason"))
    if missing_reason:
        parts.append(missing_reason)

    raw_value_text = clean_optional_text(row.get("raw_value_text"))
    if raw_value_text:
        parts.append(
            f"В одном из полей сохранился текст из источника, который не удалось безопасно разобрать автоматически: {raw_value_text}."
        )
    return join_nonempty_parts(parts)


def build_interval_qc_note(row: pd.Series) -> str | None:
    """Compose a human-readable QC note for interval metrics."""

    parts: list[str] = []
    qc_note = translate_qc_note_text(row.get("qc_note"))
    if qc_note:
        parts.append(qc_note)
    if "SOURCE_QC_PRESENT" in clean_optional_text(row.get("qc_flag")) and not qc_note:
        parts.append("В одном или нескольких исходных наблюдениях, входящих в расчёт интервала, есть замечания по качеству.")
    return join_nonempty_parts(parts)


def prepare_reference_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load compact site and profile reference frames for joins."""

    sites = pd.read_csv(PROCESSED_DIR / "sites.csv", dtype=object)
    profiles = pd.read_csv(PROCESSED_DIR / "profiles.csv", dtype=object)
    return sites, profiles


def build_sites_delivery_frame(sites: pd.DataFrame) -> pd.DataFrame:
    """Build the site reference delivery table."""

    return sites[
        [
            "site_id",
            "site_name",
            "shore_type",
            "shore_orientation_text",
            "shore_orientation_deg",
            "exposure_sectors_text",
            "lithology_text",
            "lithology_class",
            "notes",
        ]
    ].copy()


def build_profiles_delivery_frame(sites: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    """Build the profile reference delivery table."""

    merged = profiles.merge(sites[["site_id", "site_name"]], on="site_id", how="left")
    return merged[
        [
            "site_id",
            "site_name",
            "profile_id",
            "profile_num",
            "profile_name",
            "start_date",
            "end_date",
            "n_observations",
        ]
    ].copy()


def build_shoreline_observations_delivery_frame(sites: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    """Build the observation-level delivery table."""

    shoreline = pd.read_csv(PROCESSED_DIR / "shoreline_observations.csv", dtype=object)
    profile_ref = profiles[["profile_id", "site_id", "profile_num", "profile_name"]].copy()
    merged = shoreline.merge(profile_ref, on=["profile_id", "site_id"], how="left")
    merged = merged.merge(sites[["site_id", "site_name"]], on="site_id", how="left")
    merged["delivery_qc_flags"] = merged["qc_flag"].map(translate_flag_codes)
    merged["delivery_record_note"] = merged.apply(build_shoreline_record_note, axis=1)
    return merged[
        [
            "obs_id",
            "site_id",
            "site_name",
            "profile_id",
            "profile_num",
            "profile_name",
            "obs_date",
            "survey_year",
            "measured_point_name",
            "pn_name",
            "raw_measured_distance_m",
            "gp_to_pn_offset_m",
            "brow_position_pn_m",
            "brow_position_raw_m",
            "delivery_qc_flags",
            "delivery_record_note",
        ]
    ].copy()


def build_interval_metrics_delivery_frame(sites: pd.DataFrame, profiles: pd.DataFrame) -> pd.DataFrame:
    """Build the interval-level delivery table."""

    intervals = pd.read_csv(PROCESSED_DIR / "interval_metrics.csv", dtype=object)
    profile_ref = profiles[["profile_id", "site_id", "profile_num", "profile_name"]].copy()
    merged = intervals.merge(profile_ref, on=["profile_id", "site_id"], how="left")
    merged = merged.merge(sites[["site_id", "site_name"]], on="site_id", how="left")
    merged["delivery_qc_flags"] = merged["qc_flag"].map(translate_flag_codes)
    merged["delivery_qc_note"] = merged.apply(build_interval_qc_note, axis=1)
    return merged[
        [
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
            "brow_pos_start_m",
            "brow_pos_end_m",
            "retreat_m",
            "retreat_rate_m_per_year",
            "retreat_abs_m",
            "retreat_rate_abs_m_per_year",
            "n_raw_points_used",
            "delivery_qc_flags",
            "delivery_qc_note",
        ]
    ].copy()


def build_base_points_delivery_frame(sites: pd.DataFrame) -> pd.DataFrame:
    """Build the delivery table for current base points only."""

    base_points = pd.read_csv(PROCESSED_DIR / "base_points_current.csv", dtype=object)
    merged = base_points.merge(sites[["site_id", "site_name"]], on="site_id", how="left")
    merged["base_point_name_delivery"] = merged["base_point_name_norm"].fillna(merged["base_point_name_raw"])
    merged["delivery_base_point_date"] = merged.apply(humanize_base_point_date, axis=1)
    merged["delivery_base_point_status"] = merged.apply(humanize_base_point_status, axis=1)
    merged["delivery_base_point_note"] = merged.apply(humanize_base_point_note, axis=1)
    return merged[
        [
            "site_id",
            "site_name",
            "base_point_name_delivery",
            "delivery_base_point_date",
            "y_m",
            "x_m",
            "accuracy_m",
            "delivery_base_point_status",
            "delivery_base_point_note",
        ]
    ].copy()


def build_analysis_safe_subset_delivery_frame() -> pd.DataFrame:
    """Build the wider analytical delivery table."""

    subset = pd.read_csv(PROCESSED_DIR / "analysis_safe_subset.csv", dtype=object)
    subset["delivery_qc_flags"] = subset["qc_flag_analysis_safe"].map(translate_flag_codes)
    subset["delivery_qc_note"] = subset["qc_note_analysis_safe"].map(translate_qc_note_text)
    return subset[
        [
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
            "shore_type",
            "shore_orientation_text",
            "shore_orientation_deg",
            "exposure_sectors_text",
            "lithology_text",
            "lithology_class",
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
            "history_start_year",
            "history_start_group",
            "has_conflicting_shoreline_duplicates",
            "conflicting_duplicate_group_count",
            "delivery_qc_flags",
            "delivery_qc_note",
        ]
    ].copy()


def build_analysis_ready_delivery_frame() -> pd.DataFrame:
    """Build the wide pre-safe-subset analytical delivery table."""

    analysis_ready = pd.read_csv(PROCESSED_DIR / "analysis_ready.csv", dtype=object)
    analysis_ready["delivery_qc_flags"] = analysis_ready["qc_flag_analysis"].map(translate_flag_codes)
    analysis_ready["delivery_qc_note"] = analysis_ready["qc_note_analysis"].map(translate_qc_note_text)
    return analysis_ready[
        [
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
            "shore_type",
            "shore_orientation_text",
            "shore_orientation_deg",
            "exposure_sectors_text",
            "lithology_text",
            "lithology_class",
            "start_date",
            "end_date",
            "n_observations",
            "n_wind_obs",
            "mean_wind_speed_ms",
            "max_wind_speed_ms",
            "coverage_wind",
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
            "delivery_qc_flags",
            "delivery_qc_note",
        ]
    ].copy()


def build_final_dataset_delivery_frame() -> pd.DataFrame:
    """Build the compact main delivery dataset."""

    final_df = pd.read_csv(PROCESSED_DIR / "final_dataset_for_modeling.csv", dtype=object)
    final_df["delivery_qc_flags"] = final_df["qc_flag_analysis_safe"].map(translate_flag_codes)
    final_df["delivery_qc_note"] = final_df["qc_note_analysis_safe"].map(translate_qc_note_text)
    return final_df[
        [
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
            "shore_type",
            "shore_orientation_text",
            "shore_orientation_deg",
            "exposure_sectors_text",
            "lithology_text",
            "lithology_class",
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
            "history_start_year",
            "history_start_group",
            "has_conflicting_shoreline_duplicates",
            "conflicting_duplicate_group_count",
            "delivery_qc_flags",
            "delivery_qc_note",
        ]
    ].copy()


def finalize_delivery_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Translate values, drop empty columns, and rename columns for delivery."""

    translated = apply_value_translations(df)
    compact = drop_all_empty_columns(translated)
    active_columns = compact.columns.tolist()
    renamed = compact.rename(columns={column: COLUMN_LABELS.get(column, column) for column in active_columns})
    return renamed, active_columns


def build_dictionary_rows(spec: DeliveryTableSpec, active_columns: list[str]) -> list[dict[str, str]]:
    """Build data-dictionary rows for one delivery table."""

    rows: list[dict[str, str]] = []
    for column in active_columns:
        meta = COLUMN_METADATA.get(column, {})
        rows.append(
            {
                "Файл": spec.filename,
                "Исходная таблица": spec.source_table,
                "Название столбца в delivery": COLUMN_LABELS.get(column, column),
                "Исходное имя столбца": column,
                "Описание": meta.get("description", ""),
                "Единицы": meta.get("unit", ""),
                "Примечание": meta.get("notes", ""),
            }
        )
    return rows


def write_delivery_readme(specs: list[DeliveryTableSpec]) -> None:
    """Write a short human-readable README for the delivery package."""

    main_spec = next(spec for spec in specs if spec.main_dataset)
    lines = [
        "# Delivery Package",
        "",
        "Этот пакет подготовлен для внешнего получателя без доступа к полному проекту, коду и промежуточным слоям.",
        "Все CSV в этой папке построены автоматически из канонических processed-слоёв проекта и не заменяют master-таблицы.",
        "",
        "## Главный файл",
        "",
        f"- Основной файл для передачи и дальнейшего анализа: `{main_spec.filename}`",
        "- Это компактный датасет для анализа и моделирования в человеко-понятном виде.",
        "",
        "## Что входит в пакет",
        "",
    ]
    for spec in specs:
        marker = " (главный файл)" if spec.main_dataset else ""
        lines.append(f"- `{spec.filename}`{marker}: {spec.description}")

    lines.extend(
        [
            "",
            "## Что в пакет намеренно не включено",
            "",
            "- Полная история базисных точек (`base_points_history.csv`) не включается в пакет, потому что для внешнего чтения важнее текущий рабочий слой `base_points_delivery.csv`.",
            "- Технические provenance и ETL/debug-поля остаются в master-слоях проекта и не переносятся в delivery CSV без необходимости.",
            "",
            "## Как читать пакет",
            "",
            "- `sites_delivery.csv` и `profiles_delivery.csv` — справочники.",
            "- `shoreline_observations_delivery.csv` — отдельные наблюдения положения бровки.",
            "- `interval_metrics_delivery.csv` — интервалы между соседними наблюдениями.",
            "- `analysis_ready_delivery.csv` — широкий аналитический слой до безопасного сужения.",
            "- `analysis_safe_subset_delivery.csv` — расширенный аналитический слой текущего этапа.",
            f"- `{main_spec.filename}` — главный компактный слой для анализа и моделирования.",
            "",
            "## Важные ограничения данных",
            "",
            "- Знак смещения и скорости отражает направление в профильной системе координат и не должен автоматически трактоваться как готовая физическая категория.",
            "- Абсолютные метрики (`Абсолютная величина смещения, м`, `Абсолютная скорость смещения, м/год`) безопаснее для описательного сравнения между участками.",
            "- Показатели воды в текущем виде остаются годовым гидрологическим контекстом по нижнему участку водохранилища, а не локальным датированным измерением для каждой точки.",
            "- Показатели ветра в текущем проекте следует читать только как ориентировочные: покрытие по многим интервалам остаётся низким.",
            "- Delivery-пакет сделан для чтения человеком без кода, поэтому технические ETL-поля упрощены, а QC-пояснения переведены на русский.",
            "",
            "## Словарь полей",
            "",
            "- Подробное описание колонок вынесено в `data_dictionary_delivery.csv`.",
        ]
    )

    README_PATH.parent.mkdir(parents=True, exist_ok=True)
    README_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_delivery_exports(output_dir: Path | None = None) -> Path:
    """Build the full delivery export package."""

    output_dir = output_dir or DELIVERY_DIR
    logger = setup_logging("build_delivery_exports")
    output_dir.mkdir(parents=True, exist_ok=True)

    sites, profiles = prepare_reference_frames()

    builders = {
        "sites_delivery.csv": lambda: build_sites_delivery_frame(sites),
        "profiles_delivery.csv": lambda: build_profiles_delivery_frame(sites, profiles),
        "shoreline_observations_delivery.csv": lambda: build_shoreline_observations_delivery_frame(sites, profiles),
        "interval_metrics_delivery.csv": lambda: build_interval_metrics_delivery_frame(sites, profiles),
        "base_points_delivery.csv": lambda: build_base_points_delivery_frame(sites),
        "analysis_ready_delivery.csv": build_analysis_ready_delivery_frame,
        "analysis_safe_subset_delivery.csv": build_analysis_safe_subset_delivery_frame,
        "final_dataset_for_modeling_delivery.csv": build_final_dataset_delivery_frame,
    }

    dictionary_rows: list[dict[str, str]] = []
    for spec in TABLE_SPECS:
        frame = builders[spec.filename]()
        delivery_frame, active_columns = finalize_delivery_frame(frame)
        destination = output_dir / spec.filename
        delivery_frame.to_csv(destination, index=False)
        dictionary_rows.extend(build_dictionary_rows(spec, active_columns))
        logger.info("Built %s with %s rows", relative_to_root(destination), len(delivery_frame))

    dictionary_df = pd.DataFrame(dictionary_rows).fillna("")
    dictionary_df.to_csv(DATA_DICTIONARY_PATH, index=False)
    write_delivery_readme(TABLE_SPECS)
    logger.info("Wrote delivery dictionary to %s", relative_to_root(DATA_DICTIONARY_PATH))
    logger.info("Wrote delivery README to %s", relative_to_root(README_PATH))
    return output_dir


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DELIVERY_DIR)
    args = parser.parse_args()
    build_delivery_exports(output_dir=args.output_dir)


if __name__ == "__main__":
    main()
