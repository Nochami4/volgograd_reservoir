"""Build a separate enriched modeling dataset without altering the master export."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pandas as pd

from src.parsers.common import INTERIM_DIR, PROCESSED_DIR, relative_to_root, setup_logging
from src.qc_messages import (
    water_fill_gap_retained_note,
    water_fill_master_retained_note,
    wind_fill_local_context_only_note,
    wind_fill_local_note,
    wind_fill_noaa_note,
    wind_fill_not_filled_note,
    wind_fill_validation_unusable_note,
)

MASTER_DATASET_PATH = PROCESSED_DIR / "final_dataset_for_modeling.csv"
ANALYSIS_READY_PATH = PROCESSED_DIR / "analysis_ready.csv"
LOCAL_WIND_OBS_PATH = PROCESSED_DIR / "wind_obs_hourly.csv"
ENRICHED_DATASET_PATH = PROCESSED_DIR / "final_dataset_enriched_open_sources.csv"
MANIFEST_PATH = PROCESSED_DIR / "final_dataset_enriched_open_sources_manifest.md"
VALIDATION_JSON_PATH = INTERIM_DIR / "final_dataset_enriched_open_sources_validation.json"

OPEN_SOURCE_CACHE_DIR = INTERIM_DIR / "open_source_cache" / "noaa_global_hourly_kamyshin"
NOAA_METADATA_DIR = OPEN_SOURCE_CACHE_DIR / "metadata"
NOAA_HOURLY_DIR = OPEN_SOURCE_CACHE_DIR / "hourly"

NOAA_STATION_USAF = "343630"
NOAA_STATION_WBAN = "99999"
NOAA_STATION_CODE = f"{NOAA_STATION_USAF}{NOAA_STATION_WBAN}"
NOAA_STATION_NAME = "KAMYSIN"
NOAA_LOCAL_TIMEZONE = "Europe/Volgograd"

WIND_INTERVAL_COLUMNS = [
    "n_wind_obs",
    "mean_wind_speed_ms",
    "max_wind_speed_ms",
    "coverage_wind",
]
WATER_VALUE_COLUMNS = [
    "mean_water_level_mean_annual_m_abs",
    "max_water_level_mean_annual_m_abs",
    "min_water_level_mean_annual_m_abs",
    "range_water_level_mean_annual_m_abs",
    "mean_water_level_max_annual_m_abs",
    "max_water_level_max_annual_m_abs",
    "min_water_level_max_annual_m_abs",
    "range_water_level_max_annual_m_abs",
]


def sha256sum(path: Path) -> str:
    """Return a stable SHA-256 digest for a local file."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def fetch_url_with_cache(url: str, cache_path: Path, *, binary: bool = False, timeout: int = 60) -> str | bytes:
    """Read a URL into the cache when needed and return its cached content."""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists():
        return cache_path.read_bytes() if binary else cache_path.read_text(encoding="utf-8")

    try:
        with urlopen(url, timeout=timeout) as response:
            payload = response.read()
    except URLError as exc:
        raise RuntimeError(f"Could not fetch {url} and no cached copy exists at {cache_path}.") from exc

    cache_path.write_bytes(payload)
    return payload if binary else payload.decode("utf-8", errors="replace")


def parse_noaa_wnd_speed(value: object) -> float | None:
    """Parse the WND speed-rate element from NOAA Global Hourly rows in m/s."""

    text = str(value or "")
    parts = text.split(",")
    if len(parts) < 4:
        return None
    speed_raw = pd.to_numeric(parts[3], errors="coerce")
    if pd.isna(speed_raw) or float(speed_raw) == 9999:
        return None
    return float(speed_raw) / 10.0


def load_noaa_station_metadata() -> dict[str, object]:
    """Fetch and parse NOAA station metadata for the Kamyshin match candidate."""

    history_text = fetch_url_with_cache(
        "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt",
        NOAA_METADATA_DIR / "isd-history.txt",
    )
    inventory_text = fetch_url_with_cache(
        "https://www.ncei.noaa.gov/pub/data/noaa/isd-inventory.txt",
        NOAA_METADATA_DIR / "isd-inventory.txt",
    )

    station_prefix = f"{NOAA_STATION_USAF} {NOAA_STATION_WBAN}"
    station_line = next((line for line in str(history_text).splitlines() if line.startswith(station_prefix)), "")
    inventory_years: list[int] = []
    for line in str(inventory_text).splitlines():
        if not line.startswith(station_prefix):
            continue
        parts = line.split()
        if len(parts) >= 3:
            year = pd.to_numeric(parts[2], errors="coerce")
            if pd.notna(year):
                inventory_years.append(int(year))

    return {
        "station_prefix": station_prefix,
        "station_line": station_line,
        "inventory_years": sorted(set(inventory_years)),
        "station_match_ok": NOAA_STATION_NAME in station_line,
    }


def load_noaa_global_hourly(inventory_years: list[int]) -> pd.DataFrame:
    """Load cached or remote NOAA Global Hourly observations for the matched station."""

    frames: list[pd.DataFrame] = []
    for year in inventory_years:
        url = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{NOAA_STATION_CODE}.csv"
        cache_path = NOAA_HOURLY_DIR / f"{year}.csv"
        fetch_url_with_cache(url, cache_path, binary=True)
        df = pd.read_csv(cache_path, usecols=["DATE", "WND"])
        df["obs_datetime_utc"] = pd.to_datetime(df["DATE"], errors="coerce", utc=True)
        df["wind_speed_ms"] = df["WND"].map(parse_noaa_wnd_speed)
        df = df.dropna(subset=["obs_datetime_utc", "wind_speed_ms"]).copy()
        if df.empty:
            continue
        df["obs_datetime_local"] = df["obs_datetime_utc"].dt.tz_convert(NOAA_LOCAL_TIMEZONE).dt.tz_localize(None)
        df["obs_date"] = df["obs_datetime_local"].dt.floor("D")
        frames.append(df[["obs_datetime_local", "obs_date", "wind_speed_ms"]])

    if not frames:
        return pd.DataFrame(columns=["obs_datetime_local", "obs_date", "wind_speed_ms"])
    return pd.concat(frames, ignore_index=True)


def aggregate_interval_wind(intervals_df: pd.DataFrame, obs_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate wind observations over shoreline intervals with the current pipeline logic."""

    rows: list[dict[str, object]] = []
    for row in intervals_df[["interval_id", "date_start", "date_end"]].itertuples(index=False):
        subset = obs_df[(obs_df["obs_date"] >= row.date_start) & (obs_df["obs_date"] <= row.date_end)]
        n_days = max((row.date_end - row.date_start).days + 1, 1)
        coverage = subset["obs_date"].nunique() / n_days if not subset.empty else 0.0
        rows.append(
            {
                "interval_id": row.interval_id,
                "n_wind_obs": int(len(subset)),
                "mean_wind_speed_ms": subset["wind_speed_ms"].mean() if not subset.empty else None,
                "max_wind_speed_ms": subset["wind_speed_ms"].max() if not subset.empty else None,
                "coverage_wind": coverage,
            }
        )
    return pd.DataFrame(rows)


def evaluate_noaa_compatibility(local_obs_df: pd.DataFrame, noaa_obs_df: pd.DataFrame, station_metadata: dict[str, object]) -> dict[str, object]:
    """Evaluate whether NOAA can be used as a same-station compatible fill source."""

    local = local_obs_df.copy()
    local["obs_datetime"] = pd.to_datetime(local["obs_datetime"], errors="coerce")
    local["obs_date"] = pd.to_datetime(local["obs_date"], errors="coerce")
    local["wind_speed_ms"] = pd.to_numeric(local["wind_speed_ms"], errors="coerce")
    local = local.dropna(subset=["obs_datetime", "obs_date", "wind_speed_ms"]).copy()
    local = local[local["obs_datetime"].dt.year >= 2021].copy()

    noaa = noaa_obs_df.copy()
    noaa["wind_speed_ms"] = pd.to_numeric(noaa["wind_speed_ms"], errors="coerce")
    noaa = noaa.dropna(subset=["obs_datetime_local", "obs_date", "wind_speed_ms"]).copy()
    noaa = noaa[noaa["obs_datetime_local"].dt.year >= 2021].copy()

    exact_overlap = local.merge(
        noaa,
        left_on="obs_datetime",
        right_on="obs_datetime_local",
        how="inner",
        suffixes=("_local", "_noaa"),
    )
    exact_overlap_count = int(len(exact_overlap))
    exact_mae = None
    exact_median_abs_error = None
    exact_corr = None
    if exact_overlap_count:
        exact_overlap["diff"] = exact_overlap["wind_speed_ms_local"] - exact_overlap["wind_speed_ms_noaa"]
        exact_mae = float(exact_overlap["diff"].abs().mean())
        exact_median_abs_error = float(exact_overlap["diff"].abs().median())
        corr_value = exact_overlap["wind_speed_ms_local"].corr(exact_overlap["wind_speed_ms_noaa"])
        exact_corr = None if pd.isna(corr_value) else float(corr_value)

    local_daily = (
        local.groupby(local["obs_date"].dt.floor("D"))["wind_speed_ms"]
        .agg(local_mean="mean", local_max="max")
        .reset_index()
        .rename(columns={"obs_date": "obs_date_day"})
    )
    noaa_daily = (
        noaa.groupby("obs_date")["wind_speed_ms"]
        .agg(noaa_mean="mean", noaa_max="max")
        .reset_index()
        .rename(columns={"obs_date": "obs_date_day"})
    )
    daily_overlap = local_daily.merge(noaa_daily, on="obs_date_day", how="inner")
    daily_overlap_count = int(len(daily_overlap))
    daily_mean_mae = None
    daily_max_mae = None
    if daily_overlap_count:
        daily_mean_mae = float((daily_overlap["local_mean"] - daily_overlap["noaa_mean"]).abs().mean())
        daily_max_mae = float((daily_overlap["local_max"] - daily_overlap["noaa_max"]).abs().mean())

    usable_for_fill = bool(
        station_metadata.get("station_match_ok")
        and exact_overlap_count >= 4
        and exact_median_abs_error is not None
        and exact_median_abs_error <= 1.0
        and exact_corr is not None
        and exact_corr >= 0.8
    )

    return {
        **station_metadata,
        "exact_overlap_count": exact_overlap_count,
        "exact_mae": exact_mae,
        "exact_median_abs_error": exact_median_abs_error,
        "exact_corr": exact_corr,
        "daily_overlap_count": daily_overlap_count,
        "daily_mean_mae": daily_mean_mae,
        "daily_max_mae": daily_max_mae,
        "usable_for_fill": usable_for_fill,
    }


def build_wind_enrichment(
    master_df: pd.DataFrame,
    analysis_ready_df: pd.DataFrame,
    noaa_obs_df: pd.DataFrame,
    validation: dict[str, object],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Attach wind enrichment columns and provenance to the master dataset."""

    interval_frame = master_df[["interval_id", "date_start", "date_end"]].copy()
    local_interval = interval_frame.merge(
        analysis_ready_df[["interval_id", *WIND_INTERVAL_COLUMNS]],
        on="interval_id",
        how="left",
    ).rename(columns={column: f"{column}_local" for column in WIND_INTERVAL_COLUMNS})
    local_interval = local_interval[["interval_id", *[f"{column}_local" for column in WIND_INTERVAL_COLUMNS]]]

    if validation.get("usable_for_fill") and not noaa_obs_df.empty:
        noaa_interval = aggregate_interval_wind(interval_frame, noaa_obs_df).rename(
            columns={column: f"{column}_noaa" for column in WIND_INTERVAL_COLUMNS}
        )
        noaa_interval = noaa_interval[["interval_id", *[f"{column}_noaa" for column in WIND_INTERVAL_COLUMNS]]]
    else:
        noaa_interval = master_df[["interval_id"]].copy()
        for column in WIND_INTERVAL_COLUMNS:
            noaa_interval[f"{column}_noaa"] = pd.NA

    enriched = master_df.merge(local_interval, on="interval_id", how="left")
    enriched = enriched.merge(noaa_interval, on="interval_id", how="left")

    local_obs_count = pd.to_numeric(enriched["n_wind_obs_local"], errors="coerce").fillna(0)
    noaa_obs_count = pd.to_numeric(enriched["n_wind_obs_noaa"], errors="coerce").fillna(0)
    local_speed_mask = enriched["mean_wind_speed_ms_local"].notna()
    local_context_only_mask = local_obs_count.gt(0) & ~local_speed_mask

    noaa_count_fill_mask = local_obs_count.eq(0) & noaa_obs_count.gt(0) & bool(validation.get("usable_for_fill"))
    noaa_mean_fill_mask = (
        enriched["mean_wind_speed_ms_local"].isna()
        & enriched["mean_wind_speed_ms_noaa"].notna()
        & bool(validation.get("usable_for_fill"))
    )
    noaa_max_fill_mask = (
        enriched["max_wind_speed_ms_local"].isna()
        & enriched["max_wind_speed_ms_noaa"].notna()
        & bool(validation.get("usable_for_fill"))
    )
    noaa_fill_mask = noaa_count_fill_mask | noaa_mean_fill_mask | noaa_max_fill_mask

    enriched["n_wind_obs"] = enriched["n_wind_obs_local"]
    if noaa_count_fill_mask.any():
        enriched.loc[noaa_count_fill_mask, "n_wind_obs"] = enriched.loc[noaa_count_fill_mask, "n_wind_obs_noaa"]

    enriched["coverage_wind"] = enriched["coverage_wind_local"]
    if noaa_count_fill_mask.any():
        enriched.loc[noaa_count_fill_mask, "coverage_wind"] = enriched.loc[noaa_count_fill_mask, "coverage_wind_noaa"]

    enriched["mean_wind_speed_ms"] = enriched["mean_wind_speed_ms_local"]
    if noaa_mean_fill_mask.any():
        enriched.loc[noaa_mean_fill_mask, "mean_wind_speed_ms"] = enriched.loc[noaa_mean_fill_mask, "mean_wind_speed_ms_noaa"]

    enriched["max_wind_speed_ms"] = enriched["max_wind_speed_ms_local"]
    if noaa_max_fill_mask.any():
        enriched.loc[noaa_max_fill_mask, "max_wind_speed_ms"] = enriched.loc[noaa_max_fill_mask, "max_wind_speed_ms_noaa"]

    local_note = wind_fill_local_note()
    local_context_note = wind_fill_local_context_only_note()
    noaa_note = wind_fill_noaa_note()
    not_filled_note = wind_fill_not_filled_note()
    validation_unusable_note = wind_fill_validation_unusable_note()

    enriched["wind_fill_status"] = pd.NA
    enriched["wind_fill_source"] = pd.NA
    enriched["wind_fill_source_tier"] = pd.NA
    enriched["wind_fill_method"] = pd.NA
    enriched["wind_fill_confidence"] = pd.NA
    enriched["wind_fill_validation_note"] = pd.NA

    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_status"] = "exact_source_local_speed_summary"
    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_source"] = "local_kamyshin_workbook_interval_aggregate"
    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_source_tier"] = "LEVEL_A"
    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_method"] = "copied_from_existing_analysis_ready_interval_aggregation"
    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_confidence"] = "high"
    enriched.loc[local_speed_mask & ~noaa_fill_mask, "wind_fill_validation_note"] = local_note

    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_status"] = "exact_source_local_context_only"
    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_source"] = "local_kamyshin_workbook_interval_aggregate"
    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_source_tier"] = "LEVEL_A"
    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_method"] = "copied_from_existing_analysis_ready_interval_aggregation"
    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_confidence"] = "medium"
    enriched.loc[local_context_only_mask & ~noaa_fill_mask, "wind_fill_validation_note"] = local_context_note

    enriched.loc[noaa_fill_mask, "wind_fill_status"] = "same_station_compatible_source_fill"
    enriched.loc[noaa_fill_mask, "wind_fill_source"] = "NOAA_Global_Hourly_34363099999"
    enriched.loc[noaa_fill_mask, "wind_fill_source_tier"] = "LEVEL_B"
    enriched.loc[noaa_fill_mask, "wind_fill_method"] = "aggregated_from_noaa_hourly_after_utc_to_europe_volgograd_date_conversion"
    enriched.loc[noaa_fill_mask, "wind_fill_confidence"] = "medium"
    enriched.loc[noaa_fill_mask, "wind_fill_validation_note"] = noaa_note

    not_filled_mask = enriched["wind_fill_status"].isna()
    enriched.loc[not_filled_mask, "wind_fill_status"] = (
        "not_filled_validation_failed" if not validation.get("usable_for_fill") and validation.get("station_line") else "not_filled_no_valid_interval_source"
    )
    enriched.loc[not_filled_mask, "wind_fill_source"] = "none"
    enriched.loc[not_filled_mask, "wind_fill_source_tier"] = "none"
    enriched.loc[not_filled_mask, "wind_fill_method"] = "left_missing"
    enriched.loc[not_filled_mask, "wind_fill_confidence"] = "not_available"
    enriched.loc[not_filled_mask, "wind_fill_validation_note"] = (
        validation_unusable_note if not validation.get("usable_for_fill") and validation.get("station_line") else not_filled_note
    )

    stats = {
        "rows_total": int(len(enriched)),
        "rows_with_local_wind_context": int((local_obs_count.gt(0)).sum()),
        "rows_with_local_wind_speed_summary": int(local_speed_mask.sum()),
        "rows_with_local_context_only": int(local_context_only_mask.sum()),
        "rows_filled_from_noaa": int(noaa_fill_mask.sum()),
        "rows_without_any_wind_obs_after_enrichment": int((pd.to_numeric(enriched["n_wind_obs"], errors="coerce").fillna(0) == 0).sum()),
        "master_missing_mean_wind_by_omission": int(len(master_df)),
        "enriched_nonnull_mean_wind": int(enriched["mean_wind_speed_ms"].notna().sum()),
        "enriched_nonnull_coverage_wind": int(enriched["coverage_wind"].notna().sum()),
        "rows_without_wind_mean_after_enrichment": int(enriched["mean_wind_speed_ms"].isna().sum()),
    }

    drop_columns = [f"{column}_local" for column in WIND_INTERVAL_COLUMNS] + [f"{column}_noaa" for column in WIND_INTERVAL_COLUMNS]
    enriched = enriched.drop(columns=drop_columns)
    return enriched, stats


def apply_water_fill_metadata(enriched_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Attach water fill provenance while preserving the master water logic."""

    enriched = enriched_df.copy()
    water_present_mask = enriched[WATER_VALUE_COLUMNS].notna().any(axis=1)

    enriched["water_fill_status"] = pd.NA
    enriched["water_fill_source"] = pd.NA
    enriched["water_fill_source_tier"] = pd.NA
    enriched["water_fill_method"] = pd.NA
    enriched["water_fill_confidence"] = pd.NA
    enriched["water_fill_validation_note"] = pd.NA

    enriched.loc[water_present_mask, "water_fill_status"] = "existing_master_context_retained"
    enriched.loc[water_present_mask, "water_fill_source"] = "lower_section_workbook_1986_2018"
    enriched.loc[water_present_mask, "water_fill_source_tier"] = "LEVEL_A"
    enriched.loc[water_present_mask, "water_fill_method"] = "retained_existing_master_year_only_lower_section_context"
    enriched.loc[water_present_mask, "water_fill_confidence"] = "high_for_reported_annual_values"
    enriched.loc[water_present_mask, "water_fill_validation_note"] = water_fill_master_retained_note()

    missing_mask = ~water_present_mask
    enriched.loc[missing_mask, "water_fill_status"] = "not_filled_no_valid_official_extension_found"
    enriched.loc[missing_mask, "water_fill_source"] = "none"
    enriched.loc[missing_mask, "water_fill_source_tier"] = "none"
    enriched.loc[missing_mask, "water_fill_method"] = "left_missing"
    enriched.loc[missing_mask, "water_fill_confidence"] = "not_available"
    enriched.loc[missing_mask, "water_fill_validation_note"] = water_fill_gap_retained_note()

    stats = {
        "rows_with_master_water_context": int(water_present_mask.sum()),
        "rows_with_water_gap_retained": int(missing_mask.sum()),
        "water_values_filled_beyond_master": 0,
    }
    return enriched, stats


def render_manifest(
    output_path: Path,
    master_hash_before: str,
    master_hash_after: str,
    master_df: pd.DataFrame,
    validation: dict[str, object],
    wind_stats: dict[str, int],
    water_stats: dict[str, int],
) -> None:
    """Write a short human-readable enrichment manifest."""

    missing_water_count = int(master_df["mean_water_level_mean_annual_m_abs"].isna().sum())
    lines = [
        "# final_dataset_enriched_open_sources",
        "",
        "## Роль набора данных",
        "",
        "- Базовый основной набор сохраняется без изменений: `data/processed/final_dataset_for_modeling.csv`",
        "- Дополнительный обогащённый набор: `data/processed/final_dataset_enriched_open_sources.csv`",
        "- Хеш основного файла до сборки: " + master_hash_before,
        "- Хеш основного файла после сборки: " + master_hash_after,
        "- Изменился ли основной файл во время сборки: " + ("да" if master_hash_before != master_hash_after else "нет"),
        "",
        "## Аудит пропусков в основном наборе",
        "",
        "| Колонка | Число пропусков | Доля пропусков | Кандидат на обогащение | Домен обогащения | Пояснение |",
        "| --- | ---: | ---: | --- | --- | --- |",
    ]
    for column in WATER_VALUE_COLUMNS + ["water_context_scope", "water_time_resolution"]:
        missing_count = int(master_df[column].isna().sum())
        missing_share = missing_count / len(master_df) if len(master_df) else 0.0
        note = (
            "Текущий годовой водный контекст отсутствует, потому что известный ряд по нижнему участку покрывает только 1986-2018 годы."
            if column in WATER_VALUE_COLUMNS
            else "Контекстный флаг можно заполнить только при наличии прозрачного официального расширения водного ряда."
        )
        lines.append(
            f"| `{column}` | {missing_count} | {missing_share:.3f} | да | вода | {note} |"
        )
    lines.extend(
        [
            "",
            "Примечание по ветру: в основном наборе нет ветровых колонок по проектному замыслу, поэтому ветровое обогащение добавляется только в дополнительный слой и не исправляет существующую колонку основного набора.",
            "",
            "## Правила обогащения",
            "",
            "- LEVEL A: точное обогащение из исходного источника; допускается для уже подготовленных локальных ветровых агрегатов по Камышину и для сохранения текущего основного водного контекста без изменений.",
            "- LEVEL B: обогащение из совместимого источника по той же станции; допускается только для той же станции Камышин при точном совпадении метаданных и приемлемой проверке перекрытия; здесь использовано только для заполнения пустых ветровых строк локальной базы.",
            "- LEVEL C: перенос контекста по той же переменной; допускается только для уже существующего в основном наборе годового контекста по нижнему участку; никаких новых переносов сверх текущей семантики основного набора не вводилось.",
            "- LEVEL D: запрещено; без mean/median/KNN/ML-импутации, без интерполяции без предметного обоснования, без подмены станции без валидации и без тихой экстраполяции воды за пределы известного ряда по нижнему участку.",
            "",
            "## Оценка источников",
            "",
            "| Источник | Домен | Совпадение станции или области | Временное разрешение | Решение | Пояснение |",
            "| --- | --- | --- | --- | --- | --- |",
            "| `data/raw/meteo/Камышин скорость и направление ветра.xlsx` через `analysis_ready.csv` | ветер | точная проектная базовая линия | интервальные агрегаты по локальным наблюдениям | использован | Скопировано в первую очередь и далее не перезаписывалось. |",
            f"| NOAA Global Hourly `{NOAA_STATION_CODE}` | ветер | {NOAA_STATION_NAME} / `{validation.get('station_line', '').strip()}` | почасовой/синоптический ряд | {'использован только для заполнения' if validation.get('usable_for_fill') else 'не использован'} | Метаданные станции совпали точно; заполнение ограничено строками, пустыми в локальной базе. |",
            "| NOAA GSOD | ветер | то же семейство станции NOAA, но производный набор | суточная сводка | отклонён | Суточные сводки, полученные из почасовых данных, не смешиваются автоматически с текущей интервальной логикой, ориентированной на почасовой источник. |",
            "| Meteostat | ветер | точное воспроизводимое сопоставление со станцией Камышин в этой сборке не подтверждено | почасовой/суточный портал | отклонён | Автоматически не используется без прозрачного точного сопоставления станции. |",
            "| HydroWeb / AISORI-M / EIP Rosgidromet | вода | воспроизводимое расширение ряда по нижнему участку в этой сборке не подтверждено | неясно / прямой путь доступа не подтверждён | отклонён | Новый официальный ряд воды не принимался, поэтому пропуски основного набора сохранены. |",
            "",
            "## Сводка проверки по ветру",
            "",
            f"- Строка совпадения станции: `{validation.get('station_line', '').strip()}`",
            f"- Обнаруженные годы в реестре inventory: {validation.get('inventory_years', [])}",
            f"- Число точных пересечений по локальному времени: {validation.get('exact_overlap_count')}",
            f"- Медианная абсолютная ошибка на точном пересечении: {validation.get('exact_median_abs_error')}",
            f"- Корреляция на точном пересечении: {validation.get('exact_corr')}",
            f"- Число суточных пересечений: {validation.get('daily_overlap_count')}",
            f"- Средняя абсолютная ошибка суточных средних: {validation.get('daily_mean_mae')}",
            f"- Средняя абсолютная ошибка суточных максимумов: {validation.get('daily_max_mae')}",
            f"- Допустим ли NOAA для заполнения по текущему правилу: {validation.get('usable_for_fill')}",
            "",
            "## Итоги заполнения",
            "",
            f"- Строк в основном наборе, где отсутствует хотя бы одно восстановленное значение воды: {missing_water_count}",
            f"- Строк с локальным ветровым контекстом, скопированным из текущего проектного источника: {wind_stats['rows_with_local_wind_context']}",
            f"- Строк с локальными сводками по скорости ветра из точного источника: {wind_stats['rows_with_local_wind_speed_summary']}",
            f"- Строк только с локальным контекстом (число наблюдений/покрытие без полной сводки скорости): {wind_stats['rows_with_local_context_only']}",
            f"- Строк, дополнительно заполненных из совместимого NOAA-источника по той же станции: {wind_stats['rows_filled_from_noaa']}",
            f"- Строк, где после обогащения всё ещё нет среднего/максимального ветра: {wind_stats['rows_without_wind_mean_after_enrichment']}",
            f"- Строк, где после обогащения всё ещё нет вообще никаких ветровых наблюдений: {wind_stats['rows_without_any_wind_obs_after_enrichment']}",
            f"- Строк с сохранённым основным водным контекстом: {water_stats['rows_with_master_water_context']}",
            f"- Строк, где водные пропуски остались незаполненными: {water_stats['rows_with_water_gap_retained']}",
            "",
            "## Проверенные внешние источники",
            "",
            "- Метаданные станции NOAA ISD: `https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt`",
            "- Inventory NOAA ISD: `https://www.ncei.noaa.gov/pub/data/noaa/isd-inventory.txt`",
            f"- Шаблон доступа NOAA Global Hourly: `https://www.ncei.noaa.gov/data/global-hourly/access/{{year}}/{NOAA_STATION_CODE}.csv`",
            "- Страница продукта NOAA ISD: `https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database`",
            "- Обзор NOAA GSOD: `https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516`",
            "- Портал ЕИП Росгидромета, проверенный на предмет расширения водного ряда: `https://eip.meteo.ru/`",
        ]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_final_dataset_enriched_open_sources(
    output_path: Path | None = None,
    manifest_path: Path | None = None,
) -> tuple[Path, Path]:
    """Build the separate enriched dataset and its manifest."""

    output_path = output_path or ENRICHED_DATASET_PATH
    manifest_path = manifest_path or MANIFEST_PATH
    logger = setup_logging("build_final_dataset_enriched_open_sources")

    master_hash_before = sha256sum(MASTER_DATASET_PATH)

    master_df = pd.read_csv(MASTER_DATASET_PATH, parse_dates=["date_start", "date_end"])
    analysis_ready_df = pd.read_csv(ANALYSIS_READY_PATH)
    local_obs_df = pd.read_csv(LOCAL_WIND_OBS_PATH)

    station_metadata = load_noaa_station_metadata()
    noaa_obs_df = load_noaa_global_hourly(station_metadata.get("inventory_years", []))
    validation = evaluate_noaa_compatibility(local_obs_df, noaa_obs_df, station_metadata)

    enriched_df, wind_stats = build_wind_enrichment(
        master_df=master_df,
        analysis_ready_df=analysis_ready_df,
        noaa_obs_df=noaa_obs_df,
        validation=validation,
    )
    enriched_df, water_stats = apply_water_fill_metadata(enriched_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(output_path, index=False)

    master_hash_after = sha256sum(MASTER_DATASET_PATH)
    render_manifest(
        output_path=manifest_path,
        master_hash_before=master_hash_before,
        master_hash_after=master_hash_after,
        master_df=master_df,
        validation=validation,
        wind_stats=wind_stats,
        water_stats=water_stats,
    )

    validation_payload = {
        "master_hash_before": master_hash_before,
        "master_hash_after": master_hash_after,
        "wind_stats": wind_stats,
        "water_stats": water_stats,
        "validation": validation,
        "output_path": relative_to_root(output_path),
        "manifest_path": relative_to_root(manifest_path),
    }
    VALIDATION_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    VALIDATION_JSON_PATH.write_text(json.dumps(validation_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    logger.info("Built %s with %s rows", relative_to_root(output_path), len(enriched_df))
    logger.info("Wrote enrichment manifest to %s", relative_to_root(manifest_path))
    return output_path, manifest_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ENRICHED_DATASET_PATH)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    args = parser.parse_args()
    build_final_dataset_enriched_open_sources(output_path=args.output, manifest_path=args.manifest)


if __name__ == "__main__":
    main()
