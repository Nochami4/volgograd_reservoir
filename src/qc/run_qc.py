"""Run basic QC checks on generated datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.parsers.common import INTERIM_DIR, MAX_REASONABLE_WIND_YEAR, MIN_REASONABLE_WIND_YEAR, PROCESSED_DIR, REPORTS_DIR, relative_to_root, setup_logging

KEY_COLUMNS = {
    "analysis_ready.csv": ["interval_id"],
    "final_dataset_for_modeling.csv": ["interval_id"],
    "base_points.csv": ["base_point_id"],
    "interval_metrics.csv": ["interval_id"],
    "profiles.csv": ["profile_id"],
    "shoreline_observations.csv": ["obs_id"],
    "sites.csv": ["site_id"],
    "water_levels_raw.csv": ["water_obs_id"],
    "wind_obs_hourly.csv": ["wind_obs_id"],
}


def load_duplicate_report() -> pd.DataFrame:
    """Load the shoreline duplicate report when present."""

    path = INTERIM_DIR / "shoreline_duplicate_report.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


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
                "details": f"Допустимый диапазон: {MIN_REASONABLE_WIND_YEAR}..{MAX_REASONABLE_WIND_YEAR}.",
            }
        )
        mandatory_dt_mask = year_numeric.notna() & pd.to_numeric(df["hour"], errors="coerce").notna() & df["obs_datetime"].isna()
        extra_checks.append(
            {
                "name": "missing_obs_datetime_when_date_and_hour_present",
                "count": int(mandatory_dt_mask.sum()),
                "details": "Строки с распознанными годом и часом должны иметь `obs_datetime`, если только они не были помечены как недопустимые выше по пайплайну.",
            }
        )
        if int(bad_year_mask.sum()) > 0:
            blockers.append("В ветровом слое всё ещё есть годы вне допустимого диапазона.")

    if path.name == "base_points.csv":
        extra_checks.append(
            {
                "name": "base_points_empty",
                "count": int(len(df) == 0),
                "details": "Пустой слой `base_points` означает, что береговая геометрия остаётся привязанной лишь частично.",
            }
        )
        if len(df) == 0:
            blockers.append("`base_points.csv` пуст.")
        unresolved_site_share = float(df["site_id"].isna().mean()) if len(df) else 1.0
        extra_checks.append(
            {
                "name": "base_points_missing_site_id_share",
                "count": None,
                "details": f"Доля строк базисных точек без определённого `site_id`: {unresolved_site_share:.3f}.",
            }
        )
        status_distribution = df["point_status"].fillna("missing").value_counts(dropna=False).to_dict() if "point_status" in df.columns else {}
        extra_checks.append(
            {
                "name": "base_points_point_status_distribution",
                "count": None,
                "details": f"Распределение `point_status`: {status_distribution}.",
            }
        )
        if unresolved_site_share > 0:
            blockers.append("Часть базисных точек всё ещё не имеет определённого `site_id` и требует ручной проверки.")

    if path.name == "water_levels_raw.csv" and not df.empty:
        missing_mean_share = float(df["water_level_mean_annual_m_abs"].isna().mean()) if "water_level_mean_annual_m_abs" in df.columns else 1.0
        missing_max_share = float(df["water_level_max_annual_m_abs"].isna().mean()) if "water_level_max_annual_m_abs" in df.columns else 1.0
        year_only_share = float(df["obs_date"].isna().mean()) if "obs_date" in df.columns else 1.0
        extra_checks.append(
            {
                "name": "water_mean_level_missing_share",
                "count": None,
                "details": f"Доля строк без восстановленного среднего годового уровня воды для нижнего участка: {missing_mean_share:.3f}.",
            }
        )
        extra_checks.append(
            {
                "name": "water_max_level_missing_share",
                "count": None,
                "details": f"Доля строк без восстановленного максимального годового уровня воды для нижнего участка: {missing_max_share:.3f}.",
            }
        )
        extra_checks.append(
            {
                "name": "water_rows_without_full_date_share",
                "count": None,
                "details": f"Доля строк воды без полной `obs_date`: {year_only_share:.3f}.",
            }
        )
        if missing_mean_share > 0 or missing_max_share > 0:
            blockers.append("В восстановленном ряду уровней воды по нижнему участку всё ещё есть пропуски годовых значений.")

    if path.name == "analysis_ready.csv" and not df.empty:
        low_wind_share = float(df["qc_flag_analysis"].fillna("").str.contains("LOW_COVERAGE_WIND", regex=False).mean())
        low_water_share = float(df["qc_flag_analysis"].fillna("").str.contains("LOW_COVERAGE_WATER", regex=False).mean())
        adequate_wind = int((pd.to_numeric(df["coverage_wind"], errors="coerce") >= 0.8).sum())
        any_wind = int((pd.to_numeric(df["coverage_wind"], errors="coerce") > 0).sum())
        extra_checks.append(
            {
                "name": "analysis_low_coverage_wind_share",
                "count": None,
                "details": f"Доля интервалов с флагом `LOW_COVERAGE_WIND`: {low_wind_share:.3f}.",
            }
        )
        extra_checks.append(
            {
                "name": "analysis_low_coverage_water_share",
                "count": None,
                "details": f"Доля интервалов с флагом `LOW_COVERAGE_WATER`: {low_water_share:.3f}.",
            }
        )
        extra_checks.append(
            {
                "name": "analysis_intervals_with_any_wind",
                "count": any_wind,
                "details": "Интервалы, где `coverage_wind > 0`.",
            }
        )
        extra_checks.append(
            {
                "name": "analysis_intervals_with_adequate_wind",
                "count": adequate_wind,
                "details": "Интервалы, где `coverage_wind >= 0.8`.",
            }
        )
        if low_wind_share > 0.5:
            blockers.append("Для многих береговых интервалов покрытие ветром остаётся ниже порога 0.8.")

    if path.name == "final_dataset_for_modeling.csv" and not df.empty:
        all_empty_columns = [column for column in df.columns if df[column].isna().all()]
        extra_checks.append(
            {
                "name": "final_dataset_all_empty_columns",
                "count": len(all_empty_columns),
                "details": f"Полностью пустые колонки: {all_empty_columns if all_empty_columns else 'нет'}.",
            }
        )
        if all_empty_columns:
            blockers.append("В `final_dataset_for_modeling.csv` всё ещё есть полностью пустые колонки.")

    if path.name == "shoreline_observations.csv" and not df.empty:
        duplicate_report = load_duplicate_report()
        if not duplicate_report.empty:
            extra_checks.append(
                {
                    "name": "shoreline_duplicate_groups",
                    "count": int(len(duplicate_report)),
                    "details": "Группы дублирующихся ключей наблюдений описаны в `data/interim/shoreline_duplicate_report.csv`.",
                }
            )
            conflicting = int((~duplicate_report["is_full_duplicate"].fillna(False)).sum())
            if conflicting > 0:
                blockers.append("Часть ключей береговых наблюдений остаётся конфликтующей и требует ручной проверки.")

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

    wind_df = pd.read_csv(PROCESSED_DIR / "wind_obs_hourly.csv") if (PROCESSED_DIR / "wind_obs_hourly.csv").exists() else pd.DataFrame()
    water_df = pd.read_csv(PROCESSED_DIR / "water_levels_raw.csv") if (PROCESSED_DIR / "water_levels_raw.csv").exists() else pd.DataFrame()
    analysis_df = pd.read_csv(PROCESSED_DIR / "analysis_ready.csv") if (PROCESSED_DIR / "analysis_ready.csv").exists() else pd.DataFrame()
    sites_df = pd.read_csv(PROCESSED_DIR / "sites.csv") if (PROCESSED_DIR / "sites.csv").exists() else pd.DataFrame()
    base_df = pd.read_csv(PROCESSED_DIR / "base_points.csv") if (PROCESSED_DIR / "base_points.csv").exists() else pd.DataFrame()
    scope_path = INTERIM_DIR / "site_scope_review.csv"
    scope_df = pd.read_csv(scope_path) if scope_path.exists() else pd.DataFrame(columns=["site_id", "scope_status"])
    duplicate_report = load_duplicate_report()

    lines = [
        "# Сводка QC",
        "",
        "## Критические ограничения перед научной интерпретацией",
        "",
    ]
    if all_blockers:
        for file_name, blocker in all_blockers:
            lines.append(f"- `{file_name}`: {blocker}")
    else:
        lines.append("- Автоматический QC не выявил критических блокирующих проблем.")

    lines.extend(["", "## Почему задачи 3/5/6 пока не считаются аналитически безопасными", ""])
    if analysis_df.empty:
        lines.append("- `analysis_ready.csv` пуст, поэтому безопасность последующих аналитических задач оценить нельзя.")
    else:
        coverage = pd.to_numeric(analysis_df["coverage_wind"], errors="coerce")
        lines.append(f"- Интервалы с `coverage_wind >= 0.8`: {int((coverage >= 0.8).sum())}")
        lines.append(f"- Интервалы с `coverage_wind > 0`: {int((coverage > 0).sum())}")
        if not wind_df.empty and "year" in wind_df.columns:
            year_values = sorted({int(value) for value in pd.to_numeric(wind_df["year"], errors='coerce').dropna().astype(int)})
            lines.append(f"- Годы ветровых наблюдений, которые сейчас есть в `wind_obs_hourly.csv`: {year_values}")
        lines.append("- Семейства задач, опирающиеся на интервальный ветер как на фактор, пока остаются черновыми, пока временной ряд не будет расширен или лучше сопоставлен с интервалами.")

    lines.extend(["", "## Диагностика по наборам данных", ""])
    if not base_df.empty:
        unresolved = int(base_df["site_id"].isna().sum()) if "site_id" in base_df.columns else len(base_df)
        status_distribution = base_df["point_status"].fillna("missing").value_counts(dropna=False).to_dict()
        lines.append(f"- `base_points.csv`: {len(base_df)} строк; неразрешённых `site_id`: {unresolved}; распределение `point_status`: {status_distribution}.")
    else:
        lines.append("- `base_points.csv`: пуст.")

    if not water_df.empty:
        missing_mean_share = float(water_df["water_level_mean_annual_m_abs"].isna().mean()) if "water_level_mean_annual_m_abs" in water_df.columns else 1.0
        missing_max_share = float(water_df["water_level_max_annual_m_abs"].isna().mean()) if "water_level_max_annual_m_abs" in water_df.columns else 1.0
        no_date_share = float(water_df["obs_date"].isna().mean()) if "obs_date" in water_df.columns else 1.0
        available_sites = set(water_df["site_id"].dropna().astype(str))
        missing_sites = sorted(set(sites_df["site_id"].dropna().astype(str)) - available_sites) if not sites_df.empty else []
        lines.append(
            f"- `water_levels_raw.csv`: доля пропусков в восстановленных средних/максимальных годовых уровнях нижнего участка = {missing_mean_share:.3f}/{missing_max_share:.3f}; строк без полной даты = {no_date_share:.3f}."
        )
        lines.append("- Уровни воды теперь явно расшифрованы как средние и максимальные годовые значения по нижнему участку, но всё ещё остаются годовым контекстом без полной даты наблюдения.")
        lines.append(f"- Участки без каких-либо извлечённых строк воды: {missing_sites if missing_sites else 'нет'}.")

    if not duplicate_report.empty:
        lines.append(
            f"- `shoreline_duplicate_report.csv`: {len(duplicate_report)} групп дублирующихся ключей, включая {int((~duplicate_report['is_full_duplicate'].fillna(False)).sum())} конфликтующих групп(ы)."
        )

    if not scope_df.empty and "scope_status" in scope_df.columns:
        needs_review = scope_df["scope_status"].fillna("needs_review").eq("needs_review").sum()
        lines.append(f"- `site_scope_review.csv`: {int(needs_review)} участок(ов) всё ещё помечен(о) как `needs_review`.")

    lines.extend(
        [
            "",
            "| Файл | Строк | Колонок | Дубли строк | Дубли ключей | Ключевые колонки |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for result in results:
        lines.append(
            f"| `{result['file']}` | {result['rows']} | {result['columns']} | {result['duplicate_rows']} | "
            f"{result['duplicate_keys'] if result['duplicate_keys'] is not None else 'н/д'} | "
            f"{', '.join(result['key_columns']) if result['key_columns'] else 'н/д'} |"
        )

    lines.extend(["", "## Дополнительные адресные проверки", ""])
    for result in results:
        if not result["extra_checks"]:
            continue
        lines.append(f"### `{result['file']}`")
        lines.append("")
        for check in result["extra_checks"]:
            rendered_count = check["count"] if check["count"] is not None else "н/д"
            lines.append(f"- `{check['name']}`: {rendered_count}. {check['details']}")
        lines.append("")

    lines.extend(["## Пропуски по колонкам", ""])
    for result in results:
        lines.append(f"### `{result['file']}`")
        lines.append("")
        lines.append("| Колонка | Доля пропусков |")
        lines.append("| --- | ---: |")
        for column, share in sorted(result["missing_share"].items()):
            rendered = f"{share:.3f}" if share is not None else "н/д"
            lines.append(f"| `{column}` | {rendered} |")
        lines.append("")

    if not wind_df.empty:
        year_distribution = (
            pd.to_numeric(wind_df["year"], errors="coerce").dropna().astype(int).value_counts().sort_index().to_dict()
        )
        sheet_distribution = wind_df["source_sheet"].fillna("missing").value_counts().to_dict()
        bad_years = sorted(
            {
                int(value)
                for value in pd.to_numeric(wind_df["year"], errors="coerce").dropna().astype(int)
                if value < MIN_REASONABLE_WIND_YEAR or value > MAX_REASONABLE_WIND_YEAR
            }
        )
        suspicious_sheets = {sheet: count for sheet, count in sheet_distribution.items() if count < 24}
        lines.extend(
            [
                "## Диагностика ветрового слоя",
                "",
                f"- Корректных ветровых наблюдений с `obs_datetime`: {int(wind_df['obs_datetime'].notna().sum())}",
                f"- Строк с флагом `invalid_datetime`: {int(wind_df['qc_flag'].fillna('').str.contains('invalid_datetime', case=False, regex=False).sum())}",
                f"- Распределение по годам: {year_distribution}",
                f"- Распределение по исходным листам: {sheet_distribution}",
                f"- Годы вне диапазона {MIN_REASONABLE_WIND_YEAR}..{MAX_REASONABLE_WIND_YEAR}: {bad_years if bad_years else 'нет'}",
                f"- Листы с подозрительно малым числом строк (<24): {suspicious_sheets if suspicious_sheets else 'нет'}",
                "",
            ]
        )
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
