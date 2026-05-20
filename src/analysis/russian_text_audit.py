"""Audit processed tables for remaining English text in human-readable columns."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from src.parsers.common import PROCESSED_DIR, REPORTS_DIR, relative_to_root

AUDIT_COLUMNS: dict[str, list[str]] = {
    "analysis_ready.csv": ["qc_note_analysis", "scope_note"],
    "analysis_safe_subset.csv": ["qc_note_analysis", "qc_note_analysis_safe", "scope_note", "duplicate_conflict_note"],
    "base_points.csv": ["review_reason"],
    "base_points_current.csv": ["review_reason"],
    "base_points_history.csv": ["review_reason"],
    "final_dataset_for_modeling.csv": ["qc_note_analysis_safe"],
    "final_dataset_enriched_open_sources.csv": ["qc_note_analysis_safe", "wind_fill_validation_note", "water_fill_validation_note"],
    "interval_metrics.csv": ["qc_note"],
    "shoreline_observations.csv": ["qc_note"],
    "water_levels_raw.csv": ["qc_note"],
    "wind_obs_hourly.csv": ["qc_note"],
}

TECHNICAL_COLUMNS: dict[str, dict[str, str]] = {
    "base_points.csv": {
        "point_status": "Нормализованный машинный код статуса базисной точки.",
    },
    "base_points_current.csv": {
        "point_status": "Нормализованный машинный код статуса базисной точки.",
    },
    "base_points_history.csv": {
        "point_status": "Нормализованный машинный код статуса базисной точки.",
    },
    "wind_obs_hourly.csv": {
        "missing_reason": "Машинный код пропуска; человекочитаемое объяснение даётся в `qc_note` и документации.",
    },
    "water_levels_raw.csv": {
        "missing_reason": "Машинный код пропуска для водного ряда.",
    },
    "analysis_ready.csv": {
        "scope_status": "Машинный статус проверки состава проекта.",
        "water_time_resolution": "Машинный код временного разрешения водного источника.",
        "calc_method": "Технический ярлык расчётного метода.",
    },
    "analysis_safe_subset.csv": {
        "scope_status": "Машинный статус проверки состава проекта.",
        "water_time_resolution": "Машинный код временного разрешения водного источника.",
        "calc_method": "Технический ярлык расчётного метода.",
    },
    "final_dataset_enriched_open_sources.csv": {
        "wind_fill_status": "Машинный код статуса ветрового enrichment.",
        "wind_fill_method": "Машинный код метода ветрового enrichment.",
        "wind_fill_confidence": "Машинная шкала доверия для ветрового enrichment.",
        "water_fill_status": "Машинный код статуса водного enrichment.",
        "water_fill_method": "Машинный код метода водного enrichment.",
        "water_fill_confidence": "Машинная шкала доверия для водного enrichment.",
    },
}

TRANSLATED_VALUE_SUMMARY = [
    (
        "QC по интервалам",
        "No wind observations fall inside this shoreline interval in the currently available local meteorological workbook.",
        "В доступной локальной метеорологической таблице нет наблюдений ветра, попадающих внутрь этого берегового интервала.",
    ),
    (
        "QC по воде",
        "Water coverage is 0.14, below the 0.8 threshold.",
        "Покрытие уровнями воды составляет 0.14, что ниже принятого порога 0.8.",
    ),
    (
        "Годовая вода",
        "Water values are available only as annual year-level observations without full dates.",
        "Уровни воды доступны только как годовые значения без полной даты наблюдения.",
    ),
    (
        "Scope note",
        "Listed on the project map and in the source-document scope.",
        "Участок указан на проектной карте и в исходных документах по составу исследования.",
    ),
    (
        "Base points review",
        "date missing or partial in source",
        "Дата в источнике отсутствует или указана неполностью.",
    ),
    (
        "Wind parser note",
        "Explicit date token '11.09.20111' has too many year digits and was rejected.",
        "Явная дата «11.09.20111» содержит слишком много цифр в годе и была отклонена.",
    ),
]

ALLOWED_INLINE_PATTERNS = [
    r"data/[A-Za-z0-9_./-]+",
    r"`[^`]+`",
    r"\bsite_id\b",
    r"\bprofile_id\b",
    r"\binterval_id\b",
    r"\bobs_id\b",
    r"\bwater_obs_id\b",
    r"\bwind_obs_id\b",
    r"\bobs_date\b",
    r"\bdate_start\b",
    r"\bdate_end\b",
    r"\bqc_flag\b",
    r"\bqc_flag_analysis\b",
    r"\bqc_flag_analysis_safe\b",
    r"\bLOW_COVERAGE_[A-Z_]+\b",
    r"\bYEAR_ONLY_WATER_SOURCE\b",
    r"\bCONFLICTING_SHORELINE_DUPLICATES_IN_PROFILE_CONTEXT\b",
    r"\bSOURCE_QC_PRESENT\b",
    r"\bNOAA\b",
    r"\bUTC\b",
    r"\bEurope/Volgograd\b",
    r"\bKAMYSIN\b",
    r"\bkamyshin\b",
    r"\bTrue\b",
    r"\bFalse\b",
    r"\b[0-9]{4}-[0-9]{2}-[0-9]{2}\b",
]


def strip_allowed_latin_tokens(text: str) -> str:
    """Remove allowed technical tokens before checking remaining latin text."""

    cleaned = text
    for pattern in ALLOWED_INLINE_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned)
    return cleaned


def has_remaining_latin(text: str) -> bool:
    """Return True when human-readable text still contains non-allowed latin text."""

    cleaned = strip_allowed_latin_tokens(text)
    return bool(re.search(r"[A-Za-z]{2,}", cleaned))


def sample_values(series: pd.Series, limit: int = 4) -> list[str]:
    """Collect compact sample values from a series."""

    values = []
    for value in series.dropna().astype(str).unique():
        text = value.strip()
        if not text:
            continue
        values.append(text)
        if len(values) >= limit:
            break
    return values


def audit_processed_tables() -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    """Audit configured processed tables and return findings."""

    checked: list[dict[str, object]] = []
    findings: list[dict[str, str]] = []

    for file_name, columns in AUDIT_COLUMNS.items():
        path = PROCESSED_DIR / file_name
        df = pd.read_csv(path, dtype=object) if path.exists() else pd.DataFrame()
        file_payload = {
            "file_name": file_name,
            "exists": path.exists(),
            "columns": [],
        }
        if path.exists():
            for column in columns:
                if column not in df.columns:
                    file_payload["columns"].append(
                        {
                            "column": column,
                            "status": "missing_column",
                            "samples": [],
                        }
                    )
                    continue
                non_null = df[column].dropna().astype(str)
                problematic = [value for value in non_null.unique() if has_remaining_latin(str(value))]
                file_payload["columns"].append(
                    {
                        "column": column,
                        "status": "ok" if not problematic else "latin_found",
                        "samples": sample_values(non_null),
                    }
                )
                for value in problematic[:6]:
                    findings.append(
                        {
                            "file_name": file_name,
                            "column": column,
                            "value": str(value),
                        }
                    )
        checked.append(file_payload)

    return checked, findings


def render_report(checked: list[dict[str, object]], findings: list[dict[str, str]]) -> str:
    """Render the markdown audit report."""

    lines = [
        "# Аудит русскоязычного текстового слоя",
        "",
        "Отчёт проверяет человекочитаемые колонки в `data/processed/` и ищет остаточный английский текст только там, где ожидаются пояснения для человека.",
        "",
        "## Какие файлы проверены",
        "",
    ]
    for item in checked:
        status = "проверен" if item["exists"] else "не найден"
        lines.append(f"- `{item['file_name']}` — {status}")

    lines.extend(["", "## Какие колонки проверялись как человекочитаемые", ""])
    for item in checked:
        if not item["exists"]:
            continue
        lines.append(f"### `{item['file_name']}`")
        lines.append("")
        for column_info in item["columns"]:
            status_label = {
                "ok": "английские фразы не обнаружены",
                "latin_found": "обнаружены остатки латиницы",
                "missing_column": "колонка отсутствует в текущем файле",
            }[column_info["status"]]
            lines.append(f"- `{column_info['column']}` — {status_label}.")
            if column_info["samples"]:
                lines.append(f"  Примеры значений: {', '.join(column_info['samples'])}")
        lines.append("")

    lines.extend(["## Какие технические колонки оставлены без перевода", ""])
    for file_name, column_map in TECHNICAL_COLUMNS.items():
        lines.append(f"### `{file_name}`")
        lines.append("")
        for column, reason in column_map.items():
            lines.append(f"- `{column}` — {reason}")
        lines.append("")

    lines.extend(["## Какие типовые значения были переведены", ""])
    for group_name, before, after in TRANSLATED_VALUE_SUMMARY:
        lines.append(f"- `{group_name}`: `{before}` -> `{after}`")

    lines.extend(["", "## Какие значения не переводились намеренно", ""])
    lines.extend(
        [
            "- Машинные флаги `qc_flag`, `qc_flag_analysis`, `qc_flag_analysis_safe` оставлены на английском, потому что это стабильные коды для кода, тестов и joins.",
            "- `point_status`, `missing_reason`, `scope_status`, `water_time_resolution`, `calc_method`, `wind_fill_status`, `wind_fill_method`, `wind_fill_confidence`, `water_fill_status`, `water_fill_method`, `water_fill_confidence` оставлены кодами, потому что это технический слой.",
            "- Пути вида `data/interim/shoreline_duplicate_report.csv`, имена колонок вроде `site_id` и технические токены `NOAA`, `UTC`, `Europe/Volgograd` не считаются ошибкой аудита.",
        ]
    )

    lines.extend(["", "## Итог проверки", ""])
    if findings:
        lines.append("- Остались значения, содержащие латиницу в человекочитаемых колонках:")
        for finding in findings:
            lines.append(f"- `{finding['file_name']}` / `{finding['column']}`: `{finding['value']}`")
    else:
        lines.append("- Остаточный английский текст в проверяемых человекочитаемых колонках не обнаружен.")

    return "\n".join(lines) + "\n"


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=REPORTS_DIR / "tables" / "russian_text_audit.md")
    args = parser.parse_args()

    checked, findings = audit_processed_tables()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_report(checked, findings), encoding="utf-8")
    print(relative_to_root(args.output))


if __name__ == "__main__":
    main()
