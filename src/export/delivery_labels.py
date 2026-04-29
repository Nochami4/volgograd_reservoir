"""Shared labels and text transformations for delivery exports."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

COLUMN_LABELS: dict[str, str] = {
    "site_id": "site_id",
    "site_name": "site_name",
    "shore_type": "shore_type",
    "shore_orientation_text": "shore_orientation_text",
    "shore_orientation_deg": "shore_orientation_deg",
    "exposure_sectors_text": "exposure_sectors_text",
    "lithology_text": "lithology_text",
    "lithology_class": "lithology_class",
    "notes": "notes",
    "profile_id": "profile_id",
    "profile_num": "profile_num",
    "profile_name": "profile_name",
    "start_date": "start_date",
    "end_date": "end_date",
    "n_observations": "n_observations",
    "obs_id": "obs_id",
    "obs_date": "obs_date",
    "survey_year": "survey_year",
    "measured_point_name": "measured_point_name",
    "pn_name": "pn_name",
    "raw_measured_distance_m": "raw_measured_distance_m",
    "gp_to_pn_offset_m": "gp_to_pn_offset_m",
    "brow_position_pn_m": "brow_position_pn_m",
    "brow_position_raw_m": "brow_position_raw_m",
    "interval_id": "interval_id",
    "date_start": "date_start",
    "date_end": "date_end",
    "days_between": "days_between",
    "years_between": "years_between",
    "brow_pos_start_m": "brow_pos_start_m",
    "brow_pos_end_m": "brow_pos_end_m",
    "retreat_m": "retreat_m",
    "retreat_rate_m_per_year": "retreat_rate_m_per_year",
    "retreat_abs_m": "retreat_abs_m",
    "retreat_rate_abs_m_per_year": "retreat_rate_abs_m_per_year",
    "n_raw_points_used": "n_raw_points_used",
    "base_point_name_delivery": "base_point_name",
    "delivery_base_point_date": "base_point_date",
    "y_m": "y_m",
    "x_m": "x_m",
    "accuracy_m": "accuracy_m",
    "delivery_base_point_status": "base_point_status",
    "delivery_base_point_note": "base_point_note",
    "n_water_obs": "n_water_obs",
    "coverage_water": "coverage_water",
    "mean_water_level_mean_annual_m_abs": "mean_water_level_mean_annual_m_abs",
    "max_water_level_mean_annual_m_abs": "max_water_level_mean_annual_m_abs",
    "min_water_level_mean_annual_m_abs": "min_water_level_mean_annual_m_abs",
    "range_water_level_mean_annual_m_abs": "range_water_level_mean_annual_m_abs",
    "mean_water_level_max_annual_m_abs": "mean_water_level_max_annual_m_abs",
    "max_water_level_max_annual_m_abs": "max_water_level_max_annual_m_abs",
    "min_water_level_max_annual_m_abs": "min_water_level_max_annual_m_abs",
    "range_water_level_max_annual_m_abs": "range_water_level_max_annual_m_abs",
    "water_context_scope": "water_context_scope",
    "water_time_resolution": "water_time_resolution",
    "history_start_year": "history_start_year",
    "history_start_group": "history_start_group",
    "has_conflicting_shoreline_duplicates": "has_conflicting_shoreline_duplicates",
    "conflicting_duplicate_group_count": "conflicting_duplicate_group_count",
    "n_wind_obs": "n_wind_obs",
    "mean_wind_speed_ms": "mean_wind_speed_ms",
    "max_wind_speed_ms": "max_wind_speed_ms",
    "coverage_wind": "coverage_wind",
    "delivery_qc_flags": "qc_flags_display",
    "delivery_qc_note": "qc_note_display",
    "delivery_record_note": "record_note",
}

COLUMN_METADATA: dict[str, dict[str, str]] = {
    "site_id": {"description": "Стабильный код участка для связи таблиц между собой.", "unit": "", "notes": ""},
    "site_name": {"description": "Название участка наблюдений.", "unit": "", "notes": ""},
    "shore_type": {"description": "Тип или положение берега по исходному справочнику.", "unit": "", "notes": ""},
    "shore_orientation_text": {"description": "Текстовое описание ориентации берега из исходного файла.", "unit": "", "notes": ""},
    "shore_orientation_deg": {"description": "Ориентация берега в градусах, если она была определена однозначно.", "unit": "градусы", "notes": ""},
    "exposure_sectors_text": {"description": "Перечень секторов экспозиции участка.", "unit": "", "notes": ""},
    "lithology_text": {"description": "Литологическое описание береговых пород.", "unit": "", "notes": ""},
    "lithology_class": {"description": "Укрупнённый класс литологии по устойчивости.", "unit": "", "notes": ""},
    "notes": {"description": "Дополнительное примечание из исходного справочника.", "unit": "", "notes": ""},
    "profile_id": {"description": "Стабильный код профиля для связи таблиц.", "unit": "", "notes": ""},
    "profile_num": {"description": "Числовой номер профиля.", "unit": "", "notes": ""},
    "profile_name": {"description": "Название профиля в исходных материалах.", "unit": "", "notes": ""},
    "start_date": {"description": "Дата первого наблюдения, связанного с профилем.", "unit": "дата", "notes": ""},
    "end_date": {"description": "Дата последнего наблюдения, связанного с профилем.", "unit": "дата", "notes": ""},
    "n_observations": {"description": "Число наблюдений по профилю в исходном ряду.", "unit": "шт.", "notes": ""},
    "obs_id": {"description": "Стабильный код отдельного наблюдения.", "unit": "", "notes": ""},
    "obs_date": {"description": "Дата наблюдения.", "unit": "дата", "notes": ""},
    "survey_year": {"description": "Год наблюдения, выделенный из даты.", "unit": "год", "notes": ""},
    "measured_point_name": {"description": "Обозначение точки или створа в исходной записи.", "unit": "", "notes": ""},
    "pn_name": {"description": "Постоянный пункт, относительно которого приведено положение бровки.", "unit": "", "notes": ""},
    "raw_measured_distance_m": {"description": "Измеренное расстояние до бровки в исходной записи.", "unit": "м", "notes": ""},
    "gp_to_pn_offset_m": {"description": "Смещение от точки измерения до постоянного пункта.", "unit": "м", "notes": ""},
    "brow_position_pn_m": {"description": "Положение бровки относительно постоянного пункта.", "unit": "м", "notes": ""},
    "brow_position_raw_m": {"description": "Положение бровки в исходной системе отсчёта.", "unit": "м", "notes": ""},
    "interval_id": {"description": "Стабильный код интервала между соседними наблюдениями профиля.", "unit": "", "notes": ""},
    "date_start": {"description": "Дата начала интервала.", "unit": "дата", "notes": ""},
    "date_end": {"description": "Дата конца интервала.", "unit": "дата", "notes": ""},
    "days_between": {"description": "Длина интервала в днях.", "unit": "дни", "notes": ""},
    "years_between": {"description": "Длина интервала в годах.", "unit": "годы", "notes": ""},
    "brow_pos_start_m": {"description": "Положение бровки в начале интервала.", "unit": "м", "notes": ""},
    "brow_pos_end_m": {"description": "Положение бровки в конце интервала.", "unit": "м", "notes": ""},
    "retreat_m": {"description": "Смещение бровки за интервал в принятой профильной системе координат.", "unit": "м", "notes": "Знак показывает направление в профильной системе координат и не должен автоматически трактоваться как готовая физическая категория."},
    "retreat_rate_m_per_year": {"description": "Средняя скорость смещения бровки за интервал.", "unit": "м/год", "notes": "Знак показывает направление в профильной системе координат."},
    "retreat_abs_m": {"description": "Абсолютная величина смещения без учёта знака.", "unit": "м", "notes": "Эта метрика безопаснее для описательного сравнения между участками."},
    "retreat_rate_abs_m_per_year": {"description": "Абсолютная скорость смещения без учёта знака.", "unit": "м/год", "notes": "Эта метрика безопаснее для описательного сравнения между участками."},
    "n_raw_points_used": {"description": "Число исходных наблюдений, использованных при расчёте интервала.", "unit": "шт.", "notes": ""},
    "base_point_name_delivery": {"description": "Понятное обозначение базисной точки.", "unit": "", "notes": ""},
    "delivery_base_point_date": {"description": "Дата точки; если точная дата отсутствует, сохраняется исходный текст даты.", "unit": "", "notes": ""},
    "y_m": {"description": "Координата Y базисной точки.", "unit": "м", "notes": ""},
    "x_m": {"description": "Координата X базисной точки.", "unit": "м", "notes": ""},
    "accuracy_m": {"description": "Оценка точности координат, если она указана.", "unit": "м", "notes": ""},
    "delivery_base_point_status": {"description": "Человеко-понятный статус точки.", "unit": "", "notes": ""},
    "delivery_base_point_note": {"description": "Краткое пояснение по ограничениям или особенностям записи.", "unit": "", "notes": ""},
    "n_water_obs": {"description": "Число годовых значений уровня воды, попавших в интервал.", "unit": "шт.", "notes": ""},
    "coverage_water": {"description": "Доля лет интервала, для которых в текущем water-layer есть данные.", "unit": "доля от 0 до 1", "notes": "Это покрытие годового контекста, а не полнота локального датированного ряда."},
    "mean_water_level_mean_annual_m_abs": {"description": "Среднее по годам значение annual mean water level для интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "max_water_level_mean_annual_m_abs": {"description": "Максимум annual mean water level внутри интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "min_water_level_mean_annual_m_abs": {"description": "Минимум annual mean water level внутри интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "range_water_level_mean_annual_m_abs": {"description": "Размах annual mean water level внутри интервала.", "unit": "м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "mean_water_level_max_annual_m_abs": {"description": "Среднее по годам значение annual maximum water level для интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "max_water_level_max_annual_m_abs": {"description": "Максимум annual maximum water level внутри интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "min_water_level_max_annual_m_abs": {"description": "Минимум annual maximum water level внутри интервала.", "unit": "абс. м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "range_water_level_max_annual_m_abs": {"description": "Размах annual maximum water level внутри интервала.", "unit": "м", "notes": "Это годовой гидрологический контекст по нижнему участку водохранилища."},
    "water_context_scope": {"description": "К какому гидрологическому контексту относится показатель воды.", "unit": "", "notes": ""},
    "water_time_resolution": {"description": "Временное разрешение источника воды.", "unit": "", "notes": "Сейчас вода в основном доступна только по годам."},
    "history_start_year": {"description": "Год начала наблюдений по данному профилю.", "unit": "год", "notes": ""},
    "history_start_group": {"description": "Укрупнённая группа по началу истории наблюдений профиля.", "unit": "", "notes": ""},
    "has_conflicting_shoreline_duplicates": {"description": "Есть ли в контексте профиля конфликтующие дубли береговых наблюдений.", "unit": "", "notes": ""},
    "conflicting_duplicate_group_count": {"description": "Сколько конфликтующих групп дублей связано с профилем.", "unit": "шт.", "notes": ""},
    "n_wind_obs": {"description": "Число наблюдений ветра, попавших в интервал.", "unit": "шт.", "notes": ""},
    "mean_wind_speed_ms": {"description": "Средняя скорость ветра внутри интервала.", "unit": "м/с", "notes": "Из-за малого покрытия такие показатели годятся только для ориентировочной оценки."},
    "max_wind_speed_ms": {"description": "Максимальная скорость ветра внутри интервала.", "unit": "м/с", "notes": "Из-за малого покрытия такие показатели годятся только для ориентировочной оценки."},
    "coverage_wind": {"description": "Доля дней интервала, покрытых доступными наблюдениями ветра.", "unit": "доля от 0 до 1", "notes": "Низкое покрытие означает, что ветер нельзя интерпретировать как надёжный причинный фактор."},
    "delivery_qc_flags": {"description": "Человеко-понятное перечисление основных ограничений и предупреждений по записи.", "unit": "", "notes": ""},
    "delivery_qc_note": {"description": "Человеко-понятный комментарий по качеству данных и ограничениям интерпретации.", "unit": "", "notes": ""},
    "delivery_record_note": {"description": "Дополнительное примечание по записи для внешнего читателя.", "unit": "", "notes": ""},
}

FLAG_LABELS: dict[str, str] = {
    "SOURCE_QC_PRESENT": "Есть исходные замечания по качеству записи",
    "LOW_COVERAGE_WIND": "Низкое покрытие данными по ветру",
    "LOW_COVERAGE_WATER": "Низкое покрытие данными по воде",
    "YEAR_ONLY_WATER_SOURCE": "Вода представлена только как годовой контекст без точных дат",
    "CONFLICTING_SHORELINE_DUPLICATES_IN_PROFILE_CONTEXT": "В контексте профиля есть конфликтующие дубли береговых наблюдений",
    "INVALID_NUMERIC": "В исходной записи есть некорректное числовое значение",
    "MISSING_NUMERIC": "В исходной записи отсутствуют числовые значения",
    "ROW_NOTE": "В исходной строке есть примечание",
    "DUPLICATE_OBS_KEY": "Есть дублирующаяся запись наблюдения по ключу дата-участок-профиль",
}

MISSING_REASON_LABELS: dict[str, str] = {
    "all_numeric_fields_missing": "В строке отсутствуют все основные числовые значения.",
}

VALUE_LABELS_BY_COLUMN: dict[str, dict[str, str]] = {
    "water_time_resolution": {
        "year_only": "Только по годам",
        "full_date": "Полные даты",
    },
    "scope_status": {
        "reviewed": "Проверено",
        "needs_review": "Требует проверки",
    },
}

BOOLEAN_COLUMNS = {
    "has_conflicting_shoreline_duplicates",
}

_NOTE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"No wind observations fall inside this shoreline interval in the currently available local meteorological workbook\."),
        "В доступном локальном метеорологическом ряду нет наблюдений ветра, попадающих в этот береговой интервал.",
    ),
    (
        re.compile(r"Wind coverage is ([0-9.]+), below the 0\.8 threshold; interval summaries should be treated as screening-only\."),
        r"Покрытие интервала данными по ветру составляет \1, что ниже порога 0.8; такие показатели подходят только для ориентировочной оценки.",
    ),
    (
        re.compile(r"Water coverage is ([0-9.]+), below the 0\.8 threshold\."),
        r"Покрытие интервала данными по воде составляет \1, что ниже порога 0.8.",
    ),
    (
        re.compile(r"Water values are available only as annual year-level observations without full dates\."),
        "Данные по воде доступны только как годовые значения без точных дат наблюдений.",
    ),
    (
        re.compile(r"Water metrics aggregate annual mean/max levels for (.+?) in meters absolute over inclusive calendar years\."),
        r"Показатели по воде агрегируют средние годовые и максимальные годовые уровни для \1 в абсолютных отметках по полным календарным годам интервала.",
    ),
    (
        re.compile(r"The source provides years only, so this is section-level hydrological context rather than a fully dated local causal series\."),
        "Источник содержит только годы, поэтому это общий гидрологический контекст по участку водохранилища, а не локальный датированный ряд для причинной интерпретации.",
    ),
    (
        re.compile(
            r"В исходном shoreline-слое для профиля есть (\d+) конфликтующих duplicate-групп\(ы\); даты ключей: ([^.]+)\. Эти строки не удалялись молча и должны учитываться как источник осторожности\."
        ),
        r"В исходном слое береговых наблюдений для профиля есть \1 конфликтующих групп дублирующихся записей; проблемные даты: \2. Эти записи не удалялись автоматически и требуют осторожной интерпретации.",
    ),
]


def clean_optional_text(value: Any) -> str:
    """Return a normalized string or an empty string for missing values."""

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    return str(value).replace("\xa0", " ").strip()


def parse_boolish(value: Any) -> bool:
    """Interpret common string and numeric boolean-like values safely."""

    text = clean_optional_text(value).lower()
    if not text:
        return False
    if text in {"true", "1", "yes", "да"}:
        return True
    if text in {"false", "0", "no", "нет"}:
        return False
    return bool(value)


def join_nonempty_parts(parts: list[str]) -> str | None:
    """Join non-empty text parts with a single space."""

    cleaned = [part.strip() for part in parts if clean_optional_text(part)]
    return " ".join(cleaned) if cleaned else None


def translate_flag_codes(value: Any) -> str | None:
    """Translate semicolon-separated QC flag codes into Russian display text."""

    text = clean_optional_text(value)
    if not text:
        return None
    labels = [FLAG_LABELS.get(code.strip(), code.strip()) for code in text.split(";") if code.strip()]
    return "; ".join(labels) if labels else None


def translate_missing_reason(value: Any) -> str | None:
    """Translate missingness codes into Russian display text."""

    text = clean_optional_text(value)
    if not text:
        return None
    return MISSING_REASON_LABELS.get(text, text)


def translate_qc_note_text(value: Any) -> str | None:
    """Translate known QC note fragments into Russian."""

    text = clean_optional_text(value)
    if not text:
        return None

    translated = text.replace("missing_obs_date", "дата отсутствует")
    for pattern, replacement in _NOTE_PATTERNS:
        translated = pattern.sub(replacement, translated)
    return translated


def translate_value_for_column(column: str, value: Any) -> Any:
    """Translate single values for delivery display where a mapping is defined."""

    text = clean_optional_text(value)
    if not text:
        return pd.NA if pd.isna(value) or value is None else value

    if column in BOOLEAN_COLUMNS:
        if parse_boolish(value):
            return "Да"
        return "Нет"

    if column in VALUE_LABELS_BY_COLUMN:
        return VALUE_LABELS_BY_COLUMN[column].get(text, text)

    return value


def humanize_base_point_status(row: pd.Series) -> str:
    """Map technical base-point status fields to a human-readable Russian status."""

    if parse_boolish(row.get("is_new")):
        return "Новая"
    if parse_boolish(row.get("is_reinstalled")):
        return "Переустановлена"
    if parse_boolish(row.get("is_calculated")):
        return "Расчетные координаты"
    if parse_boolish(row.get("is_refined")):
        return "Координаты уточнены"

    point_status = clean_optional_text(row.get("point_status")).lower()
    if point_status == "refined":
        return "Координаты уточнены"
    if point_status == "original":
        return "Исходная"
    return "Не уточнено"


def humanize_base_point_note(row: pd.Series) -> str | None:
    """Convert internal base-point review markers into a short human note."""

    parts: list[str] = []
    review_reason = clean_optional_text(row.get("review_reason"))
    note_raw = clean_optional_text(row.get("note_raw"))

    if parse_boolish(row.get("has_uncertain_date")) or "date missing or partial in source" in review_reason:
        parts.append("Дата указана неполностью.")
    if "Coordinate assignment inferred from the document block" in review_reason:
        parts.append(
            "Требуется уточнение привязки точки: координаты были определены по блоку документа и конфликтуют с известным кластером точки 3 участка Нижний Ураков."
        )
    if note_raw:
        parts.append(note_raw)
    return join_nonempty_parts(parts)


def humanize_base_point_date(row: pd.Series) -> str | None:
    """Return the best available date token for delivery export."""

    obs_date = clean_optional_text(row.get("obs_date"))
    if obs_date:
        return obs_date
    obs_date_raw = clean_optional_text(row.get("obs_date_raw"))
    return obs_date_raw or None
