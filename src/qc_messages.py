"""Centralized human-readable Russian text for QC, scope, and review outputs."""

from __future__ import annotations

from datetime import date, datetime


LOWER_SECTION_FALLBACK_SCOPE = "Общий годовой гидрологический контекст по нижнему участку водохранилища"

SCOPE_NOTE_PROJECT_LISTED = "Участок указан на проектной карте и в исходных документах по составу исследования."
SCOPE_NOTE_MOLCHANOVSKY_MATCH = "На проектной карте участок указан как «Молчановский» и сопоставлен с участком «Молчановка»."
SCOPE_NOTE_METADATA_ONLY = "Участок есть в книге метаданных, но не входит в девять участков, подтверждённых картой и исходными документами."
SCOPE_NOTE_NEEDS_REVIEW = "Статус включения участка в проект пока требует отдельной проверки."
SCOPE_NOTE_NOT_CONFIRMED_IN_REVIEW = "Участок пока не подтверждён явно в `data/interim/site_scope_review.csv`."

LEGACY_SCOPE_NOTE_TRANSLATIONS = {
    "Listed on the project map and in the source-document scope.": SCOPE_NOTE_PROJECT_LISTED,
    "Listed on the project map as Молчановский and matched to site Молчановка.": SCOPE_NOTE_MOLCHANOVSKY_MATCH,
    "Present in the metadata workbook but not listed among the nine scoped map/source-document sites.": SCOPE_NOTE_METADATA_ONLY,
    "Scope status still needs review.": SCOPE_NOTE_NEEDS_REVIEW,
}

BASE_POINT_REVIEW_REASON_DATE = "Дата в источнике отсутствует или указана неполностью."
BASE_POINT_REVIEW_REASON_COORD_CONFLICT = (
    "Привязка координаты восстановлена по блоку документа, но конфликтует с известным кластером координат точки 3 участка Нижний Ураков."
)
BASE_POINT_REVIEW_REASON_SITE_UNRESOLVED = "Не удалось надёжно определить `site_id`."
BASE_POINT_REVIEW_REASON_LABEL_TRUNCATED = "Обозначение базисной точки в извлечённом тексте выглядит обрезанным."
BASE_POINT_REVIEW_REASON_COORD_INCOMPLETE = "В источнике неполная пара координат."
BASE_POINT_REVIEW_REASON_URAKOV_CLUSTER = (
    "Точка 3 участка Ураков Бугор от 2019-08-14 попадает в подозрительный координатный кластер точки 3 участка Нижний Ураков."
)

BASE_POINT_REVIEW_REASON_TRANSLATIONS = {
    "site_id unresolved": BASE_POINT_REVIEW_REASON_SITE_UNRESOLVED,
    "date missing or partial in source": BASE_POINT_REVIEW_REASON_DATE,
    "base point label truncated in extracted text": BASE_POINT_REVIEW_REASON_LABEL_TRUNCATED,
    "coordinate pair incomplete": BASE_POINT_REVIEW_REASON_COORD_INCOMPLETE,
    "Urakov Bugor point 3 on 2019-08-14 matches the suspicious Nizhniy Urakov point-3 coordinate cluster": BASE_POINT_REVIEW_REASON_URAKOV_CLUSTER,
    "Coordinate assignment inferred from the document block and conflicts with the known Nizhniy Urakov point-3 coordinate cluster.": BASE_POINT_REVIEW_REASON_COORD_CONFLICT,
}


def translate_review_reason_fragment(fragment: str) -> str:
    """Translate a known base-point review-reason fragment into Russian."""

    return BASE_POINT_REVIEW_REASON_TRANSLATIONS.get(str(fragment).strip(), str(fragment).strip())


def water_interval_aggregate_note(section_label: str) -> str:
    """Explain annual water aggregation for interval-level joins."""

    return (
        f"Показатели воды рассчитаны по годовым средним и максимальным уровням для блока «{section_label}» "
        "в абсолютных метрах за календарные годы, входящие в интервал. "
        "Источник содержит только годы, поэтому это гидрологический контекст по участку, "
        "а не полностью датированный локальный причинный ряд."
    )


def analysis_no_wind_obs_note() -> str:
    """Explain zero wind coverage within an interval."""

    return "В доступной локальной метеорологической таблице нет наблюдений ветра, попадающих внутрь этого берегового интервала."


def analysis_low_wind_coverage_note(coverage: float, threshold: float) -> str:
    """Explain low wind coverage within an interval."""

    return (
        f"Покрытие ветровыми наблюдениями составляет {coverage:.2f}, что ниже принятого порога {threshold:.1f}; "
        "такие интервальные сводки подходят только для ориентировочной оценки."
    )


def analysis_low_water_coverage_note(coverage: float, threshold: float) -> str:
    """Explain low water coverage within an interval."""

    return f"Покрытие уровнями воды составляет {coverage:.2f}, что ниже принятого порога {threshold:.1f}."


def analysis_year_only_water_note() -> str:
    """Explain year-only water timing."""

    return "Уровни воды доступны только как годовые значения без полной даты наблюдения."


def shoreline_duplicate_obs_key_note() -> str:
    """Explain duplicate observation keys in shoreline observations."""

    return (
        "Эта строка относится к той же комбинации участка, профиля и даты, что и другая строка береговых наблюдений; "
        "подробности см. в data/interim/shoreline_duplicate_report.csv."
    )


def interval_aggregated_points_note() -> str:
    """Explain interval construction from multiple raw points per date."""

    return "Положение бровки внутри одной даты наблюдения усреднялось перед расчётом интервала."


def interval_duplicate_dates_excluded_note(excluded_duplicate_dates: int) -> str:
    """Explain excluded conflicting duplicate dates before interval construction."""

    return (
        f"До построения интервалов были исключены {excluded_duplicate_dates} конфликтующих ключей наблюдений по датам; "
        "подробности см. в data/interim/shoreline_duplicate_report.csv."
    )


def water_year_only_source_note() -> str:
    """Explain that water observations are annual only."""

    return "Исходная книга содержит только годовые значения; полной даты наблюдения нет."


def water_shared_context_note() -> str:
    """Explain shared lower-section context across shoreline sites."""

    return (
        "Один и тот же годовой ряд для нижнего участка повторяется по береговым участкам "
        "как общий гидрологический контекст для присоединения к интервалам."
    )


def water_missing_value_note() -> str:
    """Explain missing annual water values within a source row."""

    return "В исходной строке отсутствует как минимум одно из восстановленных годовых значений уровня воды для нижнего участка."


def wind_explicit_date_parse_failed(token: str) -> str:
    """Explain that an explicit wind-date token could not be parsed safely."""

    return f"Явная дата «{token}» не была безопасно распознана."


def wind_explicit_date_too_many_year_digits(token: str) -> str:
    """Explain that an explicit wind-date token has too many year digits."""

    return f"Явная дата «{token}» содержит слишком много цифр в годе и была отклонена."


def wind_partial_excel_date_invalid(value: date | datetime, year_hint: int) -> str:
    """Explain invalid partial Excel date after substituting sheet year."""

    return f"Неполная Excel-дата «{value.isoformat()}» стала некорректной после подстановки года листа {year_hint}."


def wind_partial_numeric_date_invalid(token: str) -> str:
    """Explain invalid numeric day.month token."""

    return f"Неполный числовой токен даты «{token}» в формате день.месяц оказался некорректным."


def wind_partial_text_date_invalid(token: str) -> str:
    """Explain invalid text day.month token."""

    return f"Неполный текстовый токен даты «{token}» в формате день.месяц оказался некорректным."


def wind_row_without_reliable_date_note() -> str:
    """Explain that an observation-like row was retained without a reliable date."""

    return "Строка, похожая на наблюдение, сохранена, но надёжную дату по ячейке даты восстановить не удалось."


def wind_date_out_of_range_note(obs_date: date, min_date: date, max_date: date) -> str:
    """Explain that a parsed wind date falls outside the accepted range."""

    return f"Распознанная дата {obs_date.isoformat()} выходит за допустимый диапазон {min_date.isoformat()}..{max_date.isoformat()}."


def wind_datetime_out_of_range_note(obs_datetime: datetime) -> str:
    """Explain that a parsed wind datetime falls outside the accepted range."""

    return f"Распознанная дата и час {obs_datetime.isoformat()} выходят за допустимый диапазон."


def duplicate_conflict_profile_note(conflicting_group_count: int, duplicate_obs_dates: str) -> str:
    """Explain conflicting duplicate groups in the profile context."""

    return (
        f"В исходном слое береговых наблюдений для профиля есть {conflicting_group_count} конфликтующих групп дублирующихся записей; "
        f"проблемные даты: {duplicate_obs_dates}. Эти записи не удалялись автоматически и требуют осторожной интерпретации."
    )


def wind_fill_local_note() -> str:
    """Explain retained local wind aggregation."""

    return (
        "Для интервала сохранена агрегированная оценка из текущего локального слоя по Камышину; "
        "при низком `coverage_wind` она всё равно подходит только для ориентировочной интерпретации."
    )


def wind_fill_local_context_only_note() -> str:
    """Explain local context without a complete wind-speed summary."""

    return (
        "В текущем локальном слое по Камышину для интервала есть контекст наблюдений, "
        "но доступные строки не дали полной сводки по скорости ветра."
    )


def wind_fill_noaa_note() -> str:
    """Explain NOAA-based fill for missing wind summaries."""

    return (
        "Заполнение выполнено только по станции NOAA Global Hourly 34363099999 (KAMYSIN) "
        "и только там, где в локальном интервале отсутствовала сводка по ветру; "
        "метки времени были переведены из UTC в локальные даты Europe/Volgograd до интервальной агрегации. "
        "Совпадение метаданных станции подтверждено, но пересечение с локальной таблицей ограничено, "
        "поэтому такое заполнение нужно читать с осторожностью."
    )


def wind_fill_not_filled_note() -> str:
    """Explain that no validated wind fill source was available."""

    return (
        "Для этого интервала не нашлось подтверждённых наблюдений ветра ни из точного исходного слоя, "
        "ни из совместимого источника по той же станции; пропуск оставлен без заполнения."
    )


def wind_fill_validation_unusable_note() -> str:
    """Explain that NOAA candidate was rejected after validation."""

    return (
        "Кандидат по той же станции NOAA был загружен, но не принят для заполнения, "
        "потому что проверка перекрытия осталась ниже текущего порога совместимости."
    )


def water_fill_master_retained_note() -> str:
    """Explain retained master water context."""

    return (
        "Исходные значения воды из основного слоя сохранены без изменений. "
        "Они по-прежнему представляют только годовой контекст нижнего участка "
        "и не должны трактоваться как полностью датированный локальный фактор."
    )


def water_fill_gap_retained_note() -> str:
    """Explain retained water gap after enrichment audit."""

    return (
        "В этой сборке не был принят ни один дополнительный официальный ряд по нижнему участку "
        "с ясным происхождением и совместимой семантикой, поэтому пропуск основного слоя сохранён."
    )
