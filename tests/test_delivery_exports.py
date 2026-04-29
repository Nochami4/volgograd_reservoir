import pandas as pd

from src.export.build_delivery_exports import build_analysis_ready_delivery_frame, finalize_delivery_frame
from src.export.delivery_labels import humanize_base_point_date, humanize_base_point_note, humanize_base_point_status, translate_flag_codes, translate_qc_note_text


def test_translate_flag_codes_returns_russian_labels() -> None:
    value = "SOURCE_QC_PRESENT;LOW_COVERAGE_WIND;YEAR_ONLY_WATER_SOURCE"
    translated = translate_flag_codes(value)

    assert translated == (
        "Есть исходные замечания по качеству записи; "
        "Низкое покрытие данными по ветру; "
        "Вода представлена только как годовой контекст без точных дат"
    )


def test_translate_qc_note_text_rewrites_known_english_patterns() -> None:
    note = (
        "Wind coverage is 0.12, below the 0.8 threshold; interval summaries should be treated as screening-only. "
        "Water values are available only as annual year-level observations without full dates."
    )
    translated = translate_qc_note_text(note)

    assert "Покрытие интервала данными по ветру составляет 0.12" in translated
    assert "Данные по воде доступны только как годовые значения без точных дат наблюдений." in translated


def test_base_point_humanization_avoids_manual_review_wording() -> None:
    row = pd.Series(
        {
            "obs_date": pd.NA,
            "obs_date_raw": ".07.2011",
            "point_status": "original",
            "is_calculated": "False",
            "is_reinstalled": "False",
            "is_new": "False",
            "is_refined": "False",
            "has_uncertain_date": "True",
            "review_reason": "date missing or partial in source",
            "note_raw": "d=2,5 | , h-1",
        }
    )

    assert humanize_base_point_status(row) == "Исходная"
    assert humanize_base_point_date(row) == ".07.2011"
    assert humanize_base_point_note(row) == "Дата указана неполностью. d=2,5 | , h-1"


def test_finalize_delivery_frame_drops_empty_columns_and_renames_headers() -> None:
    frame = pd.DataFrame(
        [
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "notes": pd.NA,
                "has_conflicting_shoreline_duplicates": True,
            }
        ]
    )

    delivery_frame, active_columns = finalize_delivery_frame(frame)

    assert "notes" not in active_columns
    assert "site_id" in delivery_frame.columns
    assert "site_name" in delivery_frame.columns
    assert "has_conflicting_shoreline_duplicates" in delivery_frame.columns
    assert delivery_frame.loc[0, "has_conflicting_shoreline_duplicates"] == "Да"


def test_build_analysis_ready_delivery_frame_keeps_wind_columns_and_translated_qc() -> None:
    frame = build_analysis_ready_delivery_frame()

    assert "n_wind_obs" in frame.columns
    assert "mean_wind_speed_ms" in frame.columns
    assert "coverage_wind" in frame.columns
    assert "delivery_qc_flags" in frame.columns
    assert "delivery_qc_note" in frame.columns
    assert len(frame) > 0
