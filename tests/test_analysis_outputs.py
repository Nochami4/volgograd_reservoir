import pandas as pd
import pytest

from src.analysis.build_analysis_ready import aggregate_water_for_interval
from src.analysis.first_stage_analysis import build_final_dataset_for_modeling, build_profile_correlation_diagnostics, build_profile_correlation_tables


def test_aggregate_water_for_interval_uses_resolved_mean_and_max_columns() -> None:
    interval_row = pd.Series(
        {
            "site_id": "site_1",
            "date_start_dt": pd.Timestamp("2000-06-01"),
            "date_end_dt": pd.Timestamp("2001-08-01"),
        }
    )
    water_df = pd.DataFrame(
        [
            {
                "site_id": "site_1",
                "year": 2000,
                "obs_date": None,
                "water_level_mean_annual_m_abs": 14.5,
                "water_level_max_annual_m_abs": 15.2,
                "water_section_name": "Нижний участок (озерный)",
                "water_context_scope": "Нижний участок (озерный)",
                "water_variable_is_ambiguous": False,
            },
            {
                "site_id": "site_1",
                "year": 2001,
                "obs_date": None,
                "water_level_mean_annual_m_abs": 14.7,
                "water_level_max_annual_m_abs": 15.4,
                "water_section_name": "Нижний участок (озерный)",
                "water_context_scope": "Нижний участок (озерный)",
                "water_variable_is_ambiguous": False,
            },
        ]
    )

    result = aggregate_water_for_interval(interval_row, water_df)

    assert result["n_water_obs"] == 2
    assert result["mean_water_level_mean_annual_m_abs"] == pytest.approx(14.6)
    assert result["max_water_level_mean_annual_m_abs"] == pytest.approx(14.7)
    assert result["mean_water_level_max_annual_m_abs"] == pytest.approx(15.3)
    assert result["mean_level"] == pytest.approx(14.6)
    assert result["water_time_resolution"] == "year_only"
    assert result["water_variable_is_ambiguous"] is False


def test_profile_correlation_diagnostics_explain_negative_signed_relation() -> None:
    subset = pd.DataFrame(
        [
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p1",
                "profile_name": "Профиль 1",
                "date_start": "2000-01-01",
                "date_end": "2001-01-01",
                "retreat_m": 1.0,
                "retreat_rate_m_per_year": 1.0,
                "retreat_abs_m": 1.0,
                "has_conflicting_shoreline_duplicates": False,
            },
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p1",
                "profile_name": "Профиль 1",
                "date_start": "2001-01-01",
                "date_end": "2002-01-01",
                "retreat_m": 2.0,
                "retreat_rate_m_per_year": 2.0,
                "retreat_abs_m": 2.0,
                "has_conflicting_shoreline_duplicates": False,
            },
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p1",
                "profile_name": "Профиль 1",
                "date_start": "2002-01-01",
                "date_end": "2003-01-01",
                "retreat_m": -0.05,
                "retreat_rate_m_per_year": -0.05,
                "retreat_abs_m": 0.05,
                "has_conflicting_shoreline_duplicates": False,
            },
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p2",
                "profile_name": "Профиль 2",
                "date_start": "2000-01-01",
                "date_end": "2001-01-01",
                "retreat_m": -1.0,
                "retreat_rate_m_per_year": -1.0,
                "retreat_abs_m": 1.0,
                "has_conflicting_shoreline_duplicates": True,
            },
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p2",
                "profile_name": "Профиль 2",
                "date_start": "2001-01-01",
                "date_end": "2002-01-01",
                "retreat_m": -2.0,
                "retreat_rate_m_per_year": -2.0,
                "retreat_abs_m": 2.0,
                "has_conflicting_shoreline_duplicates": True,
            },
            {
                "site_id": "site_1",
                "site_name": "Участок 1",
                "profile_id": "p2",
                "profile_name": "Профиль 2",
                "date_start": "2002-01-01",
                "date_end": "2003-01-01",
                "retreat_m": 0.02,
                "retreat_rate_m_per_year": 0.02,
                "retreat_abs_m": 0.02,
                "has_conflicting_shoreline_duplicates": True,
            },
        ]
    )

    summary_df, _ = build_profile_correlation_tables(subset)
    diagnostics_df = build_profile_correlation_diagnostics(summary_df)

    assert len(summary_df) == 1
    summary_row = summary_df.iloc[0]
    assert summary_row["n_overlap_intervals"] == 3
    assert summary_row["pearson_retreat_rate_m_per_year"] == pytest.approx(-0.9997, abs=1e-3)
    assert summary_row["same_sign_share"] == pytest.approx(0.0)
    assert summary_row["opposite_sign_share"] == pytest.approx(2 / 3)
    assert summary_row["zero_or_near_zero_share"] == pytest.approx(1 / 3)
    assert bool(summary_row["duplicate_conflict_context_any"]) is True
    assert "negative_signed_relation" in str(summary_row["short_note"])
    assert "conflict_context_read_with_caution" in str(summary_row["short_note"])

    diagnostics_row = diagnostics_df.iloc[0]
    assert diagnostics_row["n_overlap"] == 3
    assert diagnostics_row["pearson_rate"] == pytest.approx(summary_row["pearson_retreat_rate_m_per_year"])
    assert "signed_retreat_rate_m_per_year" in diagnostics_row["correlation_basis"]


def test_build_final_dataset_for_modeling_drops_empty_and_duplicate_alias_columns(tmp_path) -> None:
    subset = pd.DataFrame(
        [
            {
                "interval_id": "a",
                "site_id": "site_1",
                "site_name": "Site 1",
                "profile_id": "p1",
                "profile_num": 1,
                "profile_name": "Profile 1",
                "date_start": "2000-01-01",
                "date_end": "2001-01-01",
                "days_between": 366,
                "years_between": 366 / 365.25,
                "retreat_m": 1.0,
                "retreat_rate_m_per_year": 1.0,
                "retreat_abs_m": 1.0,
                "retreat_rate_abs_m_per_year": 1.0,
                "n_raw_points_used": 2,
                "calc_method": "mean_brow_position_pn_by_date_then_difference",
                "shore_type": "Правый",
                "shore_orientation_text": "Юго-восток",
                "shore_orientation_deg": 135.0,
                "exposure_sectors_text": "В, ЮВ, Ю",
                "lithology_text": "Суглинки",
                "lithology_class": "Среднее по твердости",
                "notes": pd.NA,
                "start_date": "2000-01-01",
                "end_date": "2005-01-01",
                "n_observations": 5,
                "n_water_obs": 1,
                "coverage_water": 1.0,
                "mean_water_level_mean_annual_m_abs": 14.5,
                "max_water_level_mean_annual_m_abs": 14.5,
                "min_water_level_mean_annual_m_abs": 14.5,
                "range_water_level_mean_annual_m_abs": 0.0,
                "mean_water_level_max_annual_m_abs": 15.2,
                "max_water_level_max_annual_m_abs": 15.2,
                "min_water_level_max_annual_m_abs": 15.2,
                "range_water_level_max_annual_m_abs": 0.0,
                "water_context_scope": "Нижний участок (озерный)",
                "water_time_resolution": "year_only",
                "in_project_scope": True,
                "scope_status": "reviewed",
                "scope_note": "scope",
                "history_start_year": 2000,
                "history_start_group": "поздний блок (с 1986/1987)",
                "has_conflicting_shoreline_duplicates": False,
                "conflicting_duplicate_group_count": 0,
                "duplicate_conflict_obs_dates": pd.NA,
                "duplicate_conflict_note": pd.NA,
                "qc_flag": pd.NA,
                "qc_note": pd.NA,
                "qc_flag_analysis": "YEAR_ONLY_WATER_SOURCE",
                "qc_note_analysis": "Water note",
                "qc_flag_analysis_safe": "YEAR_ONLY_WATER_SOURCE",
                "qc_note_analysis_safe": "Water note",
                "mean_level": 14.5,
                "max_level": 14.5,
                "min_level": 14.5,
                "range_level": 0.0,
            }
        ]
    )

    output_path = tmp_path / "final_dataset_for_modeling.csv"
    build_final_dataset_for_modeling(subset, output_path=output_path)
    final_df = pd.read_csv(output_path)

    assert "notes" not in final_df.columns
    assert "qc_note" not in final_df.columns
    assert "mean_level" not in final_df.columns
    assert "max_level" not in final_df.columns
    assert "calc_method" not in final_df.columns
    assert "scope_status" not in final_df.columns
    assert "qc_flag_analysis" not in final_df.columns
    assert "qc_note_analysis" not in final_df.columns
    assert "mean_water_level_mean_annual_m_abs" in final_df.columns
    assert "qc_flag_analysis_safe" in final_df.columns
    assert "qc_note_analysis_safe" in final_df.columns
