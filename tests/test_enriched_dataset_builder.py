import pandas as pd

from src.analysis.build_final_dataset_enriched_open_sources import apply_water_fill_metadata, build_wind_enrichment, parse_noaa_wnd_speed


def test_parse_noaa_wnd_speed_reads_tenths_ms_and_missing_code() -> None:
    assert parse_noaa_wnd_speed("070,1,N,0010,1") == 1.0
    assert parse_noaa_wnd_speed("999,9,C,9999,9") is None
    assert parse_noaa_wnd_speed("") is None


def test_build_wind_enrichment_prioritizes_local_then_noaa_fill() -> None:
    master = pd.DataFrame(
        [
            {"interval_id": "a", "date_start": pd.Timestamp("2021-01-01"), "date_end": pd.Timestamp("2021-01-10")},
            {"interval_id": "b", "date_start": pd.Timestamp("2021-01-01"), "date_end": pd.Timestamp("2021-01-10")},
            {"interval_id": "c", "date_start": pd.Timestamp("2021-02-01"), "date_end": pd.Timestamp("2021-02-10")},
        ]
    )
    analysis_ready = pd.DataFrame(
        [
            {
                "interval_id": "a",
                "n_wind_obs": 3,
                "mean_wind_speed_ms": 5.0,
                "max_wind_speed_ms": 7.0,
                "coverage_wind": 0.3,
            },
            {
                "interval_id": "b",
                "n_wind_obs": 0,
                "mean_wind_speed_ms": pd.NA,
                "max_wind_speed_ms": pd.NA,
                "coverage_wind": 0.0,
            },
            {
                "interval_id": "c",
                "n_wind_obs": 0,
                "mean_wind_speed_ms": pd.NA,
                "max_wind_speed_ms": pd.NA,
                "coverage_wind": 0.0,
            },
        ]
    )
    noaa_obs = pd.DataFrame(
        [
            {"obs_datetime_local": pd.Timestamp("2021-01-03 06:00:00"), "obs_date": pd.Timestamp("2021-01-03"), "wind_speed_ms": 4.0},
            {"obs_datetime_local": pd.Timestamp("2021-01-04 06:00:00"), "obs_date": pd.Timestamp("2021-01-04"), "wind_speed_ms": 6.0},
        ]
    )
    validation = {
        "usable_for_fill": True,
        "station_line": "343630 99999 KAMYSIN RS",
    }

    enriched, stats = build_wind_enrichment(
        master_df=master,
        analysis_ready_df=analysis_ready,
        noaa_obs_df=noaa_obs,
        validation=validation,
    )

    row_a = enriched.loc[enriched["interval_id"] == "a"].iloc[0]
    row_b = enriched.loc[enriched["interval_id"] == "b"].iloc[0]
    row_c = enriched.loc[enriched["interval_id"] == "c"].iloc[0]

    assert row_a["mean_wind_speed_ms"] == 5.0
    assert row_a["wind_fill_status"] == "exact_source_local_speed_summary"
    assert row_b["mean_wind_speed_ms"] == 5.0
    assert row_b["max_wind_speed_ms"] == 6.0
    assert row_b["n_wind_obs"] == 2
    assert row_b["wind_fill_status"] == "same_station_compatible_source_fill"
    assert pd.isna(row_c["mean_wind_speed_ms"])
    assert row_c["wind_fill_status"] == "not_filled_no_valid_interval_source"
    assert stats["rows_with_local_wind_context"] == 1
    assert stats["rows_with_local_wind_speed_summary"] == 1
    assert stats["rows_filled_from_noaa"] == 1
    assert stats["rows_without_wind_mean_after_enrichment"] == 1


def test_apply_water_fill_metadata_marks_retained_and_missing_rows() -> None:
    enriched = pd.DataFrame(
        [
            {
                "mean_water_level_mean_annual_m_abs": 14.5,
                "max_water_level_mean_annual_m_abs": 14.7,
                "min_water_level_mean_annual_m_abs": 14.3,
                "range_water_level_mean_annual_m_abs": 0.4,
                "mean_water_level_max_annual_m_abs": 15.5,
                "max_water_level_max_annual_m_abs": 15.8,
                "min_water_level_max_annual_m_abs": 15.2,
                "range_water_level_max_annual_m_abs": 0.6,
            },
            {
                "mean_water_level_mean_annual_m_abs": pd.NA,
                "max_water_level_mean_annual_m_abs": pd.NA,
                "min_water_level_mean_annual_m_abs": pd.NA,
                "range_water_level_mean_annual_m_abs": pd.NA,
                "mean_water_level_max_annual_m_abs": pd.NA,
                "max_water_level_max_annual_m_abs": pd.NA,
                "min_water_level_max_annual_m_abs": pd.NA,
                "range_water_level_max_annual_m_abs": pd.NA,
            },
        ]
    )

    result, stats = apply_water_fill_metadata(enriched)

    assert result.loc[0, "water_fill_status"] == "existing_master_context_retained"
    assert result.loc[1, "water_fill_status"] == "not_filled_no_valid_official_extension_found"
    assert stats["rows_with_master_water_context"] == 1
    assert stats["rows_with_water_gap_retained"] == 1
