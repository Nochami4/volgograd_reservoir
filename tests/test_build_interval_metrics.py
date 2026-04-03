import pandas as pd
import pytest

from src.analysis.build_interval_metrics import compute_interval_metrics


def test_compute_interval_metrics_synthetic_case() -> None:
    observations = pd.DataFrame(
        [
            {
                "obs_id": "a",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2020-01-01",
                "brow_position_pn_m": 100.0,
                "qc_flag": None,
            },
            {
                "obs_id": "b",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2021-01-01",
                "brow_position_pn_m": 110.0,
                "qc_flag": None,
            },
            {
                "obs_id": "c",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2022-01-01",
                "brow_position_pn_m": 130.0,
                "qc_flag": None,
            },
        ]
    )

    result = compute_interval_metrics(observations)

    assert len(result) == 2
    first = result.iloc[0]
    assert first["retreat_m"] == pytest.approx(10.0)
    assert first["days_between"] == 366
    assert first["retreat_rate_m_per_year"] == pytest.approx(10.0 / (366 / 365.25))


def test_compute_interval_metrics_aggregates_same_day_points() -> None:
    observations = pd.DataFrame(
        [
            {
                "obs_id": "a1",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2020-01-01",
                "brow_position_pn_m": 100.0,
                "qc_flag": None,
            },
            {
                "obs_id": "a2",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2020-01-01",
                "brow_position_pn_m": 102.0,
                "qc_flag": None,
            },
            {
                "obs_id": "b",
                "site_id": "site_1",
                "profile_id": "site_1_profile_1",
                "obs_date": "2021-01-01",
                "brow_position_pn_m": 112.0,
                "qc_flag": None,
            },
        ]
    )

    result = compute_interval_metrics(observations)

    assert len(result) == 1
    assert result.iloc[0]["brow_pos_start_m"] == pytest.approx(101.0)
    assert result.iloc[0]["qc_flag"] == "AGGREGATED_RAW_POINTS"
