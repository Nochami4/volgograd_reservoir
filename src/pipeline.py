"""Main dataset build pipeline."""

from __future__ import annotations

from src.analysis.build_analysis_ready import build_analysis_ready
from src.analysis.build_interval_metrics import build_interval_metrics
from src.parsers.build_base_points_stub import build_base_points_stub
from src.parsers.build_profiles import build_profiles
from src.parsers.build_shoreline_observations import build_shoreline_observations
from src.parsers.build_sites import build_sites
from src.parsers.build_water_levels_raw import build_water_levels_raw
from src.parsers.build_wind_obs_hourly import build_wind_obs_hourly
from src.parsers.common import setup_logging
from src.qc.run_qc import run_qc


def main() -> None:
    """Run the full ETL and QC flow in the requested order."""

    logger = setup_logging("pipeline")
    logger.info("Starting dataset build pipeline")
    build_sites()
    build_profiles()
    build_shoreline_observations()
    build_wind_obs_hourly()
    build_water_levels_raw()
    build_base_points_stub()
    build_interval_metrics()
    build_analysis_ready()
    run_qc()
    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()
