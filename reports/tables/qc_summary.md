# QC Summary

## Critical blockers before scientific analysis

- `data/processed/analysis_ready.csv`: Wind coverage is below the 0.8 threshold for many shoreline intervals.
- `data/processed/base_points.csv`: Some base points still have unresolved site_id and require manual review.
- `data/processed/shoreline_observations.csv`: Some shoreline observation keys remain conflicting and require manual review.
- `data/processed/water_levels_raw.csv`: Water level columns remain semantically ambiguous; only neutral technical aggregation is currently safe.

## Why tasks 3/5/6 are not analysis-safe yet

- Intervals with `coverage_wind >= 0.8`: 0
- Intervals with `coverage_wind > 0`: 101
- Wind years currently present in `wind_obs_hourly.csv`: [2011, 2021, 2022, 2023, 2024, 2025]
- Task families that rely on interval-level wind forcing remain draft until the wind time series is extended or matched to intervals more completely.

## Dataset-specific Diagnostics

- `base_points.csv`: 12 rows; unresolved `site_id`: 9; point_status distribution: {'unknown': 12}.
- `water_levels_raw.csv`: ambiguous neutral columns share = 1.000; rows without full date = 1.000.
- Sites without any extracted water rows: ['suvodskaya'].
- `shoreline_duplicate_report.csv`: 3 duplicate key group(s), including 3 conflicting group(s).
- `site_scope_review.csv`: 10 site(s) still marked `needs_review`.

| File | Rows | Columns | Duplicate rows | Duplicate keys | Key columns |
| --- | ---: | ---: | ---: | ---: | --- |
| `data/processed/analysis_ready.csv` | 587 | 50 | 0 | 0 | interval_id |
| `data/processed/base_points.csv` | 12 | 16 | 0 | 0 | base_point_id |
| `data/processed/interval_metrics.csv` | 587 | 17 | 0 | 0 | interval_id |
| `data/processed/profiles.csv` | 33 | 9 | 0 | 0 | profile_id |
| `data/processed/shoreline_observations.csv` | 716 | 19 | 0 | 0 | obs_id |
| `data/processed/sites.csv` | 10 | 11 | 0 | 0 | site_id |
| `data/processed/water_levels_raw.csv` | 297 | 15 | 0 | 0 | water_obs_id |
| `data/processed/wind_obs_hourly.csv` | 1596 | 20 | 0 | 0 | wind_obs_id |

## Additional Targeted Checks

### `data/processed/analysis_ready.csv`

- `analysis_low_coverage_wind_share`: n/a. Share of intervals flagged LOW_COVERAGE_WIND: 1.000.
- `analysis_low_coverage_water_share`: n/a. Share of intervals flagged LOW_COVERAGE_WATER: 0.300.
- `analysis_intervals_with_any_wind`: 101. Intervals with coverage_wind > 0.
- `analysis_intervals_with_adequate_wind`: 0. Intervals with coverage_wind >= 0.8.

### `data/processed/base_points.csv`

- `base_points_empty`: 0. An empty base_points layer means shoreline geometry remains only partially anchored.
- `base_points_missing_site_id_share`: n/a. Share of base-point rows without resolved site_id: 0.750.
- `base_points_point_status_distribution`: n/a. Point-status distribution: {'unknown': 12}.

### `data/processed/shoreline_observations.csv`

- `shoreline_duplicate_groups`: 3. Duplicate observation keys are described in data/interim/shoreline_duplicate_report.csv.

### `data/processed/water_levels_raw.csv`

- `ambiguous_level_columns_share`: n/a. Share of rows with ambiguous neutral level columns: 1.000.
- `water_rows_without_full_date_share`: n/a. Share of water rows without full obs_date: 1.000.

### `data/processed/wind_obs_hourly.csv`

- `wind_year_out_of_range`: 0. Accepted range: 1950..2025.
- `missing_obs_datetime_when_date_and_hour_present`: 0. Rows with parsed year and hour should carry obs_datetime unless flagged invalid upstream.

## Missingness By Column

### `data/processed/analysis_ready.csv`

| Column | Missing share |
| --- | ---: |
| `brow_pos_end_m` | 0.000 |
| `brow_pos_start_m` | 0.000 |
| `calc_method` | 0.000 |
| `coverage_water` | 0.000 |
| `coverage_wind` | 0.000 |
| `date_end` | 0.000 |
| `date_start` | 0.000 |
| `days_between` | 0.000 |
| `end_date` | 0.000 |
| `exposure_sectors_text` | 0.000 |
| `in_project_scope` | 1.000 |
| `interval_id` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `max_level` | 0.230 |
| `max_wind_speed_ms` | 0.896 |
| `mean_level` | 0.230 |
| `mean_wind_speed_ms` | 0.896 |
| `min_level` | 0.230 |
| `n_observations` | 0.000 |
| `n_raw_points_used` | 0.000 |
| `n_water_obs` | 0.000 |
| `n_wind_obs` | 0.000 |
| `notes` | 1.000 |
| `profile_id` | 0.000 |
| `profile_name` | 0.000 |
| `profile_num` | 0.002 |
| `qc_flag` | 0.811 |
| `qc_flag_analysis` | 0.000 |
| `qc_note` | 1.000 |
| `qc_note_analysis` | 0.000 |
| `range_level` | 0.230 |
| `retreat_abs_m` | 0.000 |
| `retreat_m` | 0.000 |
| `retreat_rate_abs_m_per_year` | 0.000 |
| `retreat_rate_m_per_year` | 0.000 |
| `scope_note` | 0.000 |
| `scope_status` | 0.000 |
| `sheet_name_raw` | 0.000 |
| `shore_orientation_deg` | 0.000 |
| `shore_orientation_text` | 0.000 |
| `shore_type` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `source_file` | 0.000 |
| `source_file_profile` | 0.000 |
| `source_sheet` | 0.000 |
| `start_date` | 0.000 |
| `water_variable_is_ambiguous` | 0.000 |
| `years_between` | 0.000 |

### `data/processed/base_points.csv`

| Column | Missing share |
| --- | ---: |
| `accuracy_m` | 1.000 |
| `base_point_id` | 0.000 |
| `base_point_name` | 0.000 |
| `is_calculated` | 0.000 |
| `is_new` | 0.000 |
| `is_reinstalled` | 0.000 |
| `obs_date` | 0.417 |
| `point_status` | 0.000 |
| `qc_flag` | 0.000 |
| `qc_note` | 0.000 |
| `site_id` | 0.750 |
| `source_file` | 0.000 |
| `source_row_ref` | 0.000 |
| `status_note` | 1.000 |
| `x_m` | 0.167 |
| `y_m` | 0.167 |

### `data/processed/interval_metrics.csv`

| Column | Missing share |
| --- | ---: |
| `brow_pos_end_m` | 0.000 |
| `brow_pos_start_m` | 0.000 |
| `calc_method` | 0.000 |
| `date_end` | 0.000 |
| `date_start` | 0.000 |
| `days_between` | 0.000 |
| `interval_id` | 0.000 |
| `n_raw_points_used` | 0.000 |
| `profile_id` | 0.000 |
| `qc_flag` | 0.811 |
| `qc_note` | 1.000 |
| `retreat_abs_m` | 0.000 |
| `retreat_m` | 0.000 |
| `retreat_rate_abs_m_per_year` | 0.000 |
| `retreat_rate_m_per_year` | 0.000 |
| `site_id` | 0.000 |
| `years_between` | 0.000 |

### `data/processed/profiles.csv`

| Column | Missing share |
| --- | ---: |
| `end_date` | 0.000 |
| `n_observations` | 0.000 |
| `profile_id` | 0.000 |
| `profile_name` | 0.000 |
| `profile_num` | 0.030 |
| `sheet_name_raw` | 0.000 |
| `site_id` | 0.000 |
| `source_file` | 0.000 |
| `start_date` | 0.000 |

### `data/processed/shoreline_observations.csv`

| Column | Missing share |
| --- | ---: |
| `brow_position_pn_m` | 0.134 |
| `brow_position_raw_m` | 0.122 |
| `gp_to_pn_offset_m` | 0.149 |
| `is_missing` | 0.000 |
| `measured_point_name` | 0.091 |
| `missing_reason` | 0.878 |
| `obs_date` | 0.046 |
| `obs_id` | 0.000 |
| `pn_name` | 0.120 |
| `profile_id` | 0.000 |
| `qc_flag` | 0.768 |
| `qc_note` | 0.867 |
| `raw_measured_distance_m` | 0.122 |
| `raw_value_text` | 0.954 |
| `site_id` | 0.000 |
| `source_file` | 0.000 |
| `source_row` | 0.000 |
| `source_sheet` | 0.000 |
| `survey_year` | 0.046 |

### `data/processed/sites.csv`

| Column | Missing share |
| --- | ---: |
| `exposure_sectors_text` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `notes` | 1.000 |
| `shore_orientation_deg` | 0.000 |
| `shore_orientation_text` | 0.000 |
| `shore_type` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `source_file` | 0.000 |
| `source_sheet` | 0.000 |

### `data/processed/water_levels_raw.csv`

| Column | Missing share |
| --- | ---: |
| `is_missing` | 0.000 |
| `level_col_1_m` | 0.000 |
| `level_col_2_m` | 0.000 |
| `missing_reason` | 1.000 |
| `obs_date` | 1.000 |
| `preferred_level_col` | 0.000 |
| `qc_flag` | 0.000 |
| `qc_note` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `source_file` | 0.000 |
| `source_row` | 0.000 |
| `source_sheet` | 0.000 |
| `water_obs_id` | 0.000 |
| `year` | 0.000 |

### `data/processed/wind_obs_hourly.csv`

| Column | Missing share |
| --- | ---: |
| `day` | 0.001 |
| `hour` | 0.000 |
| `is_missing` | 0.000 |
| `missing_reason` | 0.986 |
| `month` | 0.001 |
| `obs_date` | 0.001 |
| `obs_datetime` | 0.001 |
| `qc_flag` | 0.986 |
| `qc_note` | 0.999 |
| `source_file` | 0.000 |
| `source_row` | 0.000 |
| `source_sheet` | 0.000 |
| `station_id` | 0.000 |
| `station_name` | 0.000 |
| `wind_dir_deg` | 0.014 |
| `wind_dir_text` | 0.014 |
| `wind_gust_ms` | 0.852 |
| `wind_obs_id` | 0.000 |
| `wind_speed_ms` | 0.014 |
| `year` | 0.001 |

## Wind Diagnostics

- Valid wind observations: 1595
- Rows flagged invalid datetime: 1
- Year distribution: {2011: 893, 2021: 6, 2022: 8, 2023: 1, 2024: 4, 2025: 683}
- Source-sheet distribution: {2011: 894, 2025: 683, 2022: 8, 2021: 6, 2024: 4, 2023: 1}
- Years outside 1950..2025: none
- Sheets with suspiciously few rows (<24): {2022: 8, 2021: 6, 2024: 4, 2023: 1}
