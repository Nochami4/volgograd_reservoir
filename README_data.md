# Data Tables

This project builds reproducible derived datasets for abrasion-prone shores of the Volgograd Reservoir.

Current dataset status: `draft`.

`dataset_status: draft`

## Rules For Missing Data

- Raw data in `data/raw/` are never modified.
- Missing raw values are not imputed.
- Derived aggregates may have missing values together with `coverage_*`, `qc_flag`, and `qc_note`.
- Every correction, fallback, or ambiguity must remain transparent in code and output tables.

## Known Limitations

- `base_points` is partially manual: semi-automatic `.doc` extraction only seeds a review template and does not replace manual checking.
- Water-level columns remain ambiguous and therefore stay neutral as `level_col_*`.
- Wind coverage is incomplete for many shoreline intervals, so interval-level wind summaries should be treated as screening variables only.
- Duplicate shoreline observation keys exist and are tracked explicitly in `data/interim/shoreline_duplicate_report.csv`.
- Site project scope is not silently filtered; review it in `data/interim/site_scope_review.csv`.

## Generated Tables

### `data/processed/sites.csv`

- Purpose: reference table for study sites and shoreline characteristics.
- Source: `data/raw/site_metadata/Литология и орентировка берега.xlsx`
- Columns:
- `site_id`: stable normalized identifier used for joins.
- `site_name`: site name as read from the source sheet.
- `shore_type`: shoreline side or type text.
- `shore_orientation_text`: textual shoreline orientation.
- `shore_orientation_deg`: orientation converted to degrees only when explicit.
- `exposure_sectors_text`: listed exposure sectors from the source.
- `lithology_text`: lithology description from the source.
- `lithology_class`: explicit lithology hardness class when present.
- `notes`: extra non-empty cells beyond the main source columns.
- `source_file`: relative path to the source workbook.
- `source_sheet`: workbook sheet name.

### `data/processed/profiles.csv`

- Purpose: profile registry with date range and observation counts.
- Source: `data/raw/profiles/251001 БРОВКИ Баранова.xls`
- Columns:
- `profile_id`: stable profile identifier.
- `site_id`: normalized site id.
- `profile_num`: extracted profile number when present.
- `profile_name`: raw header text such as `ПРОФИЛЬ № 60`.
- `sheet_name_raw`: source sheet name.
- `start_date`: first parsed observation date in ISO format.
- `end_date`: last parsed observation date in ISO format.
- `n_observations`: count of raw rows detected in the profile block.
- `source_file`: relative path to the source workbook.

### `data/processed/shoreline_observations.csv`

- Purpose: main raw-derived shoreline observation layer.
- Source: `data/raw/profiles/251001 БРОВКИ Баранова.xls`
- Columns:
- `obs_id`: stable row identifier.
- `site_id`: normalized site id.
- `profile_id`: normalized profile id.
- `obs_date`: parsed observation date in ISO format.
- `survey_year`: year extracted from `obs_date`.
- `measured_point_name`: raw measurement point or geodetic point name.
- `pn_name`: permanent reference point name.
- `raw_measured_distance_m`: raw measured distance to the brow.
- `gp_to_pn_offset_m`: offset from measurement point to permanent origin.
- `brow_position_pn_m`: brow position reduced to permanent origin.
- `brow_position_raw_m`: raw brow position relative to the measurement point.
- `raw_value_text`: preserved raw text only for fields that failed numeric or date parsing.
- `is_missing`: row-level missingness flag for the numeric shoreline fields.
- `missing_reason`: explicit missingness reason when set.
- `qc_flag`: parser QC flags such as invalid dates, invalid numerics, or row notes.
- `qc_note`: free-text note copied from source-row remarks when present.
- `source_file`: relative path to the source workbook.
- `source_sheet`: source sheet name.
- `source_row`: original row number in the workbook.
- Duplicate handling:
- Potential duplicate keys by `(site_id, profile_id, obs_date)` are not dropped silently.
- They are reported in `data/interim/shoreline_duplicate_report.csv`.
- Conflicting duplicate-key rows receive `DUPLICATE_OBS_KEY`.

### `data/processed/wind_obs_hourly.csv`

- Purpose: meteorological wind observations in a unified long format.
- Source: `data/raw/meteo/Камышин скорость и направление ветра.xlsx`
- Columns:
- `wind_obs_id`: stable observation identifier.
- `station_id`: normalized station id, currently `kamyshin`.
- `station_name`: station name from project context.
- `obs_datetime`: parsed datetime when both date and hour are available.
- `obs_date`: parsed date in ISO format.
- `year`, `month`, `day`, `hour`: decomposed temporal fields.
- `wind_dir_text`: raw wind direction token.
- `wind_dir_deg`: approximate azimuth for known direction tokens.
- `wind_speed_ms`: wind speed in m/s when parsed safely.
- `wind_gust_ms`: gust speed when an adjacent numeric field can be interpreted safely.
- `source_file`: relative path to the source workbook.
- `source_sheet`: workbook sheet name, typically a year.
- `source_row`: original row number in the workbook.
- `is_missing`: missingness flag when wind direction and speed are both absent.
- `missing_reason`: explicit missingness reason when set.
- `qc_flag`: QC flags for invalid dates, missing speed, or missing direction.
- `qc_note`: parsing note, for example when a sheet stores day and month separately from the year.
- Validation rule:
- Dates are accepted only within `1950-01-01 .. 2025-12-31`.
- If a row looks like an observation but the datetime is unreliable, the row is retained with `qc_flag=invalid_datetime` and its derived temporal fields are cleared.

### `data/processed/water_levels_raw.csv`

- Purpose: safe first-pass extraction of water-level blocks with ambiguous source semantics preserved.
- Source: `data/raw/shoreline/Скорость отступания метров в год.xlsx`
- Columns:
- `water_obs_id`: stable observation identifier.
- `site_id`: normalized site id.
- `site_name`: site name normalized from the sheet title.
- `obs_date`: intentionally empty when the source block provides year-only values.
- `year`: year extracted from the water-level block.
- `level_col_1_m`, `level_col_2_m`: neutral numeric columns retained without assigning hidden semantics.
- `source_file`: relative path to the source workbook.
- `source_sheet`: source sheet name.
- `source_row`: original row number in the workbook.
- `is_missing`: row-level missingness flag for retained numeric level columns.
- `missing_reason`: explicit missingness reason when set.
- `qc_flag`: ambiguity flag when multiple numeric level columns are retained.
- `qc_note`: explanation that neutral column names are used because semantics remain unresolved.
- `preferred_level_col`: technically preferred neutral column by completeness only; this is not a semantic interpretation of the variable.
- Notes:
- This is currently a year-only hydrological layer.
- The extraction profile is documented in `data/interim/water_levels_profile.json`.
- A manual interpretation manifest is maintained in `data/interim/water_levels_manual_dictionary.md`.

### `data/processed/base_points.csv`

- Purpose: partially populated base-point table assembled from a preserved manual template plus safe `.doc` string extraction.
- Sources: `data/raw/docs/GPS-координаты пунктов базиса с дополнениями Барановой 2024 г..doc`, `data/raw/docs/Абрисы-2006.doc`
- Columns:
- `base_point_id`: stable row identifier.
- `site_id`: normalized site id when resolved safely.
- `base_point_name`: source point token.
- `obs_date`: explicit full date only; partial dates remain missing.
- `x_m`, `y_m`: copied source coordinates without CRS reinterpretation.
- `accuracy_m`: explicit accuracy only when stated in the source.
- `point_status`: normalized status value.
- `status_note`: free-text justification for the status.
- `is_calculated`, `is_reinstalled`, `is_new`: boolean status flags derived from `point_status`.
- `source_file`, `source_row_ref`
- `qc_flag`, `qc_note`
- Notes:
- OCR-style parsing is intentionally avoided.
- Semi-automatic candidates are staged through `data/interim/base_points_manual_template.csv` and require manual verification.
- Unresolved rows are explicitly marked with `SITE_ID_UNRESOLVED`.

### `data/processed/interval_metrics.csv`

- Purpose: derived retreat metrics for neighboring shoreline observations.
- Source: `data/processed/shoreline_observations.csv`
- Columns:
- `interval_id`: stable interval identifier.
- `site_id`, `profile_id`
- `date_start`, `date_end`: adjacent observation dates.
- `days_between`, `years_between`
- `brow_pos_start_m`, `brow_pos_end_m`
- `retreat_m`: end minus start brow position.
- `retreat_rate_m_per_year`: retreat rate over the interval.
- `retreat_abs_m`: absolute magnitude of the brow-position change, independent of sign.
- `retreat_rate_abs_m_per_year`: absolute annualized magnitude of change.
- `n_raw_points_used`: number of raw rows used across both endpoint dates.
- `calc_method`: transparent method label.
- `qc_flag`: QC marker when date-level aggregation or source QC is present.
- `qc_note`: extra explanation for the interval computation.
- Sign convention:
- `retreat_m = brow_pos_end_m - brow_pos_start_m`
- `retreat_rate_m_per_year = retreat_m / years_between`
- The sign therefore follows the change in normalized brow position relative to `PN`; do not reinterpret the sign physically without checking the field convention for the specific profile set.
- For reporting a physical magnitude without sign interpretation, use `retreat_abs_m` and `retreat_rate_abs_m_per_year`.

### `data/processed/analysis_ready.csv`

- Purpose: first-pass merged analysis table for modeling and exploratory work.
- Sources:
- `data/processed/interval_metrics.csv`
- `data/processed/sites.csv`
- `data/processed/profiles.csv`
- `data/processed/wind_obs_hourly.csv`
- `data/processed/water_levels_raw.csv`
- Columns:
- All interval columns from `interval_metrics.csv`
- Site metadata columns from `sites.csv`
- Profile metadata columns from `profiles.csv`
- `n_wind_obs`, `mean_wind_speed_ms`, `max_wind_speed_ms`, `coverage_wind`
- `n_water_obs`, `mean_level`, `max_level`, `min_level`, `range_level`, `coverage_water`
- `scope_status`: manual review status from `data/interim/site_scope_review.csv`.
- `qc_flag_analysis`: interval-level flags such as `LOW_COVERAGE_WIND`, `LOW_COVERAGE_WATER`, `AMBIGUOUS_WATER_VARIABLE`, `SITE_SCOPE_NEEDS_REVIEW`.
- `qc_note_analysis`: readable explanation of interval-level coverage and ambiguity issues.

## What Is Safe To Analyze Now

- Site-level metadata joins from `sites.csv` and `profiles.csv`.
- Transparent shoreline interval construction from `shoreline_observations.csv` to `interval_metrics.csv`.
- Screening summaries of wind and water coverage at the interval level.
- Absolute shoreline-change magnitude using `retreat_abs_m` and `retreat_rate_abs_m_per_year`.
- Descriptive plots that explicitly respect `qc_flag`, `qc_note`, and coverage columns.

## What Is Not Yet Safe To Interpret

- Any physics-based or causal interpretation that depends strongly on the meaning of `level_col_*`.
- Fine-grained georeferencing that assumes `base_points.csv` is complete.
- Strong conclusions from interval-level wind aggregates where `LOW_COVERAGE_WIND` is present.
- Interpretation of `retreat_m` sign as “erosion” versus “advance” without profile-specific field-convention checking.

## Sign Convention

- `retreat_m = brow_pos_end_m - brow_pos_start_m`
- `retreat_rate_m_per_year = retreat_m / years_between`
- The sign is a positional change in the normalized brow coordinate system, not an automatically interpreted geomorphic “erosion” sign.
- For unsigned magnitude in reports, use `retreat_abs_m` and `retreat_rate_abs_m_per_year`.

## Wind Extension Required

- Tasks 3, 5, and 6 are not yet analysis-safe with the current local wind workbook alone.
- The current interval linkage often yields `LOW_COVERAGE_WIND` because many shoreline intervals are much longer than the locally available meteorological coverage.
- Extending the wind series or adding another transparent local source is required before stronger wind-retreat interpretation.

## Water Semantics Still Ambiguous

- `level_col_1_m` and `level_col_2_m` remain neutral placeholders.
- `preferred_level_col` is a completeness heuristic only.
- Do not relabel these columns semantically until the source workbook is decoded manually.

## Base Points Partially Manual

- `base_points.csv` is a reviewed-output table built from `data/interim/base_points_manual_template.csv`.
- Manual checking is still required for unresolved `site_id`, partial dates, and any point-status interpretation not explicitly written in the source.

## Interim Outputs

### `data/interim/shoreline_observations_log.json`

- Purpose: machine-readable parser log with counts of rows read, missing values, and suspicious records.

### `data/interim/base_points_manual_template.csv`

- Purpose: manual-entry template for stable base-point extraction from `.doc` sources.

### `data/interim/shoreline_duplicate_report.csv`

- Purpose: duplicate-key manifest for shoreline observations grouped by `(site_id, profile_id, obs_date)`.

### `data/interim/site_scope_review.csv`

- Purpose: manual site-scope manifest used downstream without silent filtering.

### `data/interim/water_levels_manual_dictionary.md`

- Purpose: manual decoding checklist for ambiguous water-level columns.

### `data/interim/wind_coverage_by_interval.csv`

- Purpose: interval-level manifest of `n_wind_obs`, `coverage_wind`, and a transparent coverage flag.

### `data/interim/qc_summary.json`

- Purpose: machine-readable QC summary across processed CSV outputs.

## Reports

### `reports/tables/qc_summary.md`

- Purpose: human-readable QC summary with duplicate checks and missingness tables.
