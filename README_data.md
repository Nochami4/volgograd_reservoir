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

- `base_points` are now rebuilt into transparent `history/current` layers, but some rows still require manual review because the legacy `.doc` loses table structure in string extraction.
- Water levels are now resolved as annual mean/max lower-section context, but they remain year-only shared hydrological context rather than site-local fully dated forcing.
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

- Purpose: transparent annual water-level layer for interval joins, built from the dedicated reservoir-level workbook.
- Source: `data/raw/Уровни ВДХР 1986-2018.xlsx`
- Columns:
- `water_obs_id`: stable observation identifier.
- `site_id`, `site_name`: shoreline site key used for backward-compatible joins in `analysis_ready.csv`.
- `water_section_id`, `water_section_name`: resolved hydrological context; the current series uses the lower reservoir section (`Нижний участок`, озерный участок).
- `obs_date`: intentionally empty when the source block provides year-only values.
- `year`: year extracted from the annual section-level workbook.
- `water_level_mean_annual_m_abs`: annual mean water level for the lower section, meters absolute.
- `water_level_max_annual_m_abs`: annual maximum water level for the lower section, meters absolute.
- `source_file`: relative path to the source workbook.
- `source_sheet`: source sheet name.
- `source_row`: original row number in the workbook.
- `is_missing`: row-level missingness flag for the resolved annual mean/max pair.
- `missing_reason`: explicit missingness reason when set.
- `qc_flag`: year-only and shared-section-context flags, plus missing-value flags when present.
- `qc_note`: explanation that the series is annual lower-section context repeated across sites for join compatibility.
- Notes:
- The source workbook contains annual mean and annual maximum levels by reservoir section.
- For the current shoreline analysis, the relevant section is the lower section; this is the same mean-level basis that was used in the legacy Excel plots for `Скорость отступания метров в год`.
- Full observation dates are still not available; the layer remains year-only.
- The same lower-section annual series is repeated across shoreline sites only to preserve pipeline-compatible joins. It should be treated as shared hydrological context, not as a site-local water record.
- The extraction profile is documented in `data/interim/water_levels_profile.json`.
- The resolved field dictionary is documented in `data/interim/water_levels_manual_dictionary.md`.

### `data/processed/base_points_history.csv`

- Purpose: full recoverable history of base-point coordinates extracted from `data/raw/docs/GPS-координаты пунктов базиса с дополнениями Барановой 2024 г..doc`.
- Each row preserves the best available source token, date token, coordinate pair, status flags, and review state.
- Keep this table when you need provenance, partial dates, or all historical coordinate versions for one point.
- Key columns:
- `site_name_raw`, `site_id`
- `base_point_name_raw`, `base_point_name_norm`
- `obs_date_raw`, `obs_date`
- `y_m`, `x_m`, `accuracy_m`
- `note_raw`, `point_status`
- `is_calculated`, `is_reinstalled`, `is_new`, `is_refined`
- `has_uncertain_date`, `needs_manual_review`, `review_reason`
- `source_file`, `source_row_ref`

### `data/processed/base_points_current.csv`

- Purpose: one current coordinate choice per `(site_id, base_point_name_norm)` selected from `base_points_history.csv`.
- Current selection rule:
- Prefer rows with an explicit full `obs_date`.
- Then prefer the latest available `obs_date`.
- Break ties in favor of rows without uncertain dates, without calculated-only status, and without manual-review flags.
- This table is the recommended join target for downstream georeferencing work.

### `data/processed/base_points.csv`

- Purpose: backward-compatible alias of the current base-point layer for existing pipeline/QC code.
- It contains the same current-coordinate rows plus a synthetic `base_point_id`.

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
- `n_water_obs`, `coverage_water`
- Resolved water aggregates:
- `mean_water_level_mean_annual_m_abs`, `max_water_level_mean_annual_m_abs`, `min_water_level_mean_annual_m_abs`, `range_water_level_mean_annual_m_abs`
- `mean_water_level_max_annual_m_abs`, `max_water_level_max_annual_m_abs`, `min_water_level_max_annual_m_abs`, `range_water_level_max_annual_m_abs`
- `water_context_scope`, `water_time_resolution`
- Backward-compatible aliases:
- `mean_level`, `max_level`, `min_level`, `range_level`
- These aliases now point to the interval aggregation of `water_level_mean_annual_m_abs`, because the legacy shoreline Excel plots were based on annual mean lower-section water level.
- `scope_status`: manual review status from `data/interim/site_scope_review.csv`.
- `qc_flag_analysis`: interval-level flags such as `LOW_COVERAGE_WIND`, `LOW_COVERAGE_WATER`, `YEAR_ONLY_WATER_SOURCE`, `SITE_SCOPE_NEEDS_REVIEW`.
- `qc_note_analysis`: readable explanation of interval-level coverage and remaining timing/interpretation limits.

### `data/processed/analysis_safe_subset.csv`

- Purpose: scoped interval layer for the current analytical stage, limited to confirmed project sites and enriched with duplicate-context QC.
- Source: `data/processed/analysis_ready.csv` plus `data/interim/shoreline_duplicate_report.csv`
- Notes:
- This is the recommended base layer for current descriptive analysis and for building a cleaner modeling export.
- `qc_flag_analysis_safe` and `qc_note_analysis_safe` are the main consolidated QC/context fields at this stage.

### `data/processed/final_dataset_for_modeling.csv`

- Purpose: compact final export for analysis and model preparation, built reproducibly from `data/processed/analysis_safe_subset.csv`.
- Role: this is the master dataset and should remain the untouched baseline export, even where honest gaps remain.
- Design:
- Fully empty columns are removed.
- Exact duplicate alias columns are removed.
- Wide technical/provenance columns that remain available in broader processed layers are excluded here.
- Kept groups:
- Interval identifiers and dates
- Signed and absolute retreat metrics
- Core shoreline/site metadata
- Resolved water-context aggregates and their timing/context flags
- Duplicate-context indicators and consolidated QC fields
- Use this file when you need the cleanest project-ready tabular export without losing the main analytical caveats.

### `data/processed/final_dataset_enriched_open_sources.csv`

- Purpose: separate companion export that keeps every master row and column, then adds transparent enrichment columns where open-source or already curated same-project context can be attached without inventing values.
- Relationship to master:
- `final_dataset_for_modeling.csv` stays the untouched master baseline.
- `final_dataset_enriched_open_sources.csv` does not replace the master file; it supplements it with explicit provenance-aware enrichment.
- Current scope:
- Wind is added as a separate enrichment block with per-row source, method, confidence, and status fields.
- Water values from master are retained as-is; no silent replacement of the existing year-only lower-section context is performed.
- Remaining gaps stay missing when no honest compatible source is available.
- Companion manifest: `data/processed/final_dataset_enriched_open_sources_manifest.md`

## What Is Safe To Analyze Now

- Site-level metadata joins from `sites.csv` and `profiles.csv`.
- Transparent shoreline interval construction from `shoreline_observations.csv` to `interval_metrics.csv`.
- Screening summaries of wind and water coverage at the interval level.
- Descriptive use of annual lower-section water context where it is important to distinguish annual mean and annual maximum level explicitly.
- Absolute shoreline-change magnitude using `retreat_abs_m` and `retreat_rate_abs_m_per_year`.
- Descriptive plots that explicitly respect `qc_flag`, `qc_note`, and coverage columns.

## What Is Not Yet Safe To Interpret

- Strong local or causal interpretation of water effects from the current water layer, because it is still a year-only lower-section context series rather than a fully dated site-local forcing record.
- Fine-grained georeferencing that assumes `base_points_current.csv` is complete and fully reviewed.
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

## Water Semantics And Remaining Limits

- Water semantics are now decoded explicitly as annual mean and annual maximum levels for the lower reservoir section in meters absolute.
- Full observation dates are still unavailable, so the water layer remains year-only.
- The same lower-section series is reused across shoreline sites as shared hydrological context for joins; it should not be over-interpreted as a site-local causal measurement.

## Base Points Manual Review

- `base_points_history.csv` retains rows with partial dates, inferred site linkage, and conflict markers instead of dropping them.
- `base_points_current.csv` picks one operational coordinate set per point, but this does not erase unresolved source ambiguity.
- A row is treated as `manual review` when at least one of the following is true:
- `site_id` cannot be resolved safely from project point names or an explicit project mapping.
- `obs_date_raw` is partial or missing, so `obs_date` cannot be normalized to a full ISO date.
- The extracted point label is truncated or otherwise suspicious.
- The coordinate assignment conflicts with another known point cluster.
- Explicit special case: `Ураков Бугор`, point `3`, `2019-08-14`, because its coordinates are suspiciously aligned with the `Нижний Ураков` point-3 cluster.

## Interim Outputs

### `data/interim/shoreline_observations_log.json`

- Purpose: machine-readable parser log with counts of rows read, missing values, and suspicious records.

### `data/interim/shoreline_duplicate_report.csv`

- Purpose: duplicate-key manifest for shoreline observations grouped by `(site_id, profile_id, obs_date)`.

### `data/interim/site_scope_review.csv`

- Purpose: reviewed site-scope manifest used downstream without silent filtering.
- Current scope marks the 9 map/source-document sites as `in_project_scope=true`.
- `suvodskaya` remains in metadata but is explicitly marked outside the current project scope.

### `data/interim/water_levels_manual_dictionary.md`

- Purpose: compact field dictionary for the resolved lower-section annual water layer and its join logic.

### `data/interim/wind_coverage_by_interval.csv`

- Purpose: interval-level manifest of `n_wind_obs`, `coverage_wind`, and a transparent coverage flag.

### `data/interim/qc_summary.json`

- Purpose: machine-readable QC summary across processed CSV outputs.

## Reports

### `reports/tables/qc_summary.md`

- Purpose: human-readable QC summary with duplicate checks and missingness tables.

### `reports/tables/base_points_manual_review.csv`

- Purpose: filtered manifest of all base-point rows that still require manual review.
