# Сводка QC

## Критические ограничения перед научной интерпретацией

- `data/processed/analysis_ready.csv`: Для многих береговых интервалов покрытие ветром остаётся ниже порога 0.8.
- `data/processed/shoreline_observations.csv`: Часть ключей береговых наблюдений остаётся конфликтующей и требует ручной проверки.

## Почему задачи 3/5/6 пока не считаются аналитически безопасными

- Интервалы с `coverage_wind >= 0.8`: 0
- Интервалы с `coverage_wind > 0`: 101
- Годы ветровых наблюдений, которые сейчас есть в `wind_obs_hourly.csv`: [2011, 2021, 2022, 2023, 2024, 2025]
- Семейства задач, опирающиеся на интервальный ветер как на фактор, пока остаются черновыми, пока временной ряд не будет расширен или лучше сопоставлен с интервалами.

## Диагностика по наборам данных

- `base_points.csv`: 16 строк; неразрешённых `site_id`: 0; распределение `point_status`: {'original': 15, 'refined': 1}.
- `water_levels_raw.csv`: доля пропусков в восстановленных средних/максимальных годовых уровнях нижнего участка = 0.000/0.000; строк без полной даты = 1.000.
- Уровни воды теперь явно расшифрованы как средние и максимальные годовые значения по нижнему участку, но всё ещё остаются годовым контекстом без полной даты наблюдения.
- Участки без каких-либо извлечённых строк воды: нет.
- `shoreline_duplicate_report.csv`: 3 групп дублирующихся ключей, включая 3 конфликтующих групп(ы).
- `site_scope_review.csv`: 0 участок(ов) всё ещё помечен(о) как `needs_review`.

| Файл | Строк | Колонок | Дубли строк | Дубли ключей | Ключевые колонки |
| --- | ---: | ---: | ---: | ---: | --- |
| `data/processed/analysis_ready.csv` | 587 | 60 | 0 | 0 | interval_id |
| `data/processed/analysis_safe_subset.csv` | 575 | 53 | 0 | н/д | н/д |
| `data/processed/base_points.csv` | 16 | 21 | 0 | 0 | base_point_id |
| `data/processed/base_points_current.csv` | 16 | 20 | 0 | н/д | н/д |
| `data/processed/base_points_history.csv` | 21 | 20 | 0 | н/д | н/д |
| `data/processed/final_dataset_enriched_open_sources.csv` | 575 | 54 | 0 | н/д | н/д |
| `data/processed/final_dataset_for_modeling.csv` | 575 | 38 | 0 | 0 | interval_id |
| `data/processed/interval_metrics.csv` | 587 | 17 | 0 | 0 | interval_id |
| `data/processed/profiles.csv` | 33 | 9 | 0 | 0 | profile_id |
| `data/processed/shoreline_observations.csv` | 716 | 19 | 0 | 0 | obs_id |
| `data/processed/sites.csv` | 10 | 11 | 0 | 0 | site_id |
| `data/processed/water_levels_raw.csv` | 330 | 16 | 0 | 0 | water_obs_id |
| `data/processed/wind_obs_hourly.csv` | 1596 | 20 | 0 | 0 | wind_obs_id |

## Дополнительные адресные проверки

### `data/processed/analysis_ready.csv`

- `analysis_low_coverage_wind_share`: н/д. Доля интервалов с флагом `LOW_COVERAGE_WIND`: 1.000.
- `analysis_low_coverage_water_share`: н/д. Доля интервалов с флагом `LOW_COVERAGE_WATER`: 0.283.
- `analysis_intervals_with_any_wind`: 101. Интервалы, где `coverage_wind > 0`.
- `analysis_intervals_with_adequate_wind`: 0. Интервалы, где `coverage_wind >= 0.8`.

### `data/processed/base_points.csv`

- `base_points_empty`: 0. Пустой слой `base_points` означает, что береговая геометрия остаётся привязанной лишь частично.
- `base_points_missing_site_id_share`: н/д. Доля строк базисных точек без определённого `site_id`: 0.000.
- `base_points_point_status_distribution`: н/д. Распределение `point_status`: {'original': 15, 'refined': 1}.

### `data/processed/final_dataset_for_modeling.csv`

- `final_dataset_all_empty_columns`: 0. Полностью пустые колонки: нет.

### `data/processed/shoreline_observations.csv`

- `shoreline_duplicate_groups`: 3. Группы дублирующихся ключей наблюдений описаны в `data/interim/shoreline_duplicate_report.csv`.

### `data/processed/water_levels_raw.csv`

- `water_mean_level_missing_share`: н/д. Доля строк без восстановленного среднего годового уровня воды для нижнего участка: 0.000.
- `water_max_level_missing_share`: н/д. Доля строк без восстановленного максимального годового уровня воды для нижнего участка: 0.000.
- `water_rows_without_full_date_share`: н/д. Доля строк воды без полной `obs_date`: 1.000.

### `data/processed/wind_obs_hourly.csv`

- `wind_year_out_of_range`: 0. Допустимый диапазон: 1950..2025.
- `missing_obs_datetime_when_date_and_hour_present`: 0. Строки с распознанными годом и часом должны иметь `obs_datetime`, если только они не были помечены как недопустимые выше по пайплайну.

## Пропуски по колонкам

### `data/processed/analysis_ready.csv`

| Колонка | Доля пропусков |
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
| `in_project_scope` | 0.000 |
| `interval_id` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `max_level` | 0.210 |
| `max_water_level_max_annual_m_abs` | 0.210 |
| `max_water_level_mean_annual_m_abs` | 0.210 |
| `max_wind_speed_ms` | 0.896 |
| `mean_level` | 0.210 |
| `mean_water_level_max_annual_m_abs` | 0.210 |
| `mean_water_level_mean_annual_m_abs` | 0.210 |
| `mean_wind_speed_ms` | 0.896 |
| `min_level` | 0.210 |
| `min_water_level_max_annual_m_abs` | 0.210 |
| `min_water_level_mean_annual_m_abs` | 0.210 |
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
| `range_level` | 0.210 |
| `range_water_level_max_annual_m_abs` | 0.210 |
| `range_water_level_mean_annual_m_abs` | 0.210 |
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
| `water_context_scope` | 0.210 |
| `water_time_resolution` | 0.210 |
| `water_variable_is_ambiguous` | 0.000 |
| `years_between` | 0.000 |

### `data/processed/analysis_safe_subset.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `calc_method` | 0.000 |
| `conflicting_duplicate_group_count` | 0.000 |
| `coverage_water` | 0.000 |
| `date_end` | 0.000 |
| `date_start` | 0.000 |
| `days_between` | 0.000 |
| `duplicate_conflict_note` | 0.896 |
| `duplicate_conflict_obs_dates` | 0.896 |
| `end_date` | 0.000 |
| `exposure_sectors_text` | 0.000 |
| `has_conflicting_shoreline_duplicates` | 0.000 |
| `history_start_group` | 0.000 |
| `history_start_year` | 0.000 |
| `in_project_scope` | 0.000 |
| `interval_id` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `max_water_level_max_annual_m_abs` | 0.214 |
| `max_water_level_mean_annual_m_abs` | 0.214 |
| `mean_water_level_max_annual_m_abs` | 0.214 |
| `mean_water_level_mean_annual_m_abs` | 0.214 |
| `min_water_level_max_annual_m_abs` | 0.214 |
| `min_water_level_mean_annual_m_abs` | 0.214 |
| `n_observations` | 0.000 |
| `n_raw_points_used` | 0.000 |
| `n_water_obs` | 0.000 |
| `notes` | 1.000 |
| `profile_id` | 0.000 |
| `profile_name` | 0.000 |
| `profile_num` | 0.000 |
| `qc_flag` | 0.807 |
| `qc_flag_analysis` | 0.000 |
| `qc_flag_analysis_safe` | 0.000 |
| `qc_note` | 1.000 |
| `qc_note_analysis` | 0.000 |
| `qc_note_analysis_safe` | 0.000 |
| `range_water_level_max_annual_m_abs` | 0.214 |
| `range_water_level_mean_annual_m_abs` | 0.214 |
| `retreat_abs_m` | 0.000 |
| `retreat_m` | 0.000 |
| `retreat_rate_abs_m_per_year` | 0.000 |
| `retreat_rate_m_per_year` | 0.000 |
| `scope_note` | 0.000 |
| `scope_status` | 0.000 |
| `shore_orientation_deg` | 0.000 |
| `shore_orientation_text` | 0.000 |
| `shore_type` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `start_date` | 0.000 |
| `water_context_scope` | 0.214 |
| `water_time_resolution` | 0.214 |
| `years_between` | 0.000 |

### `data/processed/base_points.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `accuracy_m` | 0.938 |
| `base_point_id` | 0.000 |
| `base_point_name_norm` | 0.000 |
| `base_point_name_raw` | 0.000 |
| `has_uncertain_date` | 0.000 |
| `is_calculated` | 0.000 |
| `is_new` | 0.000 |
| `is_refined` | 0.000 |
| `is_reinstalled` | 0.000 |
| `needs_manual_review` | 0.000 |
| `note_raw` | 0.812 |
| `obs_date` | 0.125 |
| `obs_date_raw` | 0.062 |
| `point_status` | 0.000 |
| `review_reason` | 0.812 |
| `site_id` | 0.000 |
| `site_name_raw` | 0.000 |
| `source_file` | 0.000 |
| `source_row_ref` | 0.000 |
| `x_m` | 0.000 |
| `y_m` | 0.000 |

### `data/processed/base_points_current.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `accuracy_m` | 0.938 |
| `base_point_name_norm` | 0.000 |
| `base_point_name_raw` | 0.000 |
| `has_uncertain_date` | 0.000 |
| `is_calculated` | 0.000 |
| `is_new` | 0.000 |
| `is_refined` | 0.000 |
| `is_reinstalled` | 0.000 |
| `needs_manual_review` | 0.000 |
| `note_raw` | 0.812 |
| `obs_date` | 0.125 |
| `obs_date_raw` | 0.062 |
| `point_status` | 0.000 |
| `review_reason` | 0.812 |
| `site_id` | 0.000 |
| `site_name_raw` | 0.000 |
| `source_file` | 0.000 |
| `source_row_ref` | 0.000 |
| `x_m` | 0.000 |
| `y_m` | 0.000 |

### `data/processed/base_points_history.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `accuracy_m` | 0.857 |
| `base_point_name_norm` | 0.000 |
| `base_point_name_raw` | 0.000 |
| `has_uncertain_date` | 0.000 |
| `is_calculated` | 0.000 |
| `is_new` | 0.000 |
| `is_refined` | 0.000 |
| `is_reinstalled` | 0.000 |
| `needs_manual_review` | 0.000 |
| `note_raw` | 0.762 |
| `obs_date` | 0.190 |
| `obs_date_raw` | 0.048 |
| `point_status` | 0.000 |
| `review_reason` | 0.762 |
| `site_id` | 0.000 |
| `site_name_raw` | 0.000 |
| `source_file` | 0.000 |
| `source_row_ref` | 0.000 |
| `x_m` | 0.000 |
| `y_m` | 0.000 |

### `data/processed/final_dataset_enriched_open_sources.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `conflicting_duplicate_group_count` | 0.000 |
| `coverage_water` | 0.000 |
| `coverage_wind` | 0.000 |
| `date_end` | 0.000 |
| `date_start` | 0.000 |
| `days_between` | 0.000 |
| `exposure_sectors_text` | 0.000 |
| `has_conflicting_shoreline_duplicates` | 0.000 |
| `history_start_group` | 0.000 |
| `history_start_year` | 0.000 |
| `interval_id` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `max_water_level_max_annual_m_abs` | 0.214 |
| `max_water_level_mean_annual_m_abs` | 0.214 |
| `max_wind_speed_ms` | 0.903 |
| `mean_water_level_max_annual_m_abs` | 0.214 |
| `mean_water_level_mean_annual_m_abs` | 0.214 |
| `mean_wind_speed_ms` | 0.903 |
| `min_water_level_max_annual_m_abs` | 0.214 |
| `min_water_level_mean_annual_m_abs` | 0.214 |
| `n_water_obs` | 0.000 |
| `n_wind_obs` | 0.000 |
| `profile_id` | 0.000 |
| `profile_name` | 0.000 |
| `profile_num` | 0.000 |
| `qc_flag_analysis_safe` | 0.000 |
| `qc_note_analysis_safe` | 0.000 |
| `range_water_level_max_annual_m_abs` | 0.214 |
| `range_water_level_mean_annual_m_abs` | 0.214 |
| `retreat_abs_m` | 0.000 |
| `retreat_m` | 0.000 |
| `retreat_rate_abs_m_per_year` | 0.000 |
| `retreat_rate_m_per_year` | 0.000 |
| `shore_orientation_deg` | 0.000 |
| `shore_orientation_text` | 0.000 |
| `shore_type` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `water_context_scope` | 0.214 |
| `water_fill_confidence` | 0.000 |
| `water_fill_method` | 0.000 |
| `water_fill_source` | 0.000 |
| `water_fill_source_tier` | 0.000 |
| `water_fill_status` | 0.000 |
| `water_fill_validation_note` | 0.000 |
| `water_time_resolution` | 0.214 |
| `wind_fill_confidence` | 0.000 |
| `wind_fill_method` | 0.000 |
| `wind_fill_source` | 0.000 |
| `wind_fill_source_tier` | 0.000 |
| `wind_fill_status` | 0.000 |
| `wind_fill_validation_note` | 0.000 |
| `years_between` | 0.000 |

### `data/processed/final_dataset_for_modeling.csv`

| Колонка | Доля пропусков |
| --- | ---: |
| `conflicting_duplicate_group_count` | 0.000 |
| `coverage_water` | 0.000 |
| `date_end` | 0.000 |
| `date_start` | 0.000 |
| `days_between` | 0.000 |
| `exposure_sectors_text` | 0.000 |
| `has_conflicting_shoreline_duplicates` | 0.000 |
| `history_start_group` | 0.000 |
| `history_start_year` | 0.000 |
| `interval_id` | 0.000 |
| `lithology_class` | 0.000 |
| `lithology_text` | 0.000 |
| `max_water_level_max_annual_m_abs` | 0.214 |
| `max_water_level_mean_annual_m_abs` | 0.214 |
| `mean_water_level_max_annual_m_abs` | 0.214 |
| `mean_water_level_mean_annual_m_abs` | 0.214 |
| `min_water_level_max_annual_m_abs` | 0.214 |
| `min_water_level_mean_annual_m_abs` | 0.214 |
| `n_water_obs` | 0.000 |
| `profile_id` | 0.000 |
| `profile_name` | 0.000 |
| `profile_num` | 0.000 |
| `qc_flag_analysis_safe` | 0.000 |
| `qc_note_analysis_safe` | 0.000 |
| `range_water_level_max_annual_m_abs` | 0.214 |
| `range_water_level_mean_annual_m_abs` | 0.214 |
| `retreat_abs_m` | 0.000 |
| `retreat_m` | 0.000 |
| `retreat_rate_abs_m_per_year` | 0.000 |
| `retreat_rate_m_per_year` | 0.000 |
| `shore_orientation_deg` | 0.000 |
| `shore_orientation_text` | 0.000 |
| `shore_type` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `water_context_scope` | 0.214 |
| `water_time_resolution` | 0.214 |
| `years_between` | 0.000 |

### `data/processed/interval_metrics.csv`

| Колонка | Доля пропусков |
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

| Колонка | Доля пропусков |
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

| Колонка | Доля пропусков |
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

| Колонка | Доля пропусков |
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

| Колонка | Доля пропусков |
| --- | ---: |
| `is_missing` | 0.000 |
| `missing_reason` | 1.000 |
| `obs_date` | 1.000 |
| `qc_flag` | 0.000 |
| `qc_note` | 0.000 |
| `site_id` | 0.000 |
| `site_name` | 0.000 |
| `source_file` | 0.000 |
| `source_row` | 0.000 |
| `source_sheet` | 0.000 |
| `water_level_max_annual_m_abs` | 0.000 |
| `water_level_mean_annual_m_abs` | 0.000 |
| `water_obs_id` | 0.000 |
| `water_section_id` | 0.000 |
| `water_section_name` | 0.000 |
| `year` | 0.000 |

### `data/processed/wind_obs_hourly.csv`

| Колонка | Доля пропусков |
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

## Диагностика ветрового слоя

- Корректных ветровых наблюдений с `obs_datetime`: 1595
- Строк с флагом `invalid_datetime`: 1
- Распределение по годам: {2011: 893, 2021: 6, 2022: 8, 2023: 1, 2024: 4, 2025: 683}
- Распределение по исходным листам: {2011: 894, 2025: 683, 2022: 8, 2021: 6, 2024: 4, 2023: 1}
- Годы вне диапазона 1950..2025: нет
- Листы с подозрительно малым числом строк (<24): {2022: 8, 2021: 6, 2024: 4, 2023: 1}
