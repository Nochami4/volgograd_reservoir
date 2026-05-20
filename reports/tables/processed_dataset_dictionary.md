# Словарь processed-датасета

Этот файл помогает быстро понять назначение основных таблиц в `data/processed/` без переименования технических колонок. Текущий производный слой является рабочей воспроизводимой черновой версией: он уже пригоден для первого этапа анализа, но не делает вид, что все внешние контексты и спорные случаи полностью исчерпаны.

Подробный построчный словарь основных колонок находится в `processed_dataset_dictionary.csv`.

| Таблица | Назначение | Ключевые колонки | Статус и замечания |
| --- | --- | --- | --- |
| `sites.csv` | Справочник береговых участков | `site_id`, `site_name`, `shore_type`, `shore_orientation_text`, `lithology_text` | Базовый описательный слой по участкам |
| `profiles.csv` | Реестр профилей внутри участков | `profile_id`, `site_id`, `profile_num`, `profile_name`, `start_date`, `end_date` | Нужен для временной структуры наблюдений |
| `shoreline_observations.csv` | Основной построчный слой береговых наблюдений | `obs_id`, `site_id`, `profile_id`, `obs_date`, `brow_position_pn_m`, `qc_flag`, `qc_note` | Содержит исходный ряд наблюдений и QC-контекст |
| `interval_metrics.csv` | Интервалы между соседними наблюдениями одного профиля | `interval_id`, `date_start`, `date_end`, `retreat_m`, `retreat_rate_m_per_year`, `retreat_abs_m` | Основа первого аналитического этапа |
| `analysis_ready.csv` | Полный draft-слой после объединения интервалов с ветром, водой и scope/QC-контекстом | `interval_id`, `coverage_wind`, `coverage_water`, `water_time_resolution`, `scope_status`, `qc_note_analysis` | Полный рабочий слой; ограничения здесь ещё не скрываются |
| `analysis_safe_subset.csv` | Безопасный аналитический поднабор для задач 1–2 | `interval_id`, `retreat_rate_m_per_year`, `retreat_rate_abs_m_per_year`, `history_start_group`, `has_conflicting_shoreline_duplicates`, `qc_note_analysis_safe` | Главная база первого этапа анализа |
| `final_dataset_for_modeling.csv` | Компактный итоговый processed-датасет для показа и следующих шагов анализа | `interval_id`, `site_name`, `profile_name`, `date_start`, `date_end`, `retreat_rate_abs_m_per_year`, `qc_note_analysis_safe` | Основной показываемый табличный результат |
| `wind_obs_hourly.csv` | Технический слой наблюдений ветра | `wind_obs_id`, `obs_datetime`, `wind_dir_deg`, `wind_speed_ms`, `qc_flag` | Нужен для воспроизводимости, но пока не считается финальным объясняющим блоком |
| `water_levels_raw.csv` | Годовой гидрологический join-слой | `water_obs_id`, `year`, `water_level_mean_annual_m_abs`, `water_level_max_annual_m_abs`, `qc_note` | Даёт ограниченный годовой контекст, а не локальный датированный ряд |
| `base_points_history.csv` | Полная история базисных точек с provenance | `site_id`, `base_point_name_norm`, `obs_date`, `x_m`, `y_m`, `review_reason` | Содержит спорные случаи и ручную проверку |
| `base_points_current.csv` | Текущая рабочая версия базисных точек | `site_id`, `base_point_name_norm`, `obs_date`, `x_m`, `y_m`, `point_status` | Рекомендуемый слой для текущей геопривязки |
| `base_points.csv` | Обратноссовместимый алиас текущего слоя базисных точек | `base_point_id`, `site_id`, `base_point_name_norm`, `point_status` | Сохраняется для совместимости кода и тестов |
| `final_dataset_enriched_open_sources.csv` | Дополнительный слой с прозрачным обогащением по открытым источникам | `interval_id`, `n_wind_obs`, `wind_fill_status`, `wind_fill_method`, `water_fill_status`, `qc_note_analysis_safe` | Дополнительный экспериментальный слой, не основной датасет показа |
