# Таблицы данных

Этот проект собирает воспроизводимый набор производных таблиц по данным наблюдений за береговой бровкой Волгоградского водохранилища. Эти данные используются как прикладной набор для НИР по теме «Применение нейросетей для статистической обработки информации».

Текущий статус набора данных: рабочая воспроизводимая черновая версия. Полная версия набора ещё не считается окончательно завершённой, но текущий производный слой уже пригоден для первого этапа анализа по задачам 1–2.

## Важное правило про язык и структуру

Имена колонок сохранены на английском в `snake_case`, потому что они используются кодом, тестами и соединениями таблиц. Человекочитаемые пояснения, QC-заметки, review-заметки и документация ведутся на русском языке.

## Общие правила

- Файлы в `data/raw/` не редактируются.
- Пропуски в raw-данных не импутируются автоматически.
- Все производные CSV пересобираются скриптами.
- Машинные коды вроде `qc_flag`, `qc_flag_analysis`, `point_status`, `water_time_resolution` и аналогичных полей сохраняются как стабильные технические значения.
- Для интерпретации человеком нужно смотреть на `qc_note`, `qc_note_analysis`, `qc_note_analysis_safe`, `scope_note`, `review_reason` и тематические markdown-отчёты.

## Какие таблицы являются основными

Основные таблицы в `data/processed/`:

- `sites.csv` — справочник береговых участков.
- `profiles.csv` — справочник профилей.
- `shoreline_observations.csv` — основной слой береговых наблюдений.
- `interval_metrics.csv` — интервалы между соседними наблюдениями.
- `analysis_ready.csv` — полный черновой слой после объединения интервалов с контекстом ветра, воды и scope/QC.
- `analysis_safe_subset.csv` — безопасный аналитический слой для задач 1–2.
- `final_dataset_for_modeling.csv` — компактный основной табличный экспорт.

Именно `final_dataset_for_modeling.csv` и `analysis_safe_subset.csv` стоит показывать как главные результаты первого этапа.

Служебные, но рабочие таблицы:

- `wind_obs_hourly.csv`
- `water_levels_raw.csv`
- `base_points_history.csv`
- `base_points_current.csv`
- `base_points.csv`

Экспериментальный дополнительный слой:

- `final_dataset_enriched_open_sources.csv`

Он не заменяет `final_dataset_for_modeling.csv`, а добавляет отдельные колонки обогащения по открытым источникам с прозрачным происхождением данных.

## Краткая карта processed-таблиц

### `data/processed/sites.csv`

Назначение:
справочник участков наблюдений и береговых характеристик.

Ключ:
`site_id`

Основные колонки:
`site_name`, `shore_type`, `shore_orientation_text`, `shore_orientation_deg`, `exposure_sectors_text`, `lithology_text`, `lithology_class`, `notes`

Источник:
`data/raw/site_metadata/Литология и орентировка берега.xlsx`

### `data/processed/profiles.csv`

Назначение:
реестр профилей с диапазоном дат и числом наблюдений.

Ключ:
`profile_id`

Связь:
`site_id`

Важные колонки:
`profile_num`, `profile_name`, `start_date`, `end_date`, `n_observations`

### `data/processed/shoreline_observations.csv`

Назначение:
основной построчный слой береговых наблюдений, собранный из профильной книги.

Ключ:
`obs_id`

Связи:
`site_id`, `profile_id`

Важные колонки:
`obs_date`, `survey_year`, `measured_point_name`, `pn_name`, `raw_measured_distance_m`, `gp_to_pn_offset_m`, `brow_position_pn_m`, `brow_position_raw_m`

QC-колонки:
`is_missing`, `missing_reason`, `qc_flag`, `qc_note`

Пояснение:
`missing_reason` здесь остаётся машинным кодом, а человекочитаемое объяснение смотрите в `qc_note`.

### `data/processed/wind_obs_hourly.csv`

Назначение:
длинный слой локальных метеорологических наблюдений по ветру.

Ключ:
`wind_obs_id`

Технические идентификаторы:
`station_id`, `source_sheet`, `source_row`

Важные колонки:
`obs_datetime`, `obs_date`, `wind_dir_text`, `wind_dir_deg`, `wind_speed_ms`, `wind_gust_ms`

QC-колонки:
`is_missing`, `missing_reason`, `qc_flag`, `qc_note`

Пояснение:
- `missing_reason` сохранён как машинный код, например `wind_direction_and_speed_missing`.
- Человекочитаемое описание проблем разбора и датирования находится в `qc_note`.

### `data/processed/water_levels_raw.csv`

Назначение:
прозрачный годовой слой уровней воды, используемый для интервальных соединений.

Ключ:
`water_obs_id`

Связи:
`site_id`, `site_name`

Контекстные колонки:
`water_section_id`, `water_section_name`, `water_time_resolution` в производных слоях, `water_context_scope` в производных слоях

Важные численные колонки:
`water_level_mean_annual_m_abs`, `water_level_max_annual_m_abs`

QC-колонки:
`is_missing`, `missing_reason`, `qc_flag`, `qc_note`

Пояснение:
слой остаётся годовым и общим для нижнего участка водохранилища; это не локальный датированный гидрологический ряд по каждому береговому участку.

### `data/processed/base_points_history.csv`

Назначение:
полная история базисных точек, восстановленная из legacy `.doc`.

Ключ:
внутренне строки различаются сочетанием `site_id`, `base_point_name_norm`, `obs_date`, `source_row_ref`; отдельного стабильного row-id здесь нет.

Важные колонки:
`site_name_raw`, `site_id`, `base_point_name_raw`, `base_point_name_norm`, `obs_date_raw`, `obs_date`, `y_m`, `x_m`, `accuracy_m`, `note_raw`

Review/QC-колонки:
`point_status`, `has_uncertain_date`, `needs_manual_review`, `review_reason`

Пояснение:
`point_status` остаётся машинным нормализованным кодом; человекочитаемое объяснение спорных случаев хранится в `review_reason`.

### `data/processed/base_points_current.csv`

Назначение:
одна текущая рабочая координатная версия для каждой пары `(site_id, base_point_name_norm)`.

Использование:
это рекомендованный слой для дальнейшей геопривязки.

### `data/processed/base_points.csv`

Назначение:
обратноссовместимый алиас текущего слоя базисных точек.

Ключ:
`base_point_id`

### `data/processed/interval_metrics.csv`

Назначение:
производные интервалы между соседними береговыми наблюдениями одного профиля.

Ключ:
`interval_id`

Связи:
`site_id`, `profile_id`

Основные колонки:
`date_start`, `date_end`, `days_between`, `years_between`, `retreat_m`, `retreat_rate_m_per_year`, `retreat_abs_m`, `retreat_rate_abs_m_per_year`

QC-колонки:
`calc_method`, `qc_flag`, `qc_note`

Пояснение:
`calc_method` оставлен как машинный методический ярлык; интерпретировать его нужно через документацию, а не как человекочитаемый текст в CSV.

### `data/processed/analysis_ready.csv`

Назначение:
полный черновой слой после объединения `interval_metrics.csv` с метаданными участков, профилей, ветром, водой и таблицей проверки состава проекта.

Это полный рабочий слой, где ещё не скрываются ограничения интерпретации.

Важные контекстные колонки:
- `n_wind_obs`, `mean_wind_speed_ms`, `max_wind_speed_ms`, `coverage_wind`
- `n_water_obs`, `coverage_water`
- `mean_water_level_mean_annual_m_abs`, `max_water_level_mean_annual_m_abs`, `min_water_level_mean_annual_m_abs`, `range_water_level_mean_annual_m_abs`
- `mean_water_level_max_annual_m_abs`, `max_water_level_max_annual_m_abs`, `min_water_level_max_annual_m_abs`, `range_water_level_max_annual_m_abs`
- `water_context_scope`, `water_time_resolution`
- `scope_status`, `scope_note`

Главные QC-поля этого слоя:
- `qc_flag_analysis` — машинные коды ограничений интервала.
- `qc_note_analysis` — русское человекочитаемое объяснение этих ограничений.

`qc_note_analysis` нужна специально для того, чтобы строку можно было показывать человеку без расшифровки флагов вручную.

### `data/processed/analysis_safe_subset.csv`

Назначение:
безопасный аналитический слой для текущих задач 1–2.

Источник:
`analysis_ready.csv` плюс контекст из `data/interim/shoreline_duplicate_report.csv`.

Что в нём важно:
- сохраняются только участки, входящие в текущий подтверждённый scope;
- добавляется контекст конфликтующих shoreline-дублей;
- основными QC-полями становятся `qc_flag_analysis_safe` и `qc_note_analysis_safe`.

`qc_flag_analysis_safe` остаётся машинным кодом.

`qc_note_analysis_safe` — это русское консолидированное пояснение по строке, включая ограничения исходного ряда, проблемы покрытия и duplicate-контекст.

### `data/processed/final_dataset_for_modeling.csv`

Назначение:
компактный основной export-набор для анализа и последующей подготовки моделей.

Статус:
это основной итоговый слой, который стоит показывать как главную итоговую таблицу.

Источник:
`analysis_safe_subset.csv`

Что из него убрано:
- полностью пустые колонки;
- дублирующие алиасы;
- часть расширенных технических и provenance-полей, которые всё ещё доступны в более широких слоях.

Что в нём оставлено:
- ключи, даты и интервальные метрики;
- базовый береговой контекст;
- агрегаты воды;
- duplicate-контекст;
- `qc_flag_analysis_safe`
- `qc_note_analysis_safe`

### `data/processed/final_dataset_enriched_open_sources.csv`

Назначение:
отдельный дополнительный набор, который добавляет к `final_dataset_for_modeling.csv` прозрачные признаки обогащения по открытым источникам.

Статус:
это не главный датасет для показа преподавателю и не замена основному итоговому набору.

Его лучше показывать как дополнительный экспериментальный слой.

Почему он остаётся в `data/processed/`:
- на него есть код и тесты;
- он документирован в проекте;
- он может быть полезен как демонстрация аккуратного обогащения с явным provenance;
- он не ломает основной итоговый слой, потому что существует отдельно.

Какие колонки в нём особенно важно трактовать как технические коды:
- `wind_fill_status`
- `wind_fill_source`
- `wind_fill_source_tier`
- `wind_fill_method`
- `wind_fill_confidence`
- `water_fill_status`
- `water_fill_source`
- `water_fill_source_tier`
- `water_fill_method`
- `water_fill_confidence`

Человекочитаемые пояснения в этом дополнительном слое находятся прежде всего в:
- `qc_note_analysis_safe`
- `wind_fill_validation_note`
- `water_fill_validation_note`

## Какие таблицы не стоит показывать как главные

Не стоит показывать преподавателю как главные итоговые таблицы:

- `wind_obs_hourly.csv` — это технический исходно-производный слой наблюдений погоды.
- `water_levels_raw.csv` — это join-слой годового гидрологического контекста.
- `base_points_history.csv` — это таблица с подробным provenance-слоем и случаями ручной проверки.
- `final_dataset_enriched_open_sources.csv` — это дополнительный слой для экспериментального enrichment, а не основной итоговый датасет.

## Что безопасно анализировать сейчас

- Описательную структуру участков и профилей.
- Интервальные метрики смещения бровки.
- Беззнаковую интенсивность изменения (`retreat_abs_m`, `retreat_rate_abs_m_per_year`).
- Внутриучастковые связи профилей на полностью сопоставимых интервалах.
- Годовой водный контекст только как ограниченный сопровождающий фон, а не как локальный причинный ряд.

Это соответствует текущему первому аналитическому этапу: временная структура наблюдений, интервальные изменения и корреляционная согласованность профилей внутри участков.

## Что пока нельзя интерпретировать жёстко

- Ветер как устойчивый причинный фактор там, где доминирует `LOW_COVERAGE_WIND`.
- Воду как локальный датированный forcing-фактор: текущий слой остаётся year-only контекстом по нижнему участку.
- Любую автоматическую трактовку знака `retreat_m` как готовой физической категории без проверки полевой конвенции.
- Геодезически чувствительные выводы без учёта `review_reason` и ручной проверки базисных точек.

## Interim и reports

Ключевые промежуточные файлы:

- `data/interim/shoreline_duplicate_report.csv`
- `data/interim/site_scope_review.csv`
- `data/interim/wind_coverage_by_interval.csv`
- `data/interim/water_levels_profile.json`
- `data/interim/water_levels_manual_dictionary.md`
- `data/interim/qc_summary.json`

Ключевые отчёты:

- `reports/tables/qc_summary.md`
- `reports/tables/data_codebook_ru.md`
- `reports/tables/processed_table_inventory.md`
- `reports/tables/russian_text_audit.md`

Для первого аналитического этапа также важны:

- `reports/tables/01_periods_summary.csv`
- `reports/tables/02_profile_correlation_summary.csv`
- `reports/tables/02_profile_correlation_presentation.csv`
- `reports/figures/01_site_interval_timelines_presentation.png`
- `reports/figures/01_retreat_distributions_composite.png`
- `reports/figures/02_profile_correlation_overview.png`

Для нейросетевого демонстрационного поиска подозрительных наблюдений также важны:

- `reports/tables/autoencoder_anomaly_scores.csv`
- `reports/tables/autoencoder_top_anomalies.csv`
- `reports/tables/autoencoder_anomaly_summary.md`
- `reports/figures/04_autoencoder_reconstruction_error.png`
- `reports/figures/04_autoencoder_top_anomalies.png`
