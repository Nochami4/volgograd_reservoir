# Русский кодбук по данным

## Зачем нужен этот файл

Этот кодбук объясняет по-русски смысл таблиц и ключевых колонок в `data/processed/`, не меняя их технические имена. Имена колонок сохранены на английском в `snake_case`, потому что они используются кодом, тестами и соединениями таблиц.

## Какие таблицы являются основными

- `sites.csv` — справочник участков.
- `profiles.csv` — справочник профилей.
- `shoreline_observations.csv` — основной слой береговых наблюдений.
- `interval_metrics.csv` — интервалы между соседними наблюдениями профиля.
- `analysis_ready.csv` — полный draft-слой после ETL и первичного объединения.
- `analysis_safe_subset.csv` — безопасный слой для задач 1–2.
- `final_dataset_for_modeling.csv` — компактный основной итоговый export-набор.

## Какие таблицы являются служебными или специальными

- `wind_obs_hourly.csv` — технический слой наблюдений ветра.
- `water_levels_raw.csv` — годовой гидрологический join-слой.
- `base_points_history.csv` — provenance-heavy история базисных точек.
- `base_points_current.csv` — текущий рабочий слой базисных точек.
- `base_points.csv` — обратноссовместимый alias текущего слоя.
- `final_dataset_enriched_open_sources.csv` — отдельный дополнительный слой с обогащением по открытым источникам.

## Главные ключи

- `site_id` — стабильный код участка.
- `profile_id` — стабильный код профиля.
- `interval_id` — стабильный код интервала между соседними наблюдениями.
- `obs_id` — стабильный код берегового наблюдения.
- `base_point_id` — стабильный код текущей базисной точки в alias-слое.
- `water_obs_id` — стабильный код записи уровня воды.
- `wind_obs_id` — стабильный код наблюдения ветра.

## Что такое `qc_flag` и `qc_note`

- `qc_flag` — машинные QC-коды на уровне конкретной строки таблицы.
- `qc_note` — человекочитаемое русское пояснение к этим ограничениям, если оно нужно на уровне строки.

Типичный пример:
- `shoreline_observations.csv`: `qc_flag` хранит коды вроде `DUPLICATE_OBS_KEY`, а `qc_note` объясняет смысл проблемы по-русски.

## Что такое `qc_flag_analysis` и `qc_note_analysis`

- `qc_flag_analysis` — машинные коды ограничений уже на уровне аналитического интервала в `analysis_ready.csv`.
- `qc_note_analysis` — русское пояснение этих ограничений.

Типичные коды:
- `LOW_COVERAGE_WIND`
- `LOW_COVERAGE_WATER`
- `YEAR_ONLY_WATER_SOURCE`
- `SITE_SCOPE_NEEDS_REVIEW`

Идея:
флаги остаются стабильными кодами для кода и тестов, а человек читает `qc_note_analysis`.

## Что такое `qc_flag_analysis_safe` и `qc_note_analysis_safe`

- `qc_flag_analysis_safe` — консолидированный набор машинных кодов для строки `analysis_safe_subset.csv`, `final_dataset_for_modeling.csv` и `final_dataset_enriched_open_sources.csv`.
- `qc_note_analysis_safe` — русское сводное пояснение по строке, куда могут входить:
  - ограничения покрытия ветра;
  - ограничения покрытия воды;
  - year-only контекст воды;
  - контекст конфликтующих shoreline-дублей.

Это главный человекочитаемый QC-столбец для показа преподавателю.

## Что такое `scope_status` и `scope_note`

- `scope_status` — машинный статус проверки состава проекта.
  Сейчас обычно встречаются коды `reviewed` и `needs_review`.
- `scope_note` — русское пояснение, почему участок считается входящим или не входящим в подтверждённый project scope.

Источник:
`data/interim/site_scope_review.csv`

## Что такое `review_reason`

- `review_reason` — русское пояснение, почему строка базисной точки остаётся спорной или требует ручной проверки.

Примеры:
- дата отсутствует или указана неполностью;
- координатная привязка восстановлена по контексту документа и конфликтует с известным кластером.

## Что такое `water_time_resolution` и `water_context_scope`

- `water_time_resolution` — машинный код временного разрешения водного источника.
  Сейчас ключевое значение: `year_only`.
- `water_context_scope` — текстовый контекст гидрологического слоя, например нижний участок водохранилища.

Важно:
это не локальный полностью датированный ряд по каждому береговому участку, а годовой контекст для joins.

## Что такое `has_conflicting_shoreline_duplicates` и `duplicate_conflict_note`

- `has_conflicting_shoreline_duplicates` — булев индикатор того, что в контексте профиля есть конфликтующие группы дублей береговых наблюдений.
- `duplicate_conflict_note` — русское пояснение по этим дублям.

Это специально сохранено, чтобы такие случаи не исчезали молча из выборки.

## Какие machine codes сохранены на английском

Сохранены как коды, а не как пользовательский текст:

- `LOW_COVERAGE_WIND`
- `LOW_COVERAGE_WATER`
- `YEAR_ONLY_WATER_SOURCE`
- `SOURCE_QC_PRESENT`
- `CONFLICTING_SHORELINE_DUPLICATES_IN_PROFILE_CONTEXT`
- `MISSING_NUMERIC`
- `INVALID_NUMERIC`
- `ROW_NOTE`
- `DUPLICATE_OBS_KEY`
- `lower_section`
- `year_only`
- `reviewed`
- `original`
- `refined`
- `left_missing`
- `not_available`
- `LEVEL_A`
- `kamyshin`
- `mean_brow_position_pn_by_date_then_difference`

Почему они не переводятся прямо в колонках:
- на них завязаны код, тесты и воспроизводимые joins;
- это технический слой, а не текст для чтения преподавателем;
- рядом уже есть русские `*_note` или русская документация.

## Где смотреть русские пояснения

- В самих CSV:
  - `qc_note`
  - `qc_note_analysis`
  - `qc_note_analysis_safe`
  - `scope_note`
  - `review_reason`
  - `duplicate_conflict_note`
  - `wind_fill_validation_note`
  - `water_fill_validation_note`
- В документации:
  - `README_data.md`
  - `reports/tables/russian_text_audit.md`
  - `reports/tables/processed_table_inventory.md`
  - `data/interim/water_levels_manual_dictionary.md`

## Что показывать преподавателю в первую очередь

- `final_dataset_for_modeling.csv`
- `analysis_safe_subset.csv`
- `reports/tables/qc_summary.md`
- `reports/tables/01_periods_summary.csv`
- `reports/tables/02_profile_correlation_presentation.csv`
- графики из `reports/figures/`
