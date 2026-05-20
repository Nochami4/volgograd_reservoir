# Инвентаризация processed-таблиц

Дата: 2026-04-29

## Цель

Зафиксировать, какие таблицы в `data/processed/` являются основными, какие являются служебными, и что делать со спорными `final_dataset_*`.

## Основные таблицы

- `sites.csv`
- `profiles.csv`
- `shoreline_observations.csv`
- `interval_metrics.csv`
- `analysis_ready.csv`
- `analysis_safe_subset.csv`
- `final_dataset_for_modeling.csv`

Причина:
эти таблицы образуют основной ETL/QC и аналитический контур проекта и непосредственно используются в текущих задачах, отчётах и показе преподавателю.

## Служебные, но рабочие processed-таблицы

- `wind_obs_hourly.csv`
- `water_levels_raw.csv`
- `base_points_history.csv`
- `base_points_current.csv`
- `base_points.csv`

Причина:
они не являются главным показным результатом, но нужны для воспроизводимости, joins, QC и последующей аналитики.

## Проверка `final_dataset_for_modeling.csv`

### Где создаётся

- `src/analysis/first_stage_analysis.py`

### Где используется

- `README_data.md`
- `src/qc/run_qc.py`
- `src/analysis/build_final_dataset_enriched_open_sources.py`
- `src/export/build_delivery_exports.py`
- `tests/test_analysis_outputs.py`

### Решение

Оставить в `data/processed/` как основной компактный итоговый набор.

### Почему это безопасно

- таблица пересобирается воспроизводимо из `analysis_safe_subset.csv`;
- на неё завязаны код и тесты;
- она подходит как главный табличный результат для преподавателя;
- удаление или перенос сломали бы текущую архитектуру и документацию.

## Проверка `final_dataset_enriched_open_sources.csv`

### Где создаётся

- `src/analysis/build_final_dataset_enriched_open_sources.py`

### Где используется

- `README_data.md`
- `src/export/build_delivery_exports.py`
- `tests/test_enriched_dataset_builder.py`
- `data/processed/final_dataset_enriched_open_sources_manifest.md`
- `data/interim/final_dataset_enriched_open_sources_validation.json`

### Характер таблицы

Это не главный датасет проекта, а отдельный дополнительный слой с прозрачным обогащением по открытым источникам.

### Решение

Пока оставить в `data/processed/`, но явно описать как экспериментальный и не использовать как главный файл для показа преподавателю.

### Почему не переносится сейчас

- таблица уже встроена в код, документацию и тесты;
- у неё есть собственный manifest и validation JSON;
- она демонстрирует аккуратное обогащение с явным происхождением данных без изменения основного набора;
- автоматический перенос в `data/interim/` или `archive/` был бы уже архитектурным изменением, а не только русификацией/cleanup.

## Итоговая рекомендация

- `final_dataset_for_modeling.csv` считать основным processed-результатом.
- `final_dataset_enriched_open_sources.csv` считать дополнительным экспериментальным слоем.
- Оба файла пока оставить в `data/processed/`.

## Что показывать преподавателю как главные результаты

- `final_dataset_for_modeling.csv`
- `analysis_safe_subset.csv`
- `reports/tables/qc_summary.md`
- `reports/tables/01_periods_summary.csv`
- `reports/tables/02_profile_correlation_presentation.csv`
- `reports/figures/01_site_interval_timelines_presentation.png`
- `reports/figures/01_retreat_distributions_composite.png`
- `reports/figures/02_profile_correlation_overview.png`

## Что показывать только как дополнительный слой

- `final_dataset_enriched_open_sources.csv`
- `data/processed/final_dataset_enriched_open_sources_manifest.md`

Причина:
это уже не базовый ETL/QC-результат, а демонстрация осторожного enrichment-эксперимента.
