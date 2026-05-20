# Инвентаризация очистки репозитория

Дата инвентаризации: 2026-04-29

## Статус

Инвентаризация выполнена, безопасная очистка legacy- и временных файлов выполнена, верификация пайплайна и тестов завершена. Ниже зафиксированы:
- рабочее ядро, которое оставлено без изменений;
- производные артефакты, которые сохранены;
- legacy- и временные артефакты, которые удалены;
- спорные файлы, которые не удаляются автоматически.

## Оставлено: рабочее ядро

- `src/` — рабочие парсеры, QC, аналитика и export-скрипты.
- `tests/` — тесты рабочего пайплайна.
- `data/raw/` — канонические исходники; не изменяются и не очищаются.
- `data/processed/` — текущие производные таблицы датасета:
  - `analysis_ready.csv`
  - `analysis_safe_subset.csv`
  - `base_points.csv`
  - `base_points_current.csv`
  - `base_points_history.csv`
  - `final_dataset_enriched_open_sources.csv`
  - `final_dataset_enriched_open_sources_manifest.md`
  - `final_dataset_for_modeling.csv`
  - `interval_metrics.csv`
  - `profiles.csv`
  - `shoreline_observations.csv`
  - `sites.csv`
  - `water_levels_raw.csv`
  - `wind_obs_hourly.csv`
- `data/interim/` — текущие промежуточные и QC-артефакты:
  - `qc_summary.json`
  - `shoreline_duplicate_report.csv`
  - `site_scope_review.csv`
  - `water_levels_manual_dictionary.md`
  - `water_levels_profile.json`
  - `wind_coverage_by_interval.csv`
  - `shoreline_observations.log`
  - `shoreline_observations_log.json`
  - `wind_obs_hourly.log`
  - `wind_obs_hourly_log.json`
  - `base_points_manual_template.csv`
  - `base_points_manual_template_README.md`
  - `final_dataset_enriched_open_sources_validation.json`
- `reports/tables/` — текущие QC и аналитические таблицы:
  - `qc_summary.md`
  - `01_periods_summary.csv`
  - `02_profile_correlation_summary.csv`
  - `02_profile_correlation_pairs.csv`
  - `02_profile_correlation_diagnostics.csv`
  - `02_profile_correlation_presentation.csv`
  - `base_points_manual_review.csv`
- `reports/figures/` — актуальные `.png`-графики текущего аналитического этапа:
  - `01_retreat_displacement_hist.png`
  - `01_retreat_distributions.png`
  - `01_retreat_distributions_composite.png`
  - `01_retreat_rate_hist.png`
  - `01_site_intensity_summary.png`
  - `01_site_interval_timelines.png`
  - `01_site_interval_timelines_full.png`
  - `01_site_interval_timelines_presentation.png`
  - `02_profile_correlation_appendix.png`
  - `02_profile_correlation_appendix_page_1.png`
  - `02_profile_correlation_appendix_page_2.png`
  - `02_profile_correlation_berezhnovka.png`
  - `02_profile_correlation_burty.png`
  - `02_profile_correlation_heatmaps.png`
  - `02_profile_correlation_molchanovka.png`
  - `02_profile_correlation_nizhniy_balykley.png`
  - `02_profile_correlation_nizhniy_urakov.png`
  - `02_profile_correlation_novonikolskoe.png`
  - `02_profile_correlation_overview.png`
  - `02_profile_correlation_pichuga_yuzhny.png`
  - `02_profile_correlation_proleyskiy.png`
  - `02_profile_correlation_urakov_bugor.png`
- Рабочая документация и конфигурация:
  - `README.md`
  - `README_data.md`
  - `AGENTS.md`
  - `Makefile`
  - `pyproject.toml`

## Удалено: безопасно удаляемые legacy- и временные артефакты

### Legacy-наследие в `archive/legacy/po/`

Удалены как неиспользуемые текущим пайплайном, README и тестами:
- директории:
  - `archive/legacy/po/plots/`
  - `archive/legacy/po/plots_lithology/`
  - `archive/legacy/po/presentation_results/`
  - `archive/legacy/po/results_models/`
  - `archive/legacy/po/results_models_improved/`
- legacy notebooks:
  - `archive/legacy/po/Notebook.ipynb`
  - `archive/legacy/po/Prognoz_profili_otdelno.ipynb`
  - `archive/legacy/po/Разделение_профили.ipynb`
- legacy scripts:
  - `archive/legacy/po/corr_all_shets_inp.py`
  - `archive/legacy/po/corr_inp.py`
  - `archive/legacy/po/errors_powerpoint.py`
  - `archive/legacy/po/prognoz.py`
  - `archive/legacy/po/prognoz_beregov_wind_augmented.py`
  - `archive/legacy/po/prognoz_luchshe.py`
  - `archive/legacy/po/prognoz_train_test.py`
- legacy Excel-таблицы, не используемые текущими точками входа из `src/`:
  - `archive/legacy/po/profiles_ipn.xlsx`
  - `archive/legacy/po/литология_vs_отступание.xlsx`
  - `archive/legacy/po/результаты_корреляции.xlsx`
  - `archive/legacy/po/результаты_корреляции_и_литология.xlsx`
  - `archive/legacy/po/БРОВКИ.xls`
  - `archive/legacy/po/Бровки.xlsx`
  - `archive/legacy/po/Бровки_now.xlsx`
  - `archive/legacy/po/240716 БРОВКИ Баранова.xls`
  - `archive/legacy/po/240716 Данные берега.xlsx`

### Temporary files and caches

Удалены вне `data/raw/`, если присутствовали:
- `.ipynb_checkpoints/`
- `__pycache__/`
- `*.pyc`
- `~$*.xlsx`
- `~$*.xls`
- `~$*.docx`
- `~$*.doc`
- `~$*.pptx`
- `.DS_Store`
- `Thumbs.db`

Фактически очищены:
- `archive/.ipynb_checkpoints/`
- `src/__pycache__/`
- `src/analysis/__pycache__/`
- `src/export/__pycache__/`
- `src/parsers/__pycache__/`
- `src/qc/__pycache__/`
- `tests/__pycache__/`
- `.pyc`-файлы вне `.venv/`
- пустой legacy-каталог `archive/legacy/po/plots_predictions/`

## Не удалено из-за неопределённости

- `archive/unknown/`
  - `archive/unknown/Разработка ПО для анализа абразии берегов Волгоградского водохранилища.pptx`
  - `archive/unknown/Статья/Аннотация и Введение.docx`
  - `archive/unknown/Статья/Результаты и обсуждение.docx`
  Причина: это явно не рабочие ETL-артефакты, но они похожи на документы по статье/презентации; без отдельного решения не удаляются и не перемещаются автоматически.
- Файлы в корне `archive/legacy/po/`, не входящие в явный список безопасного удаления:
  - `Камышин скорость и направление ветра.xlsx`
  - `Ориентация берега участки.xlsx`
  - `0FD01A00`
  - `4326F900`
  - `74FFE100`
  - `8E57F900`
  - `BDEDA200`
  - `D4074200`
  - `F10F0A00`
  - `FFA6F900`
  Причина: они лежат в legacy-папке и не используются текущим кодом, но часть из них выглядит как старые Excel/служебные бинарные выгрузки без понятного назначения. Их стоит удалить только после отдельного ручного решения, если нужно полностью вычистить архив.

## Почему удаление выше считается безопасным

- Текущий рабочий код читает входные данные только из `data/raw/`, а не из `archive/legacy/po/`.
- В `README.md`, `README_data.md`, `src/`, `tests/` и актуальных ноутбуках не найдено ссылок на удаляемые legacy notebooks/scripts/results-папки.
- Актуальные графики и таблицы уже генерируются текущим кодом в `reports/figures/` и `reports/tables/`.
- В `reports/figures/` сейчас нет `.pdf`/`.svg` дублей; текущий набор состоит из `.png` и сохраняется полностью.

## Проверка после очистки

- Основной ETL/QC-скрипт успешно выполнен через `./.venv/bin/python -m src.pipeline`.
- Отдельный аналитический этап успешно выполнен через `./.venv/bin/python -m src.analysis.first_stage_analysis`.
- Тесты успешно выполнены через `./.venv/bin/pytest`: `17 passed`.
- Подтверждено наличие обязательных артефактов:
  - `data/processed/analysis_ready.csv`
  - `data/processed/interval_metrics.csv`
  - `data/processed/shoreline_observations.csv`
  - `data/processed/sites.csv`
  - `data/processed/profiles.csv`
  - `data/processed/wind_obs_hourly.csv`
  - `data/processed/water_levels_raw.csv`
  - `data/processed/base_points.csv`
  - `reports/tables/qc_summary.md`
- Подтверждено наличие актуального набора `.png`-графиков в `reports/figures/`.
- Проверка ссылок в `README.md`, `README_data.md`, `notebooks/`, `src/` и `tests/` не показала ссылок на удалённые legacy-файлы.
- Во время аналитического этапа были предупреждения `matplotlib` про `constrained_layout`; скрипт при этом завершился успешно и все ожидаемые outputs были записаны.
- После повторного прогона пайплайна обновились parser log-файлы в `data/interim/`, что допустимо и соответствует текущей политике сохранения QC/log outputs.

## Примечание по окружению

- Локальное окружение `.venv/` не удалялось и не очищалось как часть cleanup, потому что оно уже игнорируется репозиторием и не относится к рабочим данным проекта.
