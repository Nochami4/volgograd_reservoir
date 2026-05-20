# Автоэнкодер: поиск подозрительных наблюдений

Автоэнкодер — это нейросетевая модель, которая учится восстанавливать входные данные. Если строка плохо восстанавливается, она отличается от типичной структуры признаков и попадает в список для проверки.

Использован файл `data/processed/final_dataset_for_modeling.csv`. В модель передано 575 строк, 23 исходных признаков до кодирования категорий и 42 числовых признаков после предобработки.

Предобработка: числовые признаки заполняются медианой и масштабируются, категориальные признаки заполняются наиболее частым значением и кодируются. Целевая переменная и прямые метрики изменения береговой бровки не используются как признаки.

Порог подозрительности выбран как 95-й процентиль ошибки восстановления на обучающей части (460 строк): `0.039610`.

Помечено подозрительных строк: 32 из 575 (5.6%).

Если при обучении появляется предупреждение о сходимости, оно не скрывается: модель используется как демонстрационный ориентир, а не как финально настроенная нейросетевая архитектура.

Эти строки не удаляются автоматически и не считаются доказанными ошибками исходных данных. Их нужно проверить предметно: посмотреть участок, профиль, даты интервала, QC-пояснения и возможные конфликтующие дубли.

Использованные признаки:

- числовые: shore_orientation_deg, n_water_obs, coverage_water, mean_water_level_mean_annual_m_abs, max_water_level_mean_annual_m_abs, min_water_level_mean_annual_m_abs, range_water_level_mean_annual_m_abs, mean_water_level_max_annual_m_abs, max_water_level_max_annual_m_abs, min_water_level_max_annual_m_abs, range_water_level_max_annual_m_abs, history_start_year, has_conflicting_shoreline_duplicates, conflicting_duplicate_group_count, interval_mid_year;
- категориальные: shore_type, shore_orientation_text, exposure_sectors_text, lithology_text, lithology_class, water_context_scope, water_time_resolution, history_start_group.
