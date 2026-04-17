# Water Levels Dictionary

This file documents the mechanically extracted water layer used in the current pipeline.

## Resolved Fields

- `water_level_mean_annual_m_abs`: annual mean water level for the lower reservoir section, meters absolute.
- `water_level_max_annual_m_abs`: annual maximum water level for the lower reservoir section, meters absolute.
- `obs_date`: intentionally empty because the source workbook provides annual values by year only.
- `water_section_id`: `lower_section`.
- `water_section_name`: `Нижний участок (озерный)`.

## Join Logic

- The lower-section annual series is attached to each shoreline site as shared hydrological context for interval-level joins.
- This preserves pipeline compatibility without inventing site-local observation dates or local water series.

## Extraction Profile

- Source workbook: `data/raw/Уровни ВДХР 1986-2018.xlsx`
- Source sheet: `Данные по участкам`
- Source rows scanned: 88
- Recognized years: 33
- Year range: 1986..2018
- Site rows written per year: 10
