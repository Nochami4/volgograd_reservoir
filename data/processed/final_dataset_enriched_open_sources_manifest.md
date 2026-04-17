# final_dataset_enriched_open_sources

## Dataset Role

- Master dataset kept untouched: `data/processed/final_dataset_for_modeling.csv`
- Enriched companion dataset: `data/processed/final_dataset_enriched_open_sources.csv`
- Master file hash before build: 73fab2780add22b30f174985bf92339f6ada9e79475b580db5391d792283fd12
- Master file hash after build: 73fab2780add22b30f174985bf92339f6ada9e79475b580db5391d792283fd12
- Master changed during build: no

## Missingness Audit For Master

| column_name | missing_count | missing_share | candidate_for_enrichment | enrichment_domain | note |
| --- | ---: | ---: | --- | --- | --- |
| `mean_water_level_mean_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `max_water_level_mean_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `min_water_level_mean_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `range_water_level_mean_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `mean_water_level_max_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `max_water_level_max_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `min_water_level_max_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `range_water_level_max_annual_m_abs` | 123 | 0.214 | yes | water | Existing year-only water context is missing because the current lower-section source covers 1986-2018 only. |
| `water_context_scope` | 123 | 0.214 | yes | water | Context flag can only be populated if an honest official water extension is found. |
| `water_time_resolution` | 123 | 0.214 | yes | water | Context flag can only be populated if an honest official water extension is found. |

Wind note: the master dataset contains no wind columns by design, so wind enrichment is additive in the enriched companion layer rather than a repair of an existing master column.

## Enrichment Rules

- LEVEL A - exact-source enrichment: allowed for already curated local Kamyshin wind aggregates and for retaining the existing master water context unchanged.
- LEVEL B - same-station compatible-source enrichment: allowed only for the same Kamyshin station when metadata match is exact and overlap validation is acceptable; used here only to fill wind rows that were empty in the local baseline.
- LEVEL C - same-variable contextual carry-forward: allowed only for the existing year-only lower-section water context already present in master; no new carry-forward beyond current master semantics was introduced.
- LEVEL D - forbidden: no mean/median/KNN/ML imputation, no interpolation without domain basis, no station substitution without validation, no silent water extrapolation outside the known lower-section series.

## Source Evaluation

| source | domain | station_or_scope_match | time_resolution | decision | note |
| --- | --- | --- | --- | --- | --- |
| `data/raw/meteo/Камышин скорость и направление ветра.xlsx` via `analysis_ready.csv` | wind | exact project baseline | interval aggregates from local observations | used | Copied first and never overwritten. |
| NOAA Global Hourly `34363099999` | wind | KAMYSIN / `343630 99999 KAMYSIN                       RS            +50.067 +045.367 +0119.0 19320101 20250725` | hourly/synoptic | not_used | Exact station metadata matched; fill is limited to rows missing in the local baseline. |
| NOAA GSOD | wind | same NOAA station family, but derived dataset | daily summary | rejected | Daily summaries derived from hourly data are not mixed into the current interval-level hourly-style logic automatically. |
| Meteostat | wind | exact Kamyshin station match not established reproducibly in this run | hourly/daily portal | rejected | Not used automatically without a transparent exact-station match. |
| HydroWeb / AISORI-M / EIP Rosgidromet | water | lower-section extension not established reproducibly in this run | unclear / access path not resolved for direct use | rejected | No new official water series was adopted, so master water gaps remain missing. |

## Wind Validation Summary

- Station match line: `343630 99999 KAMYSIN                       RS            +50.067 +045.367 +0119.0 19320101 20250725`
- Inventory years detected: [2020, 2021, 2022, 2023, 2024, 2025]
- Exact local-time overlap count: 6
- Exact overlap median absolute error: 1.0
- Exact overlap correlation: -0.225924028528766
- Daily overlap count: 8
- Daily mean absolute error: 4.695982142857143
- Daily max absolute error: 4.875
- NOAA usable for fill under current rule: False

## Fill Outcome

- Master water rows missing any resolved water value: 123
- Rows with local wind context copied from the current project source: 96
- Rows with local exact-source wind speed summaries: 56
- Rows with local context only (counts/coverage without complete speed summary): 40
- Rows additionally filled from NOAA same-station compatible source: 0
- Rows still without wind mean/max after enrichment: 519
- Rows still without any wind observations after enrichment: 479
- Rows with retained master water context: 452
- Rows where water gaps remain missing: 123

## Open-Source References

- NOAA ISD station metadata: `https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt`
- NOAA ISD inventory: `https://www.ncei.noaa.gov/pub/data/noaa/isd-inventory.txt`
- NOAA Global Hourly access pattern: `https://www.ncei.noaa.gov/data/global-hourly/access/{year}/34363099999.csv`
- NOAA ISD product page: `https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database`
- NOAA GSOD overview: `https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516`
- EIP Rosgidromet portal checked for water-extension leads: `https://eip.meteo.ru/`
