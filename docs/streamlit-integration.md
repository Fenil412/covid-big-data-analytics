# Streamlit integration

Run the existing dashboard with `streamlit run dashboard/app.py`, or build/start the Compose dashboard and open <http://localhost:8501>. It reads the processed MongoDB collections when configured and falls back to Parquet under `output/`. Streamlit filters and charts these processed summaries; the distributed aggregations belong to Spark.

## Processed result contracts

- `country_summary`: one row per country-level `location_key`; totals, active cases, population, and per-capita rates.
- `daily_summary`: global per-date new and cumulative counts.
- `country_daily_summary`: country and date level new and cumulative counts used by date-filtered maps and country selections.
- `vaccination_summary`: distinguish `total_people_vaccinated` (people receiving at least one dose) from `total_vaccine_doses_administered` (individual doses). Use the field name from Spark to choose the dashboard label.
- `regional_summary`: state/province aggregates with a parent-country field where the source provides it. It has no boundary geometry, so a regional choropleth is not available.

The dashboard derives country/region coverage from processed `location_key` values and does not exclude territories or aliases. Map display-name substitutions only help match geographic names; source names and keys remain in the hover/details view.

Legacy `total_vaccinated` output has no guaranteed meaning in this project contract. The dashboard labels it as an undefined legacy metric and does not infer that it means people or doses. Run the current Spark job to publish the separately defined vaccination columns. Old Parquet output remains readable for cases and deaths but does not provide fields missing from its schema.

The left sidebar controls the persisted light/dark theme, country and date filters, daily/weekly/monthly historical grouping, case/population/vaccination thresholds, and (when present) state/province selection. The Data Explorer provides processed-result and raw-source previews, quality summaries, search/sort, and CSV/Excel downloads.
