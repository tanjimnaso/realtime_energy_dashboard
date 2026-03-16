# Repo Roadmap

## Objective

Turn the current prototype into a portfolio repository that reads like a mid-level data engineering project for the Australian energy market.

## Current state

The repo currently contains:

- a working Streamlit app
- a working SCADA importer
- metadata and factors in flat files
- GitHub Actions for incremental SCADA refresh

This is a strong prototype, but it still reads as an app-first project rather than a data-platform-first project.

## Target structure

```text
app/
ingestion/
models/
  bronze/
  silver/
  gold/
tests/
dashboards/
docs/
  adr/
data/
```

## What each folder will hold

### `app/`

- Streamlit app entrypoint
- lightweight presentation utilities
- chart helpers

### `ingestion/`

- raw AEMO source fetchers
- incremental loaders
- one-off backfill scripts

### `models/`

- Bronze/Silver/Gold SQL or dbt models
- tests and schema docs

### `tests/`

- Python tests
- data quality assertions
- validation utilities

### `dashboards/`

- static screenshots
- sample outputs
- optional dashboard assets

### `docs/adr/`

- architecture decisions
- trade-offs and rationale

## Migration approach

Avoid breaking the live app while restructuring.

1. Create the target folders first.
2. Leave current root scripts in place.
3. Introduce new code in the target folders gradually.
4. Move the root scripts only when imports and workflow paths are stable.

## Priority work packages

### Work package 1 — Complete

- README rewrite
- ADR set
- repo scaffolding

### Work package 2 — Complete

- ingestion folder buildout (SCADA importer, monthly archive, backfill helper)
- raw CSV landing in `data/`

### Work package 3 — Complete

**Stack decision:** dbt Core + DuckDB locally. GCP/BigQuery skipped at this stage.
Rationale: Azure Databricks + Delta Lake is the production-shaped design target (better fit for Australian energy hiring market). dbt profile swap to cloud is deferred until local models are working.

Deliverables:
- `ingestion/bronze_writer.py` — converts `data/*.csv` to `data/bronze/*.parquet`
- `dbt_project.yml` + `profiles/profiles.yml` (DuckDB)
- `models/sources.yml` — Bronze Parquet as dbt sources
- `models/silver/` — `silver_dispatch_interval`, `silver_generator_metadata`, `silver_emissions_factor`
- `models/gold/` — `fct_regional_emissions_intensity`, `dim_generator`
- `tests/` — singular test for emissions intensity plausibility
- `.github/workflows/dbt-build.yml` — CI running bronze_writer + dbt build + dbt test

See: [Design doc](/docs/plans/2026-03-16-dbt-duckdb-medallion-design.md)

### Work package 4

- app refactor to read Gold DuckDB tables instead of raw CSVs
- scenario layer replacement
- Docker / CI hardening

