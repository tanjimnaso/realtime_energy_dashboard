# Design: dbt Core + DuckDB Medallion Implementation

**Date:** 2026-03-16
**Status:** Approved — proceeding to implementation

## Context

The repo already has a working SCADA ingestion pipeline writing CSVs into `data/` and a live Streamlit app reading those CSVs. The Medallion architecture (Bronze/Silver/Gold) is documented in ADR-0001 and the folder structure is scaffolded, but no dbt models or DuckDB integration exists yet.

This design implements the local development stack documented in README.md and ADR-0003: **Parquet + DuckDB + dbt Core**.

## Decision: Skip GCP/BigQuery, build dbt Core locally first

GCP/BigQuery was considered (and suggested by Codex) but rejected for this stage for three reasons:

1. Zero GCP experience → 2–3 days of IAM/service-account setup before writing a single model
2. The repo already documents Azure Databricks + Delta Lake as the production-shaped design target, which better matches the Australian utilities/energy consulting hiring market
3. The most immediate portfolio win is working Bronze/Silver/Gold dbt models, not the cloud platform underneath them — the profile swap to BigQuery or Databricks can happen after dbt is working locally

## Architecture

```
data/dispatch_scada_YYYY-MM.csv   ─┐
data/duid_lookup.csv               ├─► ingestion/bronze_writer.py ─► data/bronze/*.parquet
data/emissions_factors.csv        ─┘

data/bronze/*.parquet
  └─► dbt sources (Bronze, views only)
        └─► models/silver/   (materialized tables in nem.duckdb)
              └─► models/gold/   (materialized tables in nem.duckdb)

GitHub Actions:
  fetch-aemo-data.yml  (existing, unchanged) ── writes CSVs every 5 min
  dbt-build.yml        (new)               ── runs bronze_writer + dbt build + dbt test
```

The existing `app.py` and `importdata.py` are **not modified** in this work package. The dbt layer is built in parallel; app.py continues reading CSVs until a later work package wires it to Gold tables.

## Files to Create

| File | Purpose |
|---|---|
| `ingestion/bronze_writer.py` | Converts `data/*.csv` to `data/bronze/*.parquet` |
| `dbt_project.yml` | dbt project config |
| `profiles/profiles.yml` | DuckDB profile pointing at `nem.duckdb` |
| `models/sources.yml` | Declares Bronze Parquet as dbt sources |
| `models/silver/silver_dispatch_interval.sql` | Typed, deduplicated SCADA intervals |
| `models/silver/silver_generator_metadata.sql` | Normalised generator metadata |
| `models/silver/silver_emissions_factor.sql` | Versioned emissions factors |
| `models/silver/schema.yml` | Silver tests and column docs |
| `models/gold/fct_regional_emissions_intensity.sql` | Regional gCO₂eq/kWh per interval |
| `models/gold/dim_generator.sql` | One row per DUID |
| `models/gold/schema.yml` | Gold tests and column docs |
| `tests/assert_emissions_intensity_plausible.sql` | Custom singular test (0–1500 gCO₂eq/kWh) |
| `.github/workflows/dbt-build.yml` | CI: bronze_writer + dbt build + dbt test |

## Data Models

### Bronze (dbt sources — views over Parquet, no transformation)

- `bronze_dispatch_scada` → `SETTLEMENTDATE`, `DUID`, `SCADAVALUE`
- `bronze_generator_metadata` → `DUID`, `Unit Name`, `Technology Type`, `Region`, `Dispatch Type`
- `bronze_emissions_factor` → `technology_type`, `emission_factor_tCO2e_MWh`

### Silver (typed, deduplicated — materialized tables)

- `silver_dispatch_interval`
  - Cast `SETTLEMENTDATE` to timestamp, rename to `settlement_date`
  - Rename `DUID` → `duid`, `SCADAVALUE` → `scada_mw`
  - Deduplicate on `(settlement_date, duid)`
  - Add `dq_flag` boolean: true when `scada_mw < 0 OR scada_mw > 5000`
- `silver_generator_metadata`
  - Normalise technology type names to controlled vocabulary
  - Enforce NEM region codes: `NSW1`, `VIC1`, `QLD1`, `SA1`, `TAS1`
  - Rename columns to snake_case
- `silver_emissions_factor`
  - Clean technology type join key (lowercase, trimmed)
  - Keep `emission_factor_tCO2e_MWh` for joining

### Gold (business-ready facts — materialized tables)

- `fct_regional_emissions_intensity`
  - Joins: `silver_dispatch_interval` × `silver_generator_metadata` × `silver_emissions_factor`
  - Calculates per 5-min interval per region:
    - `total_generation_mw`
    - `total_tCO2e` (sum of `scada_mw × 5/60 × emission_factor`)
    - `emissions_intensity_gCO2eq_per_kWh`
  - Primary key: `(settlement_date, region)`
- `dim_generator`
  - One row per DUID
  - Columns: `duid`, `unit_name`, `technology_type`, `region`, `dispatch_type`

## dbt Tests

**Schema tests** (in `schema.yml`):
- `not_null` + `unique` on all primary keys
- `accepted_values` on `region` codes: `NSW1`, `VIC1`, `QLD1`, `SA1`, `TAS1`
- `accepted_values` on `dispatch_type`: `Scheduled`, `Semi-scheduled`, `Non-scheduled`
- `not_null` on `scada_mw`, `settlement_date`

**Singular test** (`tests/assert_emissions_intensity_plausible.sql`):
- Fails if any row in `fct_regional_emissions_intensity` has intensity outside 0–1500 gCO₂eq/kWh

## GitHub Actions CI (`dbt-build.yml`)

```yaml
Trigger: push to main + daily schedule (06:00 UTC, after SCADA fetch window)
Steps:
  1. Checkout
  2. Python 3.11
  3. pip install dbt-duckdb pyarrow pandas
  4. python ingestion/bronze_writer.py
  5. dbt build --profiles-dir profiles/
  6. dbt test --profiles-dir profiles/
```

`nem.duckdb` is gitignored — rebuilt from source on every run.

## What is NOT in scope

- Modifying `app.py` to read from Gold tables (Work Package 4 in roadmap)
- Azure Databricks or cloud deployment
- DISPATCHPRICE, DISPATCHREGIONSUM, rooftop PV sources (future ingestion work)
- Docker Compose or devcontainer

## Relation to Existing ADRs

| ADR | Status |
|---|---|
| ADR-0001: Medallion architecture | This design implements it |
| ADR-0002: Batch over streaming | Unchanged — batch ingestion continues |
| ADR-0003: Parquet + DuckDB local dev | This design implements it |
| ADR-0004: History boundary | Respected — SCADA history boundary unchanged |
