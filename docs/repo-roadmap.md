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

### Work package 1

- README rewrite
- ADR set
- repo scaffolding

### Work package 2

- ingestion folder buildout
- multi-source backfill scripts
- raw landing conventions

### Work package 3

- dbt project or model scaffold
- Bronze/Silver/Gold tables
- tests

### Work package 4

- app refactor to read Gold tables
- scenario layer replacement
- Docker / CI hardening

