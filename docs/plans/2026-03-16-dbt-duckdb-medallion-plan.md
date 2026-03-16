# dbt Core + DuckDB Medallion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the Bronze/Silver/Gold Medallion data layers using dbt Core + DuckDB, building on existing SCADA CSV archives without touching the live Streamlit app.

**Architecture:** `ingestion/bronze_writer.py` converts raw CSVs to Parquet in `data/bronze/`. dbt sources declare those Parquet files as Bronze. Silver models type, clean, and deduplicate. Gold models join and compute regional emissions intensity. Everything persists in `nem.duckdb`, which is gitignored and rebuilt from source on each CI run.

**Tech Stack:** Python 3.11, pandas, pyarrow, dbt-duckdb, DuckDB, GitHub Actions

---

## Source Data Reference

Three CSVs drive everything:

```
data/dispatch_scada_YYYY-MM.csv   → columns: SETTLEMENTDATE, DUID, SCADAVALUE
data/duid_lookup.csv              → columns: DUID, Unit Name, Technology Type, Region, Dispatch Type, source
data/emissions_factors.csv        → columns: technology_type, scope, emission_factor_tCO2e_MWh, ...
```

Technology types in duid_lookup: `Battery Storage`, `Coal`, `Gas Turbine`, `Hydro`, `Other`, `Solar PV`, `Wind`
NEM regions: `NSW1`, `VIC1`, `QLD1`, `SA1`, `TAS1`
Emissions factors scopes: `scope_1`, `scope_3` — we join on `scope_1` only for intensity calculation

---

## Task 1: Install dbt-duckdb and verify tooling

**Files:**
- Modify: `requirements.txt`

**Step 1: Add dbt-duckdb and pyarrow to requirements**

```
# append to requirements.txt
dbt-duckdb>=1.8.0
pyarrow>=14.0.0
duckdb>=0.10.0
```

**Step 2: Install locally**

```bash
pip install dbt-duckdb pyarrow duckdb
```

Expected: installs without error. Verify:

```bash
dbt --version
python -c "import duckdb; print(duckdb.__version__)"
```

Expected output: dbt version ≥ 1.8, duckdb version ≥ 0.10

**Step 3: Commit**

```bash
git add requirements.txt
git commit -m "deps: add dbt-duckdb, pyarrow, duckdb"
```

---

## Task 2: Create dbt project configuration

**Files:**
- Create: `dbt_project.yml`
- Create: `profiles/profiles.yml`

**Step 1: Create dbt_project.yml**

```yaml
# dbt_project.yml
name: nem_pipeline
version: "1.0.0"
config-version: 2

profile: nem_pipeline

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  nem_pipeline:
    silver:
      +materialized: table
      +schema: silver
    gold:
      +materialized: table
      +schema: gold
```

**Step 2: Create profiles/profiles.yml**

```yaml
# profiles/profiles.yml
nem_pipeline:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: nem.duckdb
      threads: 4
```

**Step 3: Verify dbt can parse the project**

```bash
dbt debug --profiles-dir profiles/
```

Expected: "All checks passed!" (connection will pass once nem.duckdb can be created)

**Step 4: Add nem.duckdb to .gitignore**

Add this line to `.gitignore`:
```
nem.duckdb
nem.duckdb.wal
data/bronze/
target/
dbt_packages/
```

**Step 5: Commit**

```bash
git add dbt_project.yml profiles/profiles.yml .gitignore
git commit -m "chore: scaffold dbt project config with DuckDB profile"
```

---

## Task 3: Write bronze_writer.py

**Files:**
- Create: `ingestion/bronze_writer.py`
- Create: `ingestion/__init__.py` (empty)

**Step 1: Create ingestion/__init__.py**

Empty file to make `ingestion` a proper Python package.

**Step 2: Write bronze_writer.py**

```python
"""
Bronze writer: converts raw data/ CSVs to typed Parquet files in data/bronze/.

Run this before dbt build to refresh the Bronze layer.
"""
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BRONZE_DIR = DATA_DIR / "bronze"


def write_dispatch_scada() -> None:
    """Concatenate all monthly SCADA CSV archives into one Bronze Parquet file."""
    archive_files = sorted(DATA_DIR.glob("dispatch_scada_????-??.csv"))
    if not archive_files:
        raise FileNotFoundError(f"No monthly SCADA archives found in {DATA_DIR}")

    frames = []
    for f in archive_files:
        df = pd.read_csv(f, dtype={"DUID": str, "SCADAVALUE": float})
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")
        frames.append(df)
        print(f"  Read {f.name}: {len(df):,} rows")

    combined = pd.concat(frames, ignore_index=True)
    out = BRONZE_DIR / "dispatch_scada.parquet"
    combined.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(combined):,} rows")


def write_generator_metadata() -> None:
    """Copy duid_lookup.csv to Bronze Parquet."""
    src = DATA_DIR / "duid_lookup.csv"
    df = pd.read_csv(src, dtype=str)
    out = BRONZE_DIR / "generator_metadata.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(df):,} rows")


def write_emissions_factors() -> None:
    """Copy emissions_factors.csv to Bronze Parquet."""
    src = DATA_DIR / "emissions_factors.csv"
    df = pd.read_csv(src, dtype={"emission_factor_tCO2e_MWh": float})
    out = BRONZE_DIR / "emissions_factors.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(df):,} rows")


def main() -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    print("=== Bronze writer ===")
    write_dispatch_scada()
    write_generator_metadata()
    write_emissions_factors()
    print("=== Done ===")


if __name__ == "__main__":
    main()
```

**Step 3: Run it to verify**

```bash
python ingestion/bronze_writer.py
```

Expected output (row counts will vary):
```
=== Bronze writer ===
  Read dispatch_scada_2026-02.csv: X,XXX rows
  Read dispatch_scada_2026-03.csv: X,XXX rows
Wrote dispatch_scada.parquet: X,XXX rows
Wrote generator_metadata.parquet: XXX rows
Wrote emissions_factors.parquet: 16 rows
=== Done ===
```

Verify files exist:
```bash
ls -lh data/bronze/
```

Expected: three `.parquet` files

**Step 4: Commit**

```bash
git add ingestion/__init__.py ingestion/bronze_writer.py
git commit -m "feat: add bronze_writer converting CSVs to Parquet"
```

---

## Task 4: Declare Bronze sources in dbt

**Files:**
- Create: `models/sources.yml`

**Step 1: Create models/sources.yml**

```yaml
version: 2

sources:
  - name: bronze
    description: "Raw AEMO data landed as Parquet by ingestion/bronze_writer.py"
    meta:
      layer: bronze
    tables:
      - name: dispatch_scada
        description: "AEMO Dispatch SCADA 5-minute generation readings"
        external:
          location: "data/bronze/dispatch_scada.parquet"
        columns:
          - name: SETTLEMENTDATE
            description: "5-minute dispatch interval end timestamp"
          - name: DUID
            description: "Dispatchable Unit Identifier"
          - name: SCADAVALUE
            description: "Generation output in MW"

      - name: generator_metadata
        description: "AEMO generator registration metadata (DUID lookup)"
        external:
          location: "data/bronze/generator_metadata.parquet"
        columns:
          - name: DUID
          - name: Unit Name
          - name: Technology Type
          - name: Region
          - name: Dispatch Type

      - name: emissions_factors
        description: "NGA emissions factors by technology type and scope"
        external:
          location: "data/bronze/emissions_factors.parquet"
        columns:
          - name: technology_type
          - name: scope
          - name: emission_factor_tCO2e_MWh
```

**Step 2: Verify dbt can see the sources**

```bash
dbt ls --profiles-dir profiles/ --resource-type source
```

Expected: lists three sources under `nem_pipeline`

**Step 3: Commit**

```bash
git add models/sources.yml
git commit -m "feat: declare bronze Parquet sources in dbt"
```

---

## Task 5: Write Silver model — silver_dispatch_interval

**Files:**
- Create: `models/silver/silver_dispatch_interval.sql`
- Create: `models/silver/schema.yml`

**Step 1: Create models/silver/silver_dispatch_interval.sql**

```sql
-- models/silver/silver_dispatch_interval.sql
-- Typed, deduplicated SCADA dispatch intervals with a data-quality flag.

with source as (
    select * from {{ source('bronze', 'dispatch_scada') }}
),

typed as (
    select
        cast(SETTLEMENTDATE as timestamp) as settlement_date,
        trim(DUID)                         as duid,
        cast(SCADAVALUE as double)         as scada_mw
    from source
    where SETTLEMENTDATE is not null
      and DUID is not null
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by settlement_date, duid
        order by settlement_date
    ) = 1
),

final as (
    select
        settlement_date,
        duid,
        scada_mw,
        -- Flag readings that are physically implausible
        (scada_mw < 0 or scada_mw > 5000) as dq_flag
    from deduped
)

select * from final
```

**Step 2: Create models/silver/schema.yml**

```yaml
version: 2

models:
  - name: silver_dispatch_interval
    description: "Typed and deduplicated 5-minute SCADA dispatch intervals with data quality flags"
    columns:
      - name: settlement_date
        description: "Dispatch interval end timestamp (UTC+10 AEST as published by AEMO)"
        tests:
          - not_null
      - name: duid
        description: "Dispatchable Unit Identifier"
        tests:
          - not_null
      - name: scada_mw
        description: "Generation output in MW"
        tests:
          - not_null
      - name: dq_flag
        description: "True if scada_mw is outside the plausible range (< 0 or > 5000 MW)"
        tests:
          - not_null
```

**Step 3: Run the model**

```bash
dbt run --profiles-dir profiles/ --select silver_dispatch_interval
```

Expected: `1 of 1 OK`

**Step 4: Run schema tests**

```bash
dbt test --profiles-dir profiles/ --select silver_dispatch_interval
```

Expected: all tests pass

**Step 5: Commit**

```bash
git add models/silver/silver_dispatch_interval.sql models/silver/schema.yml
git commit -m "feat: add silver_dispatch_interval dbt model with dq_flag"
```

---

## Task 6: Write Silver model — silver_generator_metadata

**Files:**
- Modify: `models/silver/schema.yml`
- Create: `models/silver/silver_generator_metadata.sql`

**Step 1: Create models/silver/silver_generator_metadata.sql**

```sql
-- models/silver/silver_generator_metadata.sql
-- Normalised generator registration metadata, one row per DUID.

with source as (
    select * from {{ source('bronze', 'generator_metadata') }}
),

renamed as (
    select
        trim("DUID")          as duid,
        trim("Unit Name")     as unit_name,
        trim("Technology Type") as technology_type,
        trim("Region")        as region,
        trim("Dispatch Type") as dispatch_type
    from source
    where "DUID" is not null
),

-- Enforce known NEM region codes; anything else is flagged as NULL
conformed as (
    select
        duid,
        unit_name,
        technology_type,
        case
            when region in ('NSW1','VIC1','QLD1','SA1','TAS1') then region
            else null
        end as region,
        dispatch_type
    from renamed
),

deduped as (
    select *
    from conformed
    qualify row_number() over (partition by duid order by duid) = 1
)

select * from deduped
```

**Step 2: Append to models/silver/schema.yml**

Add under `models:`:

```yaml
  - name: silver_generator_metadata
    description: "Normalised AEMO generator metadata with region codes enforced"
    columns:
      - name: duid
        description: "Dispatchable Unit Identifier (primary key)"
        tests:
          - not_null
          - unique
      - name: unit_name
        description: "Human-readable generator name"
      - name: technology_type
        description: "Generator technology (Coal, Gas Turbine, Solar PV, Wind, Hydro, Battery Storage, Other)"
        tests:
          - not_null
          - accepted_values:
              values: ['Coal', 'Brown coal', 'Gas Turbine', 'Hydro', 'Wind', 'Solar PV', 'Battery Storage', 'Other']
      - name: region
        description: "NEM region code"
        tests:
          - accepted_values:
              values: ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']
      - name: dispatch_type
        description: "AEMO dispatch classification"
        tests:
          - accepted_values:
              values: ['Scheduled', 'Semi-scheduled', 'Non-scheduled']
```

**Step 3: Run and test**

```bash
dbt run --profiles-dir profiles/ --select silver_generator_metadata
dbt test --profiles-dir profiles/ --select silver_generator_metadata
```

Expected: model runs OK, tests pass

**Step 4: Commit**

```bash
git add models/silver/silver_generator_metadata.sql models/silver/schema.yml
git commit -m "feat: add silver_generator_metadata model"
```

---

## Task 7: Write Silver model — silver_emissions_factor

**Files:**
- Modify: `models/silver/schema.yml`
- Create: `models/silver/silver_emissions_factor.sql`

**Step 1: Create models/silver/silver_emissions_factor.sql**

```sql
-- models/silver/silver_emissions_factor.sql
-- Scope 1 emissions factors only, with a normalised join key.
-- Scope 3 factors exist in Bronze but are excluded from the intensity calculation.

with source as (
    select * from {{ source('bronze', 'emissions_factors') }}
),

scope1 as (
    select
        lower(trim(technology_type))       as technology_type_key,
        trim(technology_type)              as technology_type,
        cast(emission_factor_tCO2e_MWh as double) as emission_factor_tCO2e_MWh
    from source
    where scope = 'scope_1'
)

select * from scope1
```

**Step 2: Append to models/silver/schema.yml**

```yaml
  - name: silver_emissions_factor
    description: "Scope 1 NGA emissions factors by technology type"
    columns:
      - name: technology_type_key
        description: "Lowercase normalised technology type for joining"
        tests:
          - not_null
          - unique
      - name: technology_type
        description: "Original technology type label"
        tests:
          - not_null
      - name: emission_factor_tCO2e_MWh
        description: "Scope 1 emissions factor in tCO2e per MWh"
        tests:
          - not_null
```

**Step 3: Run and test**

```bash
dbt run --profiles-dir profiles/ --select silver_emissions_factor
dbt test --profiles-dir profiles/ --select silver_emissions_factor
```

Expected: 8 rows (one per technology type for scope_1), all tests pass

**Step 4: Commit**

```bash
git add models/silver/silver_emissions_factor.sql models/silver/schema.yml
git commit -m "feat: add silver_emissions_factor model (scope_1 only)"
```

---

## Task 8: Write Gold model — dim_generator

**Files:**
- Create: `models/gold/dim_generator.sql`
- Create: `models/gold/schema.yml`

**Step 1: Create models/gold/dim_generator.sql**

```sql
-- models/gold/dim_generator.sql
-- Dimension table: one row per DUID with technology, region, and dispatch type.

select
    duid,
    unit_name,
    technology_type,
    lower(technology_type) as technology_type_key,
    region,
    dispatch_type
from {{ ref('silver_generator_metadata') }}
```

**Step 2: Create models/gold/schema.yml**

```yaml
version: 2

models:
  - name: dim_generator
    description: "Generator dimension — one row per DUID"
    columns:
      - name: duid
        description: "Dispatchable Unit Identifier (primary key)"
        tests:
          - not_null
          - unique
      - name: technology_type
        description: "Generator technology type"
        tests:
          - not_null
      - name: region
        description: "NEM region code"
        tests:
          - accepted_values:
              values: ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']
```

**Step 3: Run and test**

```bash
dbt run --profiles-dir profiles/ --select dim_generator
dbt test --profiles-dir profiles/ --select dim_generator
```

Expected: one row per DUID, tests pass

**Step 4: Commit**

```bash
git add models/gold/dim_generator.sql models/gold/schema.yml
git commit -m "feat: add dim_generator gold model"
```

---

## Task 9: Write Gold model — fct_regional_emissions_intensity

**Files:**
- Create: `models/gold/fct_regional_emissions_intensity.sql`
- Modify: `models/gold/schema.yml`

**Step 1: Create models/gold/fct_regional_emissions_intensity.sql**

```sql
-- models/gold/fct_regional_emissions_intensity.sql
-- Regional emissions intensity per 5-minute dispatch interval.
--
-- Calculation:
--   total_tCO2e = sum(scada_mw * (5/60) * emission_factor_tCO2e_MWh)
--   intensity   = (total_tCO2e / total_generation_mwh) * 1000  [gCO2eq/kWh]
--
-- Only intervals where total_generation_mwh > 0 are included.

with dispatch as (
    select * from {{ ref('silver_dispatch_interval') }}
    where not dq_flag
),

generators as (
    select * from {{ ref('dim_generator') }}
),

factors as (
    select * from {{ ref('silver_emissions_factor') }}
),

-- Join dispatch readings to generator metadata and emissions factors
joined as (
    select
        d.settlement_date,
        g.region,
        d.scada_mw,
        -- Energy generated this interval in MWh (5-minute window)
        d.scada_mw * (5.0 / 60.0)                         as generation_mwh,
        -- CO2e emitted this interval in tonnes
        d.scada_mw * (5.0 / 60.0) * coalesce(f.emission_factor_tCO2e_MWh, 0) as tCO2e
    from dispatch d
    inner join generators g on d.duid = g.duid
    left join factors f on g.technology_type_key = f.technology_type_key
    where g.region is not null
),

-- Aggregate to region × interval
aggregated as (
    select
        settlement_date,
        region,
        sum(generation_mwh)  as total_generation_mwh,
        sum(tCO2e)           as total_tCO2e
    from joined
    group by settlement_date, region
),

final as (
    select
        settlement_date,
        region,
        round(total_generation_mwh, 4) as total_generation_mwh,
        round(total_tCO2e, 6)          as total_tCO2e,
        -- Intensity in gCO2eq/kWh (tonnes/MWh * 1000 = g/kWh)
        round((total_tCO2e / nullif(total_generation_mwh, 0)) * 1000, 2)
            as emissions_intensity_gCO2eq_per_kWh
    from aggregated
    where total_generation_mwh > 0
)

select * from final
```

**Step 2: Append to models/gold/schema.yml**

```yaml
  - name: fct_regional_emissions_intensity
    description: >
      Regional grid emissions intensity per 5-minute dispatch interval.
      Primary analytical output for Scope 2 reporting and dashboard consumption.
    columns:
      - name: settlement_date
        description: "Dispatch interval end timestamp"
        tests:
          - not_null
      - name: region
        description: "NEM region code"
        tests:
          - not_null
          - accepted_values:
              values: ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']
      - name: total_generation_mwh
        description: "Total generation in MWh across all generators in the region this interval"
        tests:
          - not_null
      - name: total_tCO2e
        description: "Total CO2-equivalent emissions in tonnes for this region-interval"
        tests:
          - not_null
      - name: emissions_intensity_gCO2eq_per_kWh
        description: "Grid emissions intensity in gCO2eq/kWh (location-based Scope 2)"
        tests:
          - not_null
```

**Step 3: Run and test**

```bash
dbt run --profiles-dir profiles/ --select fct_regional_emissions_intensity
dbt test --profiles-dir profiles/ --select fct_regional_emissions_intensity
```

Expected: one row per (settlement_date, region) where generation > 0, tests pass

**Step 4: Commit**

```bash
git add models/gold/fct_regional_emissions_intensity.sql models/gold/schema.yml
git commit -m "feat: add fct_regional_emissions_intensity gold model"
```

---

## Task 10: Add singular test for plausible emissions intensity

**Files:**
- Create: `tests/assert_emissions_intensity_plausible.sql`

**Step 1: Create the singular test**

```sql
-- tests/assert_emissions_intensity_plausible.sql
-- Fails if any regional intensity row is outside the plausible physical range.
-- NEM grid intensity has historically ranged from near 0 (100% renewables)
-- to ~1000 gCO2eq/kWh (heavy coal states). 1500 is a hard upper bound.

select
    settlement_date,
    region,
    emissions_intensity_gCO2eq_per_kWh
from {{ ref('fct_regional_emissions_intensity') }}
where emissions_intensity_gCO2eq_per_kWh < 0
   or emissions_intensity_gCO2eq_per_kWh > 1500
```

A singular test returns rows that represent failures. Zero rows = test passes.

**Step 2: Run the test**

```bash
dbt test --profiles-dir profiles/ --select fct_regional_emissions_intensity
```

Expected: 0 failures

**Step 3: Run the full dbt build end-to-end**

```bash
python ingestion/bronze_writer.py && dbt build --profiles-dir profiles/
```

Expected: all models run OK, all tests pass

**Step 4: Commit**

```bash
git add tests/assert_emissions_intensity_plausible.sql
git commit -m "test: add singular test for emissions intensity plausible range"
```

---

## Task 11: Add GitHub Actions CI workflow

**Files:**
- Create: `.github/workflows/dbt-build.yml`

**Step 1: Create .github/workflows/dbt-build.yml**

```yaml
name: dbt Build and Test

on:
  push:
    branches: [main]
  schedule:
    # Daily at 06:10 UTC — after the SCADA fetch window settles
    - cron: "10 6 * * *"
  workflow_dispatch:

jobs:
  dbt-build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Write Bronze Parquet
        run: python ingestion/bronze_writer.py

      - name: dbt build (run + test)
        run: dbt build --profiles-dir profiles/
```

Note: `nem.duckdb` is gitignored and rebuilt fresh every run. No artifact upload needed — the CI run proves the pipeline works from source.

**Step 2: Commit and push**

```bash
git add .github/workflows/dbt-build.yml
git commit -m "ci: add dbt build and test GitHub Actions workflow"
git push
```

**Step 3: Verify the workflow runs on GitHub**

Check Actions tab. Expected: green run with all dbt tests passing.

---

## Task 12: Update documentation

**Files:**
- Modify: `docs/repo-roadmap.md` — mark Work Package 3 complete
- Modify: `README.md` — update Running locally section

**Step 1: Add dbt usage to README Running locally section**

Under `## Running locally`, add after the existing content:

```markdown
### Run the dbt pipeline (Bronze → Silver → Gold)

Install dbt dependencies (included in requirements.txt):

```bash
pip install -r requirements.txt
```

Build Bronze Parquet files from raw CSVs:

```bash
python ingestion/bronze_writer.py
```

Run all dbt models and tests:

```bash
dbt build --profiles-dir profiles/
```

Explore the compiled DAG and docs:

```bash
dbt docs generate --profiles-dir profiles/
dbt docs serve
```
```

**Step 2: Mark Work Package 3 complete in roadmap**

In `docs/repo-roadmap.md`, change `### Work package 3 — In progress` to `### Work package 3 — Complete`.

**Step 3: Commit**

```bash
git add README.md docs/repo-roadmap.md
git commit -m "docs: update README and roadmap for completed dbt/DuckDB work package"
```

---

## Verification Checklist

Before declaring this work package complete:

- [ ] `python ingestion/bronze_writer.py` runs without error and produces three `.parquet` files in `data/bronze/`
- [ ] `dbt build --profiles-dir profiles/` runs all 5 models and all tests green
- [ ] `nem.duckdb` is in `.gitignore` (not committed)
- [ ] `data/bronze/` is in `.gitignore` (not committed)
- [ ] GitHub Actions `dbt-build.yml` workflow passes on push to main
- [ ] `dbt docs generate` produces a browsable DAG showing the Bronze → Silver → Gold lineage
