# NEM Emissions Intensity Pipeline

## Business framing

Australia's mandatory climate disclosure regime (Scope 2 reporting under AASB S2) makes granular electricity-emissions data a real business need.

Many businesses are investing into an internal product, or paying suppliers for products that can calculate their emissions from captured metering data.

### Sources:<br>
https://www.corrs.com.au/insights/australias-data-centre-boom-needs-an-energy-plan

https://www.theguardian.com/environment/2026/mar/02/datacentres-australia-power-prices-water-supply-emissions

https://www.aemo.com.au/-/media/files/stakeholder_consultation/consultations/nem-consultations/2024/2025-iasr-scenarios/final-docs/oxford-economics-australia-data-centre-energy-consumption-report.pdf

##  Analytical question:

> How clean or dirty was the NEM, by region and interval, and what does that imply for corporate Scope 2 reporting, renewable timing, and operational load-shifting?

This repo intentionally separates two products:

- `Disclosure-grade emissions analytics`
  Focused on defensible interval-level calculations using generator dispatch, metadata, and emissions factors.
- `Historical context and transition narrative`
  Focused on long-run market context, planning signals, and scenario storytelling.

## Data Engineering

This data engineering project is designed as the portfolio version of the data platform that Australian energy employers, retailers, consultancies, and sustainability teams are building:<br>

**What it does:**
- GitHub Actions fetches AEMO dispatch data every 5 minutes
- Appends to CSV
- Joins generator unit fuel type from AEMO Generation Information
- Applies DCCEEW emissions factors to calculate emissions by fuel type
- Streamlit dashboard visualises live grid emissions intensity

**Data sources:**
- [AEMO Dispatch SCADA](https://nemweb.com.au/Reports/Current/Dispatch_SCADA/)
- [AEMO Generation Information](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information)
- [DCCEEW National Greenhouse Accounts Factors](https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors)

## Architecture

```mermaid
flowchart LR
    A["AEMO NEMWeb<br/>Dispatch SCADA"] --> B["Bronze<br/>Raw daily ZIP / CSV landing"]
    C["AEMO generator metadata<br/>DUID, region, technology"] --> B
    D["NGA Factors / AEMO CDEII inputs<br/>Emissions factors"] --> B
    E["AEMO price / regionsum / rooftop PV<br/>(planned)"] --> B

    B --> F["Silver<br/>Typed, deduplicated, conformed interval tables<br/>with dq flags"]
    F --> G["Gold<br/>Regional intensity, renewable share,<br/>daily summaries, scenario tables"]

    G --> H["Streamlit dashboard<br/>Live grid + historical context + outlook"]
    G --> I["Business-ready datasets<br/>Scope 2 / market analysis"]
    F --> J["dbt tests / quality checks"]
    B --> K["Lineage + replayability"]
```

## Data Usage

The project uses different history horizons for different purposes:

- `FY25-26 operational backfill`
  Raw daily Dispatch SCADA archive ingestion for the current reporting period.
- `Disclosure-grade emissions history`
  Defensible interval-level model from the NGER actual-data era onward.
- `1998+ market context`
  Aggregated long-run context series used for narrative and trend framing, kept separate from disclosure-grade emissions facts.

## Data Lineage

- [AEMO NEM overview](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/about-the-national-electricity-market-nem)
- [AEMO Electricity data](https://www.aemo.com.au/en/energy-systems/market-it-systems/electricity-system-guides/electricity-data)
- [AEMO Historical data help](https://markets-portal-help.docs.public.aemo.com.au/Content/InformationSystems/Electricity/HistoricalData.htm)
- [AEMO Aggregated data](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data)

## Architecture decisions

The key design decisions are documented in:

- [ADR-0001: Use a Medallion architecture for AEMO market data](/Users/tanjimislam/PycharmProjects/realtime_energy_dashboard/docs/adr/0001-medallion-architecture.md)
- [ADR-0002: Use incremental batch ingestion instead of streaming](/Users/tanjimislam/PycharmProjects/realtime_energy_dashboard/docs/adr/0002-batch-over-streaming.md)
- [ADR-0003: Use Parquet and DuckDB for local development](/Users/tanjimislam/PycharmProjects/realtime_energy_dashboard/docs/adr/0003-parquet-duckdb-local-dev.md)
- [ADR-0004: Separate disclosure-grade emissions history from 1998+ market context](/Users/tanjimislam/PycharmProjects/realtime_energy_dashboard/docs/adr/0004-history-boundary-and-comparability.md)



