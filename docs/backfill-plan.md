# Backfill Plan

## Goal

Create a defensible historical foundation for three different analytical needs:

1. `Operational monitoring`
2. `Disclosure-grade emissions analytics`
3. `Long-run market context`

These are related, but they are not the same dataset and should not be presented as if they have the same comparability standard.

## Planning principles

- Use the most granular and auditable source available for emissions analytics.
- Preserve raw data exactly as received before conformance.
- Keep `1998+ market context` separate from `disclosure-grade interval emissions`.
- Prefer incremental batch over heroic one-off reprocessing.

## Backfill phases

### Phase 1: FY25-26 operational backfill

Purpose:

- fill the current reporting-year history needed by the dashboard
- make the current repo immediately useful

Sources:

- AEMO `Dispatch_SCADA` archive
- current metadata and emissions factors already in the repo

Actions:

1. Pull daily Dispatch SCADA archives from `2025-07-01` onward.
2. Persist raw daily files into Bronze.
3. Normalize into monthly partitions for current app compatibility.
4. Continue incremental five-minute refresh for the current day.

Deliverables:

- complete FY25-26 raw landing set
- monthly SCADA partitions
- stable live/current-day snapshot

### Phase 2: disclosure-grade historical emissions layer

Purpose:

- produce the defensible emissions-intensity history for business reporting analytics

Boundary:

- anchor comparability from the NGER actual-data era onward
- earlier years can exist, but should not automatically inherit the same disclosure-grade claim

Actions:

1. Backfill all available granular dispatch history that can be sourced reliably.
2. Version emissions factors by effective period and provenance.
3. Add dq flags for:
   - dummy or reserve-trader DUIDs
   - missing SCADA
   - negative or implausible generation
   - suspicious joins to metadata
4. Build Silver and Gold models for regional intensity.

Deliverables:

- conformed interval fact table
- regional intensity Gold model
- quality checks and comparability notes

### Phase 3: 1998+ market context

Purpose:

- provide long-run narrative context for managers, researchers, and the dashboard

Sources:

- AEMO aggregated market history
- other clearly labeled aggregate context datasets if needed

Actions:

1. Backfill long-run aggregated series to the start of the NEM where possible.
2. Store these as separate context tables.
3. Label clearly in the app and README that these are context/history views, not the same as interval-level disclosure-grade emissions facts.

Deliverables:

- long-run annual / FY trend tables
- context charts for 5Y, 10Y, MAX, and market-transition storytelling

## Suggested source-to-layer mapping

### Bronze

- raw `Dispatch_SCADA` ZIP or extracted CSV
- raw metadata files
- raw emissions factor inputs
- raw aggregated context series

### Silver

- `silver_dispatch_interval`
- `silver_generator_metadata`
- `silver_emissions_factor_versioned`
- `silver_aggregated_context`

### Gold

- `fct_regional_emissions_intensity`
- `fct_daily_summary`
- `fct_historical_context`

## Recommended order of execution

1. FY25-26 backfill
2. incremental ingestion hardening
3. Bronze/Silver/Gold models for interval emissions
4. 1998+ aggregated context layer
5. app refactor to consume Gold instead of ad hoc pandas joins

## Success criteria

- today file is reconstructed reliably from the archive path
- FY25-26 history is complete and queryable
- disclosure-grade history is clearly bounded and documented
- 1998+ context charts are available without overclaiming comparability

