# ADR-0001: Use a Medallion architecture for AEMO market data

## Status

Accepted

## Context

The project joins multiple AEMO and related source types:

- raw dispatch data
- generator metadata
- emissions factors
- later, price, region summary, and rooftop PV

These inputs arrive in different formats, with different update cadences, and with correction risk over time.

The repo also serves different consumers:

- engineering / reproducibility
- business analytics
- dashboard presentation

## Decision

Use a Medallion pattern:

- Bronze for raw landed source data
- Silver for typed, deduplicated, conformed interval tables with dq flags
- Gold for business-facing facts and dimensions

## Rationale

- preserves auditability and replayability
- handles AEMO-specific cleaning in one layer instead of many
- supports both dashboard and analytical outputs
- maps directly to the Databricks / Delta / dbt vocabulary used by Australian energy employers

## Consequences

Positive:

- cleaner lineage
- easier testing
- clearer trust boundaries
- easier migration to cloud lakehouse patterns later

Negative:

- more structure than a single-script ETL
- requires discipline in table ownership and naming

