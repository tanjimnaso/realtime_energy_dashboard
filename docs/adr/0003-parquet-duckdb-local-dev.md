# ADR-0003: Use Parquet and DuckDB for local development

## Status

Accepted

## Context

The target production-shaped architecture should map to the Azure Databricks and lakehouse patterns common in Australian energy, but this repo must remain:

- cheap
- portable
- fast for local iteration
- usable without managed cloud infrastructure

## Decision

Use file-based storage with Parquet and local analytical querying with DuckDB for the structured data platform layer.

## Rationale

- Parquet is columnar, compact, and lakehouse-compatible
- DuckDB is simple, fast, and ideal for local development
- dbt Core can work against DuckDB cleanly
- the design can later migrate to Databricks + Delta without changing the conceptual model

## Consequences

Positive:

- good local developer experience
- low infrastructure cost
- platform-neutral data layout

Negative:

- no built-in enterprise governance
- concurrency, permissions, and multi-user workflows are limited locally

