# ADR-0002: Use incremental batch ingestion instead of streaming

## Status

Accepted

## Context

The source data updates every five minutes, which can make streaming seem attractive. But the project is aimed at:

- analytical use cases
- Scope 2 reporting support
- dashboard monitoring
- a portfolio repo that must stay explainable and cheap to run

## Decision

Use incremental batch ingestion with a five-minute polling cadence rather than a streaming architecture.

## Rationale

- AEMO data is naturally published in report files and archive pages
- the business use case is not millisecond-sensitive
- GitHub Actions and simple schedulers can support near-real-time refresh
- batch keeps the repo comprehensible and avoids resume-padding architecture

## Consequences

Positive:

- cheaper to run
- simpler operational model
- easier local development and testing
- better fit for a solo portfolio project

Negative:

- not true real-time
- event-driven alerting would require a later architectural upgrade

