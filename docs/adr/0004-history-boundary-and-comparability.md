# ADR-0004: Separate disclosure-grade emissions history from 1998+ market context

## Status

Accepted

## Context

The NEM began in 1998, and long-run market context is useful for the project narrative. However, interval-level emissions-intensity comparability is constrained by:

- source availability
- metadata evolution
- emissions factor methodology changes
- structural market changes

The repo should not imply that all years carry the same analytical confidence.

## Decision

Model two distinct historical layers:

- `disclosure-grade emissions analytics`
- `long-run aggregated market context`

Use the former for auditable business-facing calculations and the latter for narrative/history views.

## Rationale

- avoids overstating comparability
- keeps the business case honest
- supports both portfolio storytelling and engineering rigor

## Consequences

Positive:

- clearer semantic boundaries
- better trust and documentation
- easier source governance

Negative:

- more than one historical pathway to explain
- slightly more complex dashboard and README wording

