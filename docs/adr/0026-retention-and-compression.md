# ADR 0026: Use Tiered Retention And TimescaleDB Compression

- Status: Accepted
- Date: 2026-05-31

## Context

The system will generate raw readings, alarm events, model state snapshots, prediction points, scenario points, control commands, and recovery events. These data classes have different volume, value, and audit requirements.

Keeping everything forever is not production-grade. Expiring everything quickly would weaken operational history and auditability.

## Options Considered

### Option A: Keep everything forever

- Pros: Simple and avoids accidental data loss.
- Cons: Unbounded growth and poor lifecycle management.

### Option B: Use short retention for everything

- Pros: Minimizes storage.
- Cons: Loses important audit and operational history.

### Option C: Use tiered retention by data value and volume

- Pros: Practical, production-grade, and preserves high-value records longer.
- Cons: Requires explicit policy management.

## Decision

Use **Option C: tiered retention and TimescaleDB compression policies**.

## Default Policy

| Data | Retention | Compress After | Reason |
| --- | ---: | ---: | --- |
| Raw readings | `180 days` | `7 days` | Core historian data |
| Alarm events | `2 years` | `30 days` | Operational audit |
| Model state snapshots | `90 days` | `7 days` | Recovery and model analysis |
| Prediction points | `30 days` | `3 days` | Forecast points become stale |
| Scenario points | `90 days` | `7 days` | Useful for review and demonstration |
| Control commands | `3 years` | optional | Long-lived audit trail |
| Recovery events | `2 years` | optional | Reliability audit |

## Rationale

High-volume time-series data should be compressed and expired earlier. Audit-oriented data such as control commands, alarms, and recovery events should live longer because it explains operational decisions and system behavior.

## Environment Overrides

Retention should be configurable by deployment environment:

- local/demo environments may use shorter retention
- production environments may use longer retention
- regulated environments may require policy-specific retention

## Consequences

- SQL migrations should include TimescaleDB compression and retention policy setup where appropriate.
- Scenario metadata may outlive detailed scenario points.
- Operators should understand that forecast points are not permanent historical truth.
- Retention policies must be documented in deployment docs.

