# ADR 0005: Use PostgreSQL And TimescaleDB As The Historian

- Status: Accepted
- Date: 2026-05-31

## Context

The project already uses PostgreSQL/TimescaleDB for time-series readings, metadata, views, and alarm events.

## Options Considered

### Option A: Use plain PostgreSQL only

- Pros: Simpler dependency footprint.
- Cons: Weaker time-series ergonomics and continuous aggregate support.

### Option B: Use PostgreSQL with TimescaleDB

- Pros: Strong relational model plus time-series features; already implemented.
- Cons: Requires TimescaleDB extension and compatible deployment image.

### Option C: Use a separate time-series database

- Pros: Could provide specialized performance.
- Cons: Adds operational complexity and fragments relational metadata from readings.

## Decision

Continue using **PostgreSQL with TimescaleDB**.

## Rationale

The twin needs asset metadata, readings, alarms, model state snapshots, predictions, and scenario results. PostgreSQL/TimescaleDB supports this mix well and keeps the stack understandable.

## Consequences

- Time-series schema design remains central.
- Continuous aggregates should be treated as part of migration-managed schema.
- Data retention, compression, and indexing strategies must be documented before production.

