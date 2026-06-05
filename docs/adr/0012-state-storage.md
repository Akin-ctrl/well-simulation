# ADR 0012: Keep Active Twin State In Memory With Durable Snapshots

- Status: Accepted
- Date: 2026-05-31

## Context

The twin needs fast active state updates while preserving enough durable history for recovery, auditability, and analysis.

## Options Considered

### Option A: In-memory only

- Pros: Fastest and simplest.
- Cons: State is lost on restart; weak production recovery story.

### Option B: PostgreSQL/TimescaleDB only

- Pros: Durable and simple infrastructure.
- Cons: Makes the database the hot path for every model tick.

### Option C: Redis for live state plus PostgreSQL for history

- Pros: Fast live state with durable history.
- Cons: Adds infrastructure before scaling requires it.

### Option D: In-memory live state plus PostgreSQL/TimescaleDB snapshots

- Pros: Fast, simple, recoverable, and avoids extra infrastructure.
- Cons: Possible state loss between snapshots.

## Decision

Use **Option D: active twin state in memory with periodic PostgreSQL/TimescaleDB snapshots**.

## Rationale

This is the right lightweight production path. It keeps model ticks fast while giving the system a recovery and audit trail without adding Redis prematurely.

## Consequences

- Add model state snapshot persistence.
- Define snapshot cadence and recovery behavior.
- Revisit Redis or another state store if multi-instance runtime or stricter recovery is needed.

