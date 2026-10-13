# ADR 0012: Keep Active Twin State In Memory With Durable Snapshots

- Status: Accepted
- Date: 2026-05-31
- Implementation: Complete for the single-instance synthetic model

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

## Implementation on 2026-09-29

The service keeps active lifecycle state in memory. No physical process state or durable snapshot exists yet. Snapshot recovery belongs to Milestone 7.

## Implementation on 2026-09-29

The process state now advances in memory. A restart deliberately initializes a new synthetic run and reports `cold_start_no_snapshot`. Durable snapshots remain Milestone 7 work.

## Implementation on 2026-10-12

Milestone 7 adds a TimescaleDB snapshot about every 30 model seconds and a
final copy on clean shutdown. The one-slot writer mailbox keeps database
latency off the one-second model loop. On startup, twin-core checks the latest
snapshot for each active well and restores compatible state. A rejected or
missing snapshot starts that well cold with a recorded reason.

## Implementation correction on 2026-10-12

The first shutdown path queued the final copy behind any earlier database
write and waited only 3 seconds. The final copy could remain unsaved. After the
model loop stops, twin-core now writes that copy directly through a bounded
database connection. Periodic writes still use the background writer to keep
the one-second model loop independent of database latency. A failed final write
is logged and counted, and the service exits nonzero. Recovery can still use
the latest successful periodic snapshot.
