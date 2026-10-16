# ADR 0005: Use PostgreSQL And TimescaleDB As The Historian

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Implemented

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

## Amendment 2026-10-16: Verify the engine and its recovery path

### Context

TimescaleDB 2.19.3 failed to restore a compressed null value in the historian.
A physical backup recovered successfully, but the logical restore remained
broken. PostgreSQL 14 cannot run the TimescaleDB release containing the fix.

### Decision

Use PostgreSQL 16.15 with TimescaleDB 2.30.2, pinned by image digest in Compose
and CI. Upgrade PostgreSQL using pg_upgrade in copy mode while the extension
remains at 2.19.3, then update the extension. Rehearse on a verified backup.
Require preserved data and effective permissions, successful logical restores,
and application tests before switching the running stack to its converted volume.
The schema remains one SQL file under the amended [ADR 0006](0006-schema-management.md).

Keeping the old engine with physical backups was a valid fallback. Decompressing
all old data before a logical transfer was another option, but would rewrite
the historian and require more space. Copy-mode pg_upgrade avoids the broken
restore decoder and preserves the original data files for recovery.

### Consequences

**Good.** Backup restoration becomes a checked release condition. The original
volume remains available while the converted copy is validated.

**Costs.** The procedure needs both PostgreSQL binaries, separate data volumes,
and a pause in application writes for the final conversion.

**Accepted risks.** Returning to the old volume after new writes begin would
omit those writes. Recovery must account for them explicitly. Engine versions
remain pinned until their replacements pass the same checks.

The [engine upgrade design](../design/database-engine-upgrade.md) records the
procedure, source references, failures, and verification results.

