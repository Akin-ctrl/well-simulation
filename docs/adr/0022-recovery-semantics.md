# ADR 0022: Restore From Snapshots And Mark Recovery Gaps

- Status: Accepted
- Date: 2026-05-31

## Context

The twin-core service may restart, crash, or fall behind. Recovery must preserve credibility. The system should not silently invent continuous model behavior for periods where the model was not running.

## Options Considered

### Option A: Start from defaults every time

- Pros: Simple.
- Cons: Loses state and undermines the twin's continuity.

### Option B: Restore latest snapshot and replay all missed time

- Pros: Produces continuous simulated history.
- Cons: Can create fake precision for downtime periods and may cause unstable catch-up.

### Option C: Restore latest snapshot and skip missed time

- Pros: Honest about downtime.
- Cons: Creates a visible gap and needs status/event handling.

### Option D: Restore latest snapshot, use bounded catch-up for small gaps, and mark larger recovery gaps

- Pros: Credible, stable, and production-friendly.
- Cons: Requires recovery events and model status handling.

## Decision

Use **Option D: restore latest valid snapshots, use bounded catch-up for small gaps, and mark larger recovery gaps**.

## Rationale

The system should be honest about what it simulated. Small runtime delays can be handled with bounded catch-up, but larger downtime should be exposed as a recovery gap rather than silently replayed as if the model had been continuously operating.

## Recovery Flow

On startup, the twin-core service should:

1. load the latest valid model state snapshot for each wellhead
2. initialize from model config defaults if no snapshot exists
3. compare snapshot timestamp with current time
4. apply bounded catch-up only if the gap is below a configured threshold
5. record a recovery event if the gap exceeds the threshold
6. expose model status as `recovering` before returning to `running`
7. resume fixed-step simulation

## Model Status Values

The system should expose model status values such as:

- `initializing`
- `running`
- `lagging`
- `recovering`
- `degraded`
- `unavailable`

## Consequences

- Recovery events should be persisted.
- Telemetry and dashboard views should be able to show recovery gaps.
- Metrics should include recovery count and simulation lag.
- The model avoids generating fake continuity across downtime.

