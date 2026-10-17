# ADR 0022: Restore From Snapshots And Mark Recovery Gaps

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Complete for startup recovery and recorded clock jumps

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

The system should be honest about what it simulated. Small runtime delays can be handled with bounded catch-up. Longer downtime should be shown as a recovery gap, not replayed as though the model had been running the whole time.

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

## Implementation on 2026-09-29

The scheduler bounds catch-up and records skipped time. Snapshot restoration, recovery events, and model recovery status still need the Milestone 7 schema.

## Implementation on 2026-09-29

A new process run starts from a synthetic balance and is labelled `cold_start_no_snapshot`. Scheduler gaps increment a per-well counter without integrating missed time. Snapshot restoration and durable recovery events remain Milestone 7 work.

## Implementation on 2026-10-03

A host that sleeps is now treated as downtime. Twin-core compares tick time
with the wall clock on every scheduler pass. When the wall clock has moved
ahead, the missing time is counted as skipped ticks per well and is not
replayed, as this record requires for longer gaps. See the ADR 0020 amendment
of the same date.

## Amendment 2026-10-03: What state a well resumes from after a long gap

### The problem

The recovery flow above says to restore the latest snapshot, catch up small
gaps, and record larger ones. It does not say what the restored state should
be after a long gap. Resuming every field as saved would show pressures from
before the outage hours later. Starting from balance would throw away the
accumulated damage that snapshots exist to keep.

### Decision

- A gap of 5 seconds or less is caught up with real one-second steps.
- A longer gap is recorded as skipped ticks and a `snapshot_restored` recovery
  event. Then:
  - slow damage (water cut, corrosion, blockage) is restored exactly as saved,
    and not advanced through the gap;
  - the operating balance is re-solved with that damage and the current
    controls;
  - each fast state (tubing and casing pressure, wellhead and annulus
    temperature, pump factor) moves from its saved value toward the balance by
    $e^{-g/\tau}$, where $g$ is the gap and $\tau$ that state's time constant.
    Tubing pressure's is about 6 minutes, and the others are under 4;
  - annulus pressure follows the restored annulus temperature by the fixed-mass
    gas law, and flow and the diagnostic readings are recomputed.
- A snapshot from another model version, or made under other parameters, is not
  restored. The well cold-starts and a `snapshot_incompatible` event records why.

The equations, time constants, and their check against the running model are in
the [Milestone 7 design](../design/twin-core-milestone-7.md).

### Rejected

- **Resume exactly as saved.** Simplest, but after hours offline the fast
  states are stale values, not the state a real well would be in.
- **A fixed cut-off, such as 30 minutes.** A 25-minute gap would restore values
  that would be 98% settled in reality, then jump at 30 minutes.
- **Always re-settle fast states.** A short restart would jump visibly to
  balance.

**Do not replay the gap or advance slow damage through it.** Both would present
model history that was never computed.

## Implementation on 2026-10-12

Twin-core writes a fleet snapshot about every 30 model seconds and on clean
shutdown. At startup it checks each saved model version, parameter fingerprint,
timestamp, and process state before using it. It runs up to five real steps for
a short gap. For a longer gap it keeps saved slow damage and settles fast
states using the rule above. The well reports `recovering` until its first
new tick. Startup decisions and clock jumps enter `twin_recovery_event`.
The local Docker restart restored all 12 active wells; a real host sleep test
has not run.
