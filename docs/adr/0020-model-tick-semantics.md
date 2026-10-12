# ADR 0020: Use Fixed Timestep Model Updates

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Partial

## Context

The twin-core service needs a clear time advancement strategy. The same model logic should support live simulation, forecasts, and what-if scenarios.

## Options Considered

### Option A: Variable timestep based on wall-clock elapsed time

- Pros: Naturally reflects elapsed runtime.
- Cons: Can create unstable jumps after pauses, delays, or restarts; harder to test.

### Option B: Fixed timestep

- Pros: Deterministic, testable, stable, and reusable for forecasts and scenarios.
- Cons: Requires explicit lag and catch-up handling.

### Option C: Event-driven updates only

- Pros: Efficient when nothing changes.
- Cons: Poor fit for continuous process dynamics.

## Decision

Use **Option B: fixed timestep model updates**.

## Rationale

Fixed timestep updates make the model easier to test, explain, and reuse. The same step function can drive live simulation, forecasts, and what-if scenarios.

## Initial Timing Policy

- Internal model timestep: `1 second`.
- Telemetry emission cadence: `5 seconds`.
- Model state snapshot cadence: `30 seconds`.
- Forecast and scenario simulations use the same fixed-step model function.

## Lag Handling

The service must not apply one large elapsed-time update after a pause. Instead, it should use bounded catch-up:

- run at most a configured number of catch-up steps per loop
- record lag metrics when behind schedule
- resume safely if lag exceeds the catch-up budget

## Consequences

- Model behavior becomes deterministic for tests and replay.
- Runtime metrics should include simulation lag.
- Forecast and scenario outputs can be compared directly with live model behavior.

## Implementation on 2026-09-29

The service runs a one-second fixed scheduler with bounded catch-up, skipped-tick logging, and lag metrics. No physical model step, model telemetry emission, or state snapshot is produced yet.

## Implementation on 2026-09-29

The fixed scheduler now executes a pure one-second process step for each active well. Each skipped tick is counted per well and never replayed as a large step. Telemetry emission and durable snapshots remain later milestones.

## Amendment 2026-10-03: Tick time follows the wall clock when it jumps

### The problem

The scheduler counts time with the monotonic clock, so a wall-clock change
cannot move its deadlines. Tick times were then worked out as the UTC time at
startup plus monotonic time elapsed since startup. On 2026-10-03 the host slept
for about 9 hours 15 minutes. The monotonic clock stood still for that time and
the wall clock did not. Every tick after waking carried a time 33,291 seconds
in the past. The Modbus gateway rejected every snapshot as stale, so no reading
was stored, while twin-core still reported ready, because its readiness check
used the same paused clock.

### Decision

Scheduling stays on the monotonic clock. Before each scheduler pass,
`twin_core/runtime.py` compares tick time with the wall clock using
`reconcile_clock` in `twin_core/scheduler.py`:

- Within 2 seconds either way: ordinary jitter, nothing changes.
- Wall clock ahead by more than 2 seconds: the model did not run for that time.
  Each whole second is counted as a skipped tick on every well, the
  `well_sim_twin_clock_adjustments_total{direction="forward"}` metric goes up,
  and tick time is moved onto the wall clock. The model then takes one ordinary
  one-second step. It never takes one step the length of the missing time.
- Wall clock behind by more than 2 seconds: tick times must never go back, or
  the model refuses the step. Ticks hold until the wall clock passes the last
  tick. Holding is waiting, not missed time, so no gap is recorded.

Readiness also compares the last tick with the wall clock. It reports
`tick_clock_drift` when they are more than 3 seconds apart, and
`wall_clock_moved_back` while ticks are held.

**Do not stamp ticks from the monotonic clock alone again.** It cannot see a
machine that slept.

### Consequences

- A host that sleeps or has its clock corrected recovers without a restart, and
  the missing time appears as a recorded gap instead of hidden stale times.
- Two extra clock reads per scheduler pass.
- A wall clock that is wrong and then corrected forward shows up as a gap even
  though the model did run. That is the honest reading of what the wall clock
  says, and it is visible in the metric and the log.
