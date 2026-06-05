# ADR 0020: Use Fixed Timestep Model Updates

- Status: Accepted
- Date: 2026-05-31

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

