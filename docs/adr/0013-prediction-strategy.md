# ADR 0013: Start With On-Demand Forecasts And Evolve To Hybrid Forecasting

- Status: Accepted
- Date: 2026-05-31
- Implementation: On-demand and scheduled forecasts implemented

## Context

A digital twin should forecast future state. Forecasts can be generated only when requested, continuously on a schedule, or both.

## Options Considered

### Option A: On-demand forecasts only

- Pros: Simple; useful for operator-driven exploration.
- Cons: Does not automatically surface developing risk.

### Option B: Scheduled forecasts only

- Pros: Good for dashboards and continuous risk monitoring.
- Cons: Higher compute/storage load and more moving parts.

### Option C: Hybrid forecasts

- Pros: Supports both automatic monitoring and operator-driven questions.
- Cons: More complexity than either mode alone.

## Decision

Start with **on-demand forecasts**, then evolve to **hybrid forecasting** with scheduled short-horizon forecasts for active wells.

## Rationale

On-demand forecasting is the fastest way to validate model behavior. Scheduled forecasts can be added once the model and persistence design are stable.

## Consequences

- First API should support request-driven forecasts with horizon and step size.
- Forecast outputs should be persisted.
- Later scheduled forecasts should populate dashboard-ready latest predictions.

## Implementation on 2026-10-13

Milestone 8 adds an on-demand forecast route in twin-core and an authenticated
public API route. A request copies current running state and controls, advances
the existing one-second model for a bounded horizon, and saves the full run and
sampled points. There is no background forecast scheduler yet.


## Amendment 2026-10-17: Add owned, bounded schedules

### Context

The owner approved scheduled forecasts after the dashboard, operating screens and
historical scenario work. The existing model and saved-run boundary now support
short automatic forecasts without changing live controls.

### Decision

Keep the on-demand workflow and add one administrator-owned schedule per well.
Schedules start disabled, use intervals of 5–1440 minutes and forecast 5–30 minutes
ahead. A separate worker claims one occurrence at a time, skips missed intervals
and shares the existing projection capacity. It rechecks active administrator
authority before claiming and before saving. Logout does not cancel owned schedules.

Save the forecast, its points and job completion in one transaction. Expired or
interrupted claims fail without automatic replay. A saved result identifies the
schedule, job and due time. Readers see the latest attempt separately from the
latest completed result. Settings changes require a reason, confirmation and
current revision, with an atomic audit event.

### Consequences

The earlier on-demand-only implementation remains part of the history. The current
implementation is hybrid. Schedules add database and calculation load, bounded by
configuration limits, one worker and shared projection slots. They do not provide
field calibration or guaranteed risk detection. See the [design and verification](../design/scheduled-forecasts.md).
