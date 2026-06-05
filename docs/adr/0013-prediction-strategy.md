# ADR 0013: Start With On-Demand Forecasts And Evolve To Hybrid Forecasting

- Status: Accepted
- Date: 2026-05-31

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

