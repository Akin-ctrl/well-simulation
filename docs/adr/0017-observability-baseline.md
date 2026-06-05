# ADR 0017: Use Structured Logs, Health Checks, And Metrics As The Observability Baseline

- Status: Accepted
- Date: 2026-05-31

## Context

The production-grade system must make service health, telemetry freshness, ingestion status, API behavior, forecasts, scenarios, and controls observable.

## Options Considered

### Option A: Plain print/log statements

- Pros: Simple.
- Cons: Weak debugging and poor production signal.

### Option B: Structured logs plus health checks

- Pros: Strong baseline.
- Cons: Limited performance and pipeline visibility.

### Option C: Structured logs, health/readiness endpoints, and metrics

- Pros: Production-grade baseline without too much complexity.
- Cons: Requires metrics discipline.

### Option D: Full OpenTelemetry tracing from day one

- Pros: Excellent distributed visibility.
- Cons: Premature until service boundaries stabilize.

## Decision

Use **Option C: structured logs, health/readiness endpoints, and service-level metrics**.

## Rationale

This gives enough operational visibility to run and debug the system while avoiding the complexity of full distributed tracing too early.

## Consequences

- Services should log structured events to stdout/stderr.
- API and service processes should expose health/readiness endpoints.
- Metrics should cover model ticks, Modbus reads, ingestion writes, API errors, forecasts, scenarios, and controls.
- OpenTelemetry tracing is deferred.

