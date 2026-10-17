# ADR 0029: Expose Prometheus-Compatible Metrics With Optional Observability Stack

- Status: Accepted
- Date: 2026-05-31
- Implementation: Implemented

## Context

The project has selected structured logs, health/readiness endpoints, and metrics as the observability baseline. The next decision is the metrics format and whether Prometheus/Grafana should be part of the default local deployment.

## Options Considered

### Option A: Logs only

- Pros: Simplest.
- Cons: Insufficient for production-grade observability.

### Option B: Custom JSON metrics endpoints

- Pros: Easy to implement.
- Cons: Non-standard and less compatible with existing tools.

### Option C: Prometheus-compatible metrics endpoints

- Pros: Standard, language-neutral, and works across Python and Node services.
- Cons: Requires metric naming and label discipline.

### Option D: Include a full Prometheus/Grafana stack by default

- Pros: Impressive and useful.
- Cons: Adds moving parts before the core service stack stabilizes.

## Decision

Use **Option C: Prometheus-compatible `/metrics` endpoints**, with Prometheus/Grafana added later through an optional Compose profile rather than the default stack.

## Rationale

Prometheus-compatible metrics are production-friendly and easy to expose from both Python and TypeScript services. Keeping Prometheus/Grafana out of the default stack avoids unnecessary operational weight while leaving a clean path to dashboards.

## Metrics Scope

The following services should expose metrics where practical:

- `twin-core`
- `api`
- `modbus-gateway`
- `ingestion`

Database metrics may be added later with a PostgreSQL exporter.

## Naming Convention

Metrics should use the prefix:

```text
well_sim_
```

Example metrics:

```text
well_sim_twin_model_tick_duration_seconds
well_sim_twin_simulation_lag_seconds
well_sim_twin_active_wellheads
well_sim_twin_forecast_runs_total
well_sim_twin_scenario_runs_total
well_sim_twin_control_commands_total
well_sim_ingestion_modbus_reads_total
well_sim_ingestion_modbus_read_errors_total
well_sim_ingestion_records_inserted_total
well_sim_ingestion_batch_duration_seconds
well_sim_api_requests_total
well_sim_api_request_duration_seconds
well_sim_api_errors_total
```

## Compose Profile

Prometheus and Grafana may later be added through an optional profile:

```bash
docker compose --profile observability up
```

## Consequences

- Services should expose `/metrics` where practical.
- Metrics should avoid high-cardinality labels.
- Dashboards can be added after core service metrics stabilize.
- Default local setup remains lightweight.


## Amendment: optional stack delivered on 2026-10-17

Add Prometheus and Grafana through the `observability` Compose profile, with
pinned official images, separate persistent volumes and loopback host ports.
Provision one Service health dashboard from versioned files. Keep the normal
well stack independent of these tools and their credentials.

Keep the existing `api_` and `wellhead_` metrics as published interfaces. New
readiness and timestamp metrics use `well_sim_`; recording rules join them into
consistent service status and elapsed-time queries. A successful scrape alone
does not establish readiness. Missing, stale or future observations stay unknown.

Grafana has a separate local administrator password, initialized from the ignored
environment file. It cannot change well controls or alarm state. The design,
limits and verification are in [Optional service metrics](../design/observability.md).
Startup and recovery instructions are in the [metrics runbook](../../observability/README.md).
