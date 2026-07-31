# Wellhead Monitoring Simulation

This project is a fullstack industrial wellhead monitoring simulation. It generates synthetic wellhead telemetry, exposes readings through a Modbus TCP gateway, stores time-series measurements in PostgreSQL/TimescaleDB, evaluates alarm rules, and serves a React/TypeScript operational dashboard.

The project is **not yet a digital twin**. Today it is a SCADA-style simulation and data acquisition platform. The next major evolution is to turn the random telemetry generator into a lightweight, stateful process model that can track wellhead state, respond to control inputs, run what-if scenarios, and forecast future conditions.

## Why This Exists

Industrial digital twin systems need more than dashboards. They need a reliable telemetry pipeline, asset metadata, historical storage, alarm logic, operational interfaces, and a process model that can explain and predict behavior.

This repository currently implements the telemetry and monitoring foundation. The documentation in `docs/` records the decisions required to evolve it into a production-grade lightweight digital twin.

## Current Capabilities

- Metadata-driven wellhead and parameter configuration.
- Synthetic wellhead telemetry generation.
- Modbus TCP gateway for industrial protocol simulation.
- TimescaleDB/PostgreSQL historian for time-series readings.
- PostgreSQL alarm rules and alarm event generation.
- TypeScript API with OpenAPI documentation.
- React dashboard with overview, analytics, and per-wellhead detail views.
- Docker-based local orchestration.

## Current Dashboard Scope

The dashboard currently focuses on credible monitoring rather than pretending to be a finished digital twin:

- Fleet overview for all simulated wellheads.
- Current readings grouped by wellhead and parameter.
- Pressure, temperature, flow, water cut, and GOR trend views.
- Per-wellhead detail pages with active alarms and parameter history.
- Active alarm data from the backend.

The next dashboard slice is an **Alarm Center**: a dedicated operational page for active alarms, severity counts, affected assets, threshold context, alarm age, and links back to the impacted wellhead. Acknowledge and shelving workflows are intentionally out of scope until the project has an alarm lifecycle model.

## Target Direction

The target system is a lightweight wellhead digital twin with:

- Stateful process simulation per wellhead.
- Control inputs such as choke position, valve state, and pump state.
- Prediction and what-if simulation APIs.
- Historical comparison between observed and predicted behavior.
- Production-grade deployment, observability, security, and documentation.

## Near-Term Build Order

1. Add the Alarm Center using the existing alarm rules and event data.
2. Add a Trend Explorer with wellhead, parameter, and time-window filters.
3. Add sensor/data freshness indicators for ingestion credibility.
4. Document simulation assumptions directly in the app and docs.
5. Add the twin gap layer: observed vs simulated state and divergence.
6. Add bounded what-if scenarios after the stateful model exists.

## Documentation

- `docs/README.md` — documentation index.
- `docs/architecture/overview.md` — current and target architecture.
- `docs/architecture/production-readiness.md` — production-readiness checklist.
- `docs/adr/README.md` — architecture decision records.
- `docs/openapi/README.md` — public and planned internal API contracts.

## Important Status Note

The local Docker stack has been smoke-tested, and the API/dashboard contract is documented under `docs/openapi`. The project is still not production-ready: the twin core is not implemented, observability is incomplete, and automated CI contract validation still needs to be added.
