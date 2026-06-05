# Wellhead Monitoring Simulation

This project is a fullstack industrial wellhead monitoring simulation. It generates synthetic wellhead telemetry, exposes readings through a Modbus TCP gateway, stores time-series measurements in PostgreSQL/TimescaleDB, evaluates alarm rules, and provides the foundation for a React/TypeScript operational dashboard.

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
- TypeScript API and React dashboard foundation.
- Docker-based local orchestration.

## Target Direction

The target system is a lightweight wellhead digital twin with:

- Stateful process simulation per wellhead.
- Control inputs such as choke position, valve state, and pump state.
- Prediction and what-if simulation APIs.
- Historical comparison between observed and predicted behavior.
- Production-grade deployment, observability, security, and documentation.

## Documentation

- `docs/README.md` — documentation index.
- `docs/architecture/overview.md` — current and target architecture.
- `docs/architecture/production-readiness.md` — production-readiness checklist.
- `docs/adr/README.md` — architecture decision records.
- `docs/openapi/README.md` — public and planned internal API contracts.

## Important Status Note

The existing implementation still needs integration work before it should be treated as production-ready. Known gaps include end-to-end Docker smoke testing, missing twin-core implementation, incomplete production observability, and no automated CI contract validation yet.
