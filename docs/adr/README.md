# Architecture Decision Records

Architecture Decision Records document important technical and product decisions.

Each ADR should answer:

- What decision are we making?
- What options did we consider?
- Why did we choose this option?
- What are the consequences?
- What would make us revisit the decision?

## Status Values

- `Proposed`
- `Accepted`
- `Superseded`
- `Deprecated`

## Index

- `0001-project-positioning.md` — position as monitoring simulation before digital twin.
- `0002-lightweight-digital-twin-architecture.md` — use layered lightweight twin architecture.
- `0003-stateful-process-model.md` — make the process model the twin core.
- `0004-industrial-interface.md` — keep Modbus TCP as the industrial edge interface.
- `0005-data-historian.md` — use PostgreSQL/TimescaleDB as the historian.
- `0006-schema-management.md` — use SQL migrations as the canonical schema source.
- `0007-control-and-what-if-boundaries.md` — separate live control from what-if scenarios.
- `0008-production-deployment-baseline.md` — containerized service baseline.
- `0009-model-fidelity.md` — start with a lightweight deterministic model.
- `0010-model-runtime.md` — run the twin core as a separate Python service.
- `0011-twin-core-communication.md` — use internal HTTP plus durable database persistence.
- `0012-state-storage.md` — keep active twin state in memory with durable snapshots.
- `0013-prediction-strategy.md` — start with on-demand forecasts and evolve to hybrid forecasting.
- `0014-scenario-execution.md` — run what-if scenarios against isolated cloned state.
- `0015-control-and-safety.md` — restrict live control to actuator-like variables.
- `0016-api-contract.md` — use REST APIs documented with OpenAPI.
- `0017-observability-baseline.md` — use structured logs, health checks, and metrics.
- `0018-deployment-target.md` — use Docker Compose as the first production-grade deployment target.
- `0019-model-equations.md` — use a reduced-order mechanistic wellhead model.
- `0020-model-tick-semantics.md` — use fixed timestep model updates.
- `0021-model-parameterization.md` — use version-controlled defaults with database overrides.
- `0022-recovery-semantics.md` — restore from snapshots and mark recovery gaps.
- `0023-default-parameter-set.md` — use documented synthetic engineering defaults.
- `0024-twin-persistence-schema.md` — use relational twin metadata with time-series run outputs.
- `0025-forecast-horizon-confidence.md` — use short operational forecasts with heuristic confidence.
- `0026-retention-and-compression.md` — use tiered retention and TimescaleDB compression.
- `0027-historical-branching.md` — stage historical scenario branching from model snapshots.
- `0028-api-specification-workflow.md` — use per-service OpenAPI specs committed to documentation.
- `0029-metrics-stack.md` — expose Prometheus-compatible metrics with optional observability stack.
- `0030-portfolio-deployment-scope.md` — target a self-contained portfolio deployment.
- `0031-portfolio-demo-narrative.md` — optimize the demo for fast reviewer understanding.
