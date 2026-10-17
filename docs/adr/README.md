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
- `Accepted, amended`: a later section changes or extends the decision
- `Accepted, corrected`: a later section corrects the original decision
- `Superseded`
- `Deprecated`

## Index

Status is whether the decision was accepted. Implementation is whether the
code does it yet. Read the latest amendment for the current decision; older
context and dated implementation notes remain historical evidence.

| ADR | Decision | Status | Implementation |
| --- | --- | --- | --- |
| [0001](0001-project-positioning.md) | Position The Project As A Wellhead Monitoring Simulation Before Digital Twin | Accepted, amended | Implemented |
| [0002](0002-lightweight-digital-twin-architecture.md) | Use A Layered Lightweight Digital Twin Architecture | Accepted | Implemented for the synthetic reference well |
| [0003](0003-stateful-process-model.md) | Make A Stateful Process Model The Twin Core | Accepted | Implemented |
| [0004](0004-industrial-interface.md) | Keep Modbus TCP As The Industrial Edge Interface | Accepted | Implemented |
| [0005](0005-data-historian.md) | Use PostgreSQL And TimescaleDB As The Historian | Accepted, amended | Implemented; engine recovery verified separately |
| [0006](0006-schema-management.md) | Keep SQL As The Canonical Schema Source | Accepted, amended | Implemented; single-file amendment adopted |
| [0007](0007-control-and-what-if-boundaries.md) | Separate Live Control From What-If Simulation | Accepted, amended | Separate scenario, control and parameter screens implemented |
| [0008](0008-production-deployment-baseline.md) | Use A Containerized Service Baseline | Accepted | Implemented |
| [0009](0009-model-fidelity.md) | Start With A Lightweight Deterministic Model | Accepted | Implemented |
| [0010](0010-model-runtime.md) | Run The Twin Core As A Separate Python Service | Accepted | Implemented |
| [0011](0011-twin-core-communication.md) | Use Internal HTTP Plus Durable Database Persistence | Accepted | Implemented |
| [0012](0012-state-storage.md) | Keep Active Twin State In Memory With Durable Snapshots | Accepted | Implemented |
| [0013](0013-prediction-strategy.md) | Start With On-Demand Forecasts And Evolve To Hybrid Forecasting | Accepted, amended | On-demand and scheduled forecasts implemented |
| [0014](0014-scenario-execution.md) | Run What-If Scenarios Against Isolated Cloned State | Accepted, amended | Implemented |
| [0015](0015-control-and-safety.md) | Restrict Live Control To Actuator-Like Variables | Accepted | Implemented |
| [0016](0016-api-contract.md) | Use REST APIs Documented With OpenAPI | Accepted | Implemented |
| [0017](0017-observability-baseline.md) | Use Structured Logs, Health Checks, And Metrics As The Observability Baseline | Accepted, amended | Implemented; dashboard readiness includes its API path |
| [0018](0018-deployment-target.md) | Use Docker Compose As The First Production-Grade Deployment Target | Accepted | Implemented |
| [0019](0019-model-equations.md) | Use A Reduced-Order Mechanistic Wellhead Model | Accepted, amended | Implemented |
| [0020](0020-model-tick-semantics.md) | Use Fixed Timestep Model Updates | Accepted, amended | Implemented |
| [0021](0021-model-parameterization.md) | Use Version-Controlled Defaults With Database Overrides | Accepted, amended | Implemented |
| [0022](0022-recovery-semantics.md) | Restore From Snapshots And Mark Recovery Gaps | Accepted, amended | Implemented |
| [0023](0023-default-parameter-set.md) | Use Documented Synthetic Engineering Defaults | Accepted, amended | Implemented |
| [0024](0024-twin-persistence-schema.md) | Use Relational Twin Metadata With Time-Series Run Outputs | Accepted, amended | Implemented |
| [0025](0025-forecast-horizon-confidence.md) | Use Short Operational Forecasts With Heuristic Confidence | Accepted | Implemented |
| [0026](0026-retention-and-compression.md) | Use Tiered Retention And TimescaleDB Compression | Accepted, amended | Implemented |
| [0027](0027-historical-branching.md) | Stage Historical Scenario Branching From Model Snapshots | Accepted, amended | Implemented |
| [0028](0028-api-specification-workflow.md) | Use Per-Service OpenAPI Specs Committed To Documentation | Accepted | Implemented |
| [0029](0029-metrics-stack.md) | Expose Prometheus-Compatible Metrics With Optional Observability Stack | Accepted, amended | Implemented |
| [0030](0030-portfolio-deployment-scope.md) | Target A Self-Contained Portfolio Deployment | Accepted | Implemented |
| [0031](0031-portfolio-demo-narrative.md) | Optimize The Demo For Fast Reviewer Understanding | Accepted, amended | Partial; workspace implemented, final portfolio delivery pending |
| [0032](0032-alarm-center-scope.md) | Implement Active Alarm Inspection Before Alarm Lifecycle Workflows | Accepted, amended | Implemented |
| [0033](0033-role-model-and-access-control.md) | Separate Operating Authority From Administrative Authority | Accepted | Implemented |
| [0034](0034-audit-trail-scope.md) | Record An Audit Trail For Actors, Not For Auditors | Accepted, amended | Implemented |
| [0035](0035-metadata-reload.md) | How Metadata Changes Reach A Running Service | Accepted, amended | Implemented |
| [0036](0036-model-telemetry-through-modbus.md) | Send Model Telemetry Through The Existing Modbus Path | Accepted, amended | Implemented |
| [0037](0037-session-revocation.md) | Logout Ends Every Session Of The User, On The Server | Accepted | Implemented |
| [0038](0038-live-control-command-lifecycle.md) | Apply Live Model Controls Through Durable, Reviewed Commands | Accepted, amended | Implemented |
| [0039](0039-alarm-operator-actions.md) | Record Acknowledgement and Timed Shelving Separately From Alarm Conditions | Accepted | Implemented |

Of 39 decisions, 38 have their agreed local implementation. ADR 0031 retains
external portfolio publication and contributor-consent gates.

The twin-core process model now feeds the Modbus gateway and historian.
Durable model state, forecasts, isolated scenarios, live synthetic controls,
and authenticated parameter publication are implemented.
