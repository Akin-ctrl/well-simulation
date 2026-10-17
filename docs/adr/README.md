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

Status is whether the decision was accepted. Implementation is whether the
code does it yet. They are different questions, and only one of them was
previously written down.

| ADR | Decision | Implementation |
| --- | --- | --- |
| [0001](0001-project-positioning.md) | Position The Project As A Wellhead Monitoring Simulation Before Digital Twin | Implemented |
| [0002](0002-lightweight-digital-twin-architecture.md) | Use A Layered Lightweight Digital Twin Architecture | Partial |
| [0003](0003-stateful-process-model.md) | Make A Stateful Process Model The Twin Core | Implemented |
| [0004](0004-industrial-interface.md) | Keep Modbus TCP As The Industrial Edge Interface | Implemented |
| [0005](0005-data-historian.md) | Use PostgreSQL And TimescaleDB As The Historian (amended) | Implemented; engine recovery verified separately |
| [0006](0006-schema-management.md) | Keep SQL As The Canonical Schema Source (amended) | Implemented; single-file amendment adopted |
| [0007](0007-control-and-what-if-boundaries.md) | Separate Live Control From What-If Simulation | Separate scenario, control and parameter screens implemented |
| [0008](0008-production-deployment-baseline.md) | Use A Containerized Service Baseline | Implemented |
| [0009](0009-model-fidelity.md) | Start With A Lightweight Deterministic Model | Implemented |
| [0010](0010-model-runtime.md) | Run The Twin Core As A Separate Python Service | Implemented |
| [0011](0011-twin-core-communication.md) | Use Internal HTTP Plus Durable Database Persistence | Implemented |
| [0012](0012-state-storage.md) | Keep Active Twin State In Memory With Durable Snapshots | Implemented |
| [0013](0013-prediction-strategy.md) | Start With On-Demand Forecasts And Evolve To Hybrid Forecasting | On-demand implemented |
| [0014](0014-scenario-execution.md) | Run What-If Scenarios Against Isolated Cloned State | Implemented |
| [0015](0015-control-and-safety.md) | Restrict Live Control To Actuator-Like Variables | Implemented |
| [0016](0016-api-contract.md) | Use REST APIs Documented With OpenAPI | Implemented |
| [0017](0017-observability-baseline.md) | Use Structured Logs, Health Checks, And Metrics As The Observability Baseline (amended) | Implemented; dashboard readiness includes its API path |
| [0018](0018-deployment-target.md) | Use Docker Compose As The First Production-Grade Deployment Target | Implemented |
| [0019](0019-model-equations.md) | Use A Reduced-Order Mechanistic Wellhead Model | Implemented |
| [0020](0020-model-tick-semantics.md) | Use Fixed Timestep Model Updates | Implemented |
| [0021](0021-model-parameterization.md) | Use Version-Controlled Defaults With Database Overrides | Implemented |
| [0022](0022-recovery-semantics.md) | Restore From Snapshots And Mark Recovery Gaps | Implemented |
| [0023](0023-default-parameter-set.md) | Use Documented Synthetic Engineering Defaults | Implemented |
| [0024](0024-twin-persistence-schema.md) | Use Relational Twin Metadata With Time-Series Run Outputs | Implemented |
| [0025](0025-forecast-horizon-confidence.md) | Use Short Operational Forecasts With Heuristic Confidence | Implemented |
| [0026](0026-retention-and-compression.md) | Use Tiered Retention And TimescaleDB Compression | Implemented |
| [0027](0027-historical-branching.md) | Stage Historical Scenario Branching From Model Snapshots | Implemented |
| [0028](0028-api-specification-workflow.md) | Use Per-Service OpenAPI Specs Committed To Documentation | Implemented |
| [0029](0029-metrics-stack.md) | Expose Prometheus-Compatible Metrics With Optional Observability Stack | Partial |
| [0030](0030-portfolio-deployment-scope.md) | Target A Self-Contained Portfolio Deployment | Implemented |
| [0031](0031-portfolio-demo-narrative.md) | Optimize The Demo For Fast Reviewer Understanding (amended) | Partial; workspace implemented, final portfolio delivery pending |
| [0032](0032-alarm-center-scope.md) | Implement Active Alarm Inspection Before Alarm Lifecycle Workflows | Implemented |
| [0033](0033-role-model-and-access-control.md) | Separate Operating Authority From Administrative Authority | Implemented |
| [0034](0034-audit-trail-scope.md) | Record An Audit Trail For Actors, Not For Auditors | Implemented |
| [0035](0035-metadata-reload.md) | How Metadata Changes Reach A Running Service | Implemented |
| [0036](0036-model-telemetry-through-modbus.md) | Send Model Telemetry Through The Existing Modbus Path | Implemented |
| [0037](0037-session-revocation.md) | Logout Ends Every Session Of The User, On The Server | Implemented |
| [0038](0038-live-control-command-lifecycle.md) | Apply Live Model Controls Through Durable, Reviewed Commands | Implemented |

Of 38 decisions: 32 implemented, 5 partial, and 1 not started.
The partial group includes ADR 0007: API and scenario UI separation are complete,
while live command forms remain deferred.

The twin-core process model now feeds the Modbus gateway and historian.
Durable model state, forecasts, isolated scenarios, live synthetic controls,
and authenticated parameter publication are implemented.
