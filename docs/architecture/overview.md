# Architecture Overview

## Current System

The current system is a simulated industrial monitoring pipeline:

```text
Twin-core model
  -> read-only fleet snapshot
  -> Modbus TCP gateway
  -> Database ingestion service
  -> PostgreSQL/TimescaleDB historian
  -> SQL alarm rules and analytics views
  -> TypeScript API and React dashboard
```

Twin-core reads active assets and calculates a deterministic process state for
each well on a fixed one-second step. The gateway reads one complete fleet
snapshot and serves all 18 seeded model signals as Modbus registers.
Ingestion polls the gateway and stores source-labelled readings. The seven
diagnostic signals added in model version 0.3.0 remain synthetic estimates.
Twin-core also saves model state in TimescaleDB and restores compatible state
after a restart. Recovery decisions and wall-clock jumps are recorded there.

The [Milestone 4 design](../design/twin-core-milestone-4.md) explains the
service boundary and data quality rules. The [Milestone 5 design](../design/twin-core-milestone-5.md)
explains the original process equations, units, and limits. The
[reference-well design](../design/reference-well-physics.md) explains the
additional phase and diagnostic equations. The
[Milestone 6 design](../design/twin-core-milestone-6.md) describes the Modbus
mapping and freshness rules. The [Milestone 7 design](../design/twin-core-milestone-7.md)
describes snapshots, parameter overrides, and restart recovery. The
[Milestone 8 design](../design/twin-core-milestone-8.md) describes saved forecasts
and isolated scenarios. The [8B design](../design/twin-core-milestone-8b.md)
describes live synthetic controls and parameter publication.

## Current Dashboard Layer

The dashboard currently exposes the monitoring foundation:

- fleet-level overview for all simulated wellheads
- latest recorded telemetry snapshots, which may be partial after a read failure
- active alarms from database alarm evaluation
- analytics charts for pressure, temperature, flow, water cut, and GOR
- per-wellhead detail pages with current readings, alarms, and trends
- an Alarm Center with severity, threshold, age, and links to wellheads

Ingestion rejects a failed or mixed-time Modbus poll before inserting a batch.
The API can still show partial or stale latest views during source failure,
metadata changes, or while older readings expire.

## Lightweight Digital Twin

The running architecture puts twin-core between asset metadata and telemetry
output. It also serves bounded forecasts and isolated scenarios on request:

```text
Asset metadata
  -> Twin model registry
  -> Stateful wellhead process model
  -> Simulated telemetry
  -> Historian
  -> Twin-core forecast and scenario runner
  -> Authenticated API
  -> Operational dashboard
```

## Runtime Components

- **Twin core**: maintains current process state for each wellhead.
- **Process model**: updates pressure, flow, temperature, valve state, pump state, and degradation over time.
- **Telemetry adapter**: converts supported model fields into Modbus-readable register values.
- **Historian**: stores source-labelled readings, alarm events, model state
  snapshots, recovery events, and saved forecast and scenario runs.
- **Forecast and scenario API**: copies current model state, advances the same
  one-second model privately, and saves complete runs. It never applies a live
  control command. See [Milestone 8](../design/twin-core-milestone-8.md).
- **Dashboard**: shows live state, historical trends, and alarms. Forecast and
  scenario views belong to Milestone 9.
- **Live control API**: saves an operator draft and explicit activation. Twin-core
  checks current authority and applies the durable targets before a model tick.
  Active control rows take precedence over older snapshots during recovery.
- **Parameter publication API**: validates administrator proposals through the
  Python resolver and writes an attributed override. A restart loads it into
  the running model.

## Digital Twin Evolution Path

The project becomes a credible lightweight digital twin by adding capabilities in this order:

1. **Explainable monitoring**: overview, detail pages, alarm center, trend explorer, and data freshness.
2. **Stateful process model**: deterministic wellhead state that evolves over fixed timesteps.
3. **Observed vs simulated comparison**: store both measured telemetry and model-estimated state.
4. **Divergence detection**: flag when observed readings move away from expected model behavior.
5. **Forecasting**: short-horizon predictions with confidence and clear assumptions.
6. **What-if scenarios**: cloned-state simulations that do not mutate live state.

## Key Boundary

The project should not claim to be a high-fidelity reservoir simulator. The target is a **lightweight operational digital twin**: useful for demonstrating state, causality, prediction, alarms, and decision support.

## Diagrams

- [Data model](../relevant_visuals/data-model.pdf): the
  relational schema, showing the asset hierarchy, parameter metadata, and the
  time-series tables.
- [Flow diagram](../relevant_visuals/flow-diagram.pdf): the older random simulator path to the dashboard.

These predate the current schema. The complete definition in `data/sql/schema.sql` is
canonical where the two disagree. The [schema design](../design/consolidated-schema.md)
explains installation, structural checks, and adoption of existing data.
