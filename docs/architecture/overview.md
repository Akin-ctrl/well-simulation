# Architecture Overview

## Current System

The current system is a simulated industrial monitoring pipeline:

```text
Wellhead simulator
  -> Modbus TCP gateway
  -> Database ingestion service
  -> PostgreSQL/TimescaleDB historian
  -> SQL alarm rules and analytics views
  -> TypeScript API and React dashboard foundation
```

The current simulator generates synthetic telemetry. It does not yet maintain a physics-aware or behavior-aware model of the wellhead.

## Current Dashboard Layer

The dashboard currently exposes the monitoring foundation:

- fleet-level overview for all simulated wellheads
- latest complete telemetry snapshots
- active alarms from database alarm evaluation
- analytics charts for pressure, temperature, flow, water cut, and GOR
- per-wellhead detail pages with current readings, alarms, and trends

The next dashboard page is an Alarm Center. It should present active alarms as evidence an operator can act on. Each alarm needs its severity, the affected wellhead, the parameter and its value, the threshold, the trigger time, and its age. Each also needs a link to the wellhead detail page.

## Target Lightweight Digital Twin

The target architecture adds a twin core between asset metadata and telemetry output:

```text
Asset metadata
  -> Twin model registry
  -> Stateful wellhead process model
  -> Simulated/observed telemetry
  -> Historian
  -> Prediction and what-if services
  -> Operational dashboard and API
```

## Target Runtime Components

- **Twin core**: maintains current process state for each wellhead.
- **Process model**: updates pressure, flow, temperature, valve state, pump state, and degradation over time.
- **Telemetry adapter**: converts model state into Modbus-readable register values.
- **Historian**: stores raw readings, model state snapshots, predictions, and alarm events.
- **Control API**: accepts bounded operational inputs.
- **Scenario API**: runs what-if simulations without mutating live state.
- **Dashboard**: shows live state, historical trends, alarms, predictions, and scenario outputs.

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
