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

## Key Boundary

The project should not claim to be a high-fidelity reservoir simulator. The target is a **lightweight operational digital twin**: useful for demonstrating state, causality, prediction, alarms, and decision support.

