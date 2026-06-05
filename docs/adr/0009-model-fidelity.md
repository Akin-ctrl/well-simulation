# ADR 0009: Start With A Lightweight Deterministic Model

- Status: Accepted
- Date: 2026-05-31

## Context

The existing simulator generates synthetic wellhead readings. To become a lightweight digital twin, the system needs internal state, causal behavior, and control response. It should not overclaim high-fidelity reservoir simulation.

## Options Considered

### Option A: Continue random independent telemetry

- Pros: Easy to maintain; already close to current implementation.
- Cons: Not a digital twin; no memory, causality, control response, or prediction value.

### Option B: Build high-fidelity reservoir/process physics

- Pros: More realistic and engineering-heavy.
- Cons: Too complex for this project stage; requires domain data, validation, and specialist assumptions.

### Option C: Start with a deterministic rule/equation-based model

- Pros: Explainable, testable, demo-friendly, and enough to show operational twin behavior.
- Cons: Simplified physics; must be documented honestly.

### Option D: Start with machine learning

- Pros: Could learn complex patterns from real data later.
- Cons: Premature without real historical data; less explainable for a first twin.

## Decision

Use **Option C: a deterministic, stateful, rule/equation-based operational model** as the first digital twin model.

## Rationale

The first twin should demonstrate state, causality, alarms, forecasts, scenarios, and operator decisions without pretending to be a reservoir simulator. A lightweight deterministic model is the clearest way to make the system credible and testable.

## Initial Model Scope

The first version should model:

- `reservoir_pressure_proxy`
- `tubing_pressure`
- `casing_pressure`
- `flow_rate`
- `choke_position`
- `pump_status`
- `master_valve_status`
- `wing_valve_status`
- `temperature`
- `water_cut`
- `blockage_factor`
- `corrosion_factor`

## Consequences

- Random telemetry should be replaced or wrapped by model-driven telemetry.
- Process outputs such as pressure and flow should emerge from model state and controls.
- Model equations and assumptions must be documented.
- Tests should verify basic control-response behavior.

