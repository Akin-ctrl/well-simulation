# ADR 0019: Use A Reduced-Order Mechanistic Wellhead Model

- Status: Accepted
- Date: 2026-05-31

## Context

The project needs a model that is realistic, credible, and explainable. A digital twin should not be driven by random telemetry or vague heuristics. At the same time, this repository should not claim to implement a high-fidelity reservoir simulator or replace specialist tools.

The model should sit in the middle: simplified enough to implement and test, but grounded in recognizable petroleum and process engineering relationships.

## Options Considered

### Option A: Simple heuristic rules

- Pros: Fast to build and easy to understand.
- Cons: Too toy-like; weak credibility for industrial/process engineering reviewers.

### Option B: Reduced-order mechanistic model

- Pros: Realistic, explainable, testable, and grounded in physical relationships.
- Cons: Requires more careful parameter design and documentation than simple heuristics.

### Option C: High-fidelity petroleum/reservoir simulation

- Pros: Highest theoretical realism.
- Cons: Out of scope; requires specialist validation, detailed subsurface data, and complex tooling.

### Option D: Data-driven or ML model

- Pros: Useful later if real historical data exists.
- Cons: Premature; not explainable enough as the first model and lacks training data.

## Decision

Use **Option B: a reduced-order mechanistic wellhead model**.

## Rationale

The model must be serious enough to support a credible lightweight digital twin narrative. It should encode cause/effect behavior using simplified but recognizable relationships:

- reservoir inflow behavior
- tubing pressure dynamics
- choke-controlled outflow
- pump and valve effects
- thermal lag
- water cut drift
- corrosion and blockage degradation

This provides a model that is explainable in documentation, testable in code, and useful for forecasting and what-if scenarios.

## Model Boundary

The model is intended for operational simulation and decision support demonstrations. It is **not** a certified reservoir engineering model and should not be described as a replacement for specialist tools such as OLGA, PIPESIM, Eclipse, or similar high-fidelity simulators.

## Version-One Model Structure

Each wellhead model should follow this update flow:

```text
control inputs
  -> actuator and constraint state
  -> reservoir inflow estimate
  -> choke/outflow estimate
  -> pressure inventory update
  -> thermal update
  -> water/degradation update
  -> telemetry output
```

## Core Relationships

### Reservoir Inflow

Use a simplified inflow performance relationship. The first implementation may use a productivity-index style equation:

```text
q_in = PI * max(reservoir_pressure_proxy - flowing_pressure_proxy, 0)
```

For future realism, this can be extended to a Vogel-style inflow relationship for solution-gas-drive behavior.

### Choke-Controlled Outflow

Use a simplified choke relationship where outflow depends on choke opening, effective valve availability, pump contribution, pressure differential, and blockage:

```text
q_out = C_choke * choke_opening_factor * valve_availability
        * pump_factor * sqrt(max(tubing_pressure - downstream_pressure, 0))
        * blockage_factor
```

The relationship should preserve the expected behavior:

- closing the choke reduces flow
- reduced outflow increases upstream pressure
- blockage reduces flow capacity
- closed master/wing valves force outflow toward zero
- emergency shutdown overrides normal outflow

### Tubing Pressure Dynamics

Use a pressure inventory balance:

```text
tubing_pressure_next = tubing_pressure
  + pressure_gain_coefficient * (q_in - q_out) * dt
  - natural_loss_coefficient * dt
```

This captures the operational behavior that pressure rises when inflow exceeds outflow and falls when outflow exceeds inflow.

### Temperature Dynamics

Use a first-order lag model toward an operating temperature target:

```text
target_temperature = ambient_temperature
  + flow_heating_coefficient * normalized_flow
  + stress_heating_coefficient * normalized_pressure

temperature_next = temperature
  + thermal_response_rate * (target_temperature - temperature) * dt
```

### Water Cut Drift

Water cut should evolve slowly rather than jump randomly:

```text
water_cut_next = water_cut
  + water_cut_drift_rate * dt
  + instability_factor * operating_stress * dt
```

### Degradation

Corrosion and blockage should evolve as slow-moving internal state. They may depend on water cut, H2S/CO2 levels, sand, velocity, and operating stress:

```text
corrosion_factor_next = corrosion_factor
  + corrosion_rate_coefficient * corrosion_risk * dt

blockage_factor_next = blockage_factor
  - blockage_growth_coefficient * solids_risk * dt
```

`blockage_factor` should be clamped to a safe range so the model remains numerically stable.

## Consequences

- Model parameters must be explicit and documented.
- Tests should verify monotonic expectations, such as closing choke reducing flow and increasing upstream pressure.
- The dashboard can explain predictions in terms of model state and controls.
- Future calibration can adjust model parameters without changing the architecture.

## Revisit If

- The project obtains real well data suitable for calibration.
- A specific well type requires a different inflow model.
- The model needs multiphase flow, nodal analysis, or detailed reservoir coupling.

