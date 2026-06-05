# ADR 0003: Make A Stateful Process Model The Twin Core

- Status: Accepted
- Date: 2026-05-31

## Context

The current simulator produces synthetic readings. A digital twin requires internal state and causal behavior across time.

## Options Considered

### Option A: Continue generating independent random readings

- Pros: Easy to maintain.
- Cons: Not a digital twin; no process memory or causal relationships.

### Option B: Train an ML model first

- Pros: Can learn complex patterns if enough data exists.
- Cons: The project has no real labeled operational dataset yet; hard to explain and validate.

### Option C: Start with a deterministic/rule-based process model

- Pros: Explainable; testable; easy to connect to controls and scenarios.
- Cons: Simplified physics; must avoid overstating accuracy.

## Decision

Start with **Option C: deterministic/rule-based stateful process model**.

## Rationale

The first twin version should prioritize clarity and causality. Each wellhead should have model state such as tubing pressure, casing pressure, flow rate, choke position, valve states, pump state, water cut, temperature, and degradation factors.

## Consequences

- `wellhead_simulator.py` should eventually be replaced or refactored into a model-driven simulator.
- Model state snapshots should be stored separately from raw telemetry.
- Unit tests should verify model response to control changes and faults.

