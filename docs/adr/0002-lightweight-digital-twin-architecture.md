# ADR 0002: Use A Layered Lightweight Digital Twin Architecture

- Status: Accepted
- Date: 2026-05-31

## Context

The project needs a path from telemetry simulation to a lightweight digital twin without becoming an unrealistic high-fidelity reservoir simulator.

## Options Considered

### Option A: Keep the current random telemetry generator

- Pros: Simple; easy to run.
- Cons: No causality, internal state, prediction, or control response.

### Option B: Build a high-fidelity physics simulator

- Pros: More realistic; closer to advanced engineering simulation.
- Cons: Too complex for this repo; requires domain data and specialist validation.

### Option C: Build a layered lightweight twin

- Pros: Adds state, causality, control, prediction, and what-if behavior while staying understandable.
- Cons: Lower fidelity than a specialist reservoir/process simulator.

## Decision

Adopt **Option C: layered lightweight twin architecture**.

## Rationale

The goal is to demonstrate industrial digital twin concepts in a production-grade software system. A lightweight model is enough to show asset state, dynamic behavior, alarms, predictions, and scenarios without pretending to solve reservoir engineering.

## Consequences

- The system will include a dedicated twin/model layer.
- Model fidelity must be documented honestly.
- The dashboard should show modeled state and predictions separately from raw readings.

