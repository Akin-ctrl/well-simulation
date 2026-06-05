# ADR 0007: Separate Live Control From What-If Simulation

- Status: Accepted
- Date: 2026-05-31

## Context

A digital twin should allow users to change operational inputs and run scenarios. These are not the same thing.

Live control mutates the active simulated process. What-if simulation explores possible futures without changing live state.

## Options Considered

### Option A: Use one API for both live control and scenarios

- Pros: Fewer endpoints.
- Cons: Easy to accidentally mutate live state when running experiments.

### Option B: Separate live control APIs from scenario APIs

- Pros: Safer; clearer mental model; easier authorization and auditing.
- Cons: More API surface.

### Option C: Only support what-if scenarios

- Pros: Safe and simple.
- Cons: The live twin cannot respond to operator actions.

## Decision

Use **separate APIs and data models** for live control and what-if simulation.

## Rationale

Production-grade digital twin software should make mutation boundaries obvious. Operators and reviewers must know whether they are changing the active simulated state or running a disposable scenario.

## Consequences

- Live control commands must be audited.
- Scenario runs should have isolated inputs and outputs.
- Dashboard UI must visually distinguish live state from scenario results.

