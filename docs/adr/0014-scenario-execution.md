# ADR 0014: Run What-If Scenarios Against Isolated Cloned State

- Status: Accepted
- Date: 2026-05-31

## Context

What-if scenarios are essential for a digital twin, but they must not mutate live twin state.

## Options Considered

### Option A: Run scenarios by mutating live state

- Pros: Simple technically.
- Cons: Unsafe and conceptually wrong; experiments alter active state.

### Option B: Clone live state, run scenario, discard results

- Pros: Safe and simple.
- Cons: Loses scenario history and auditability.

### Option C: Clone live state, run scenario, persist results

- Pros: Safe, auditable, and useful for dashboard comparison.
- Cons: Requires scenario persistence tables.

### Option D: Support branching from historical state immediately

- Pros: Powerful.
- Cons: More complex and depends on mature snapshot history.

## Decision

Use **Option C: clone live state, run scenarios in isolation, and persist scenario results**.

## Rationale

This creates a safe and production-grade boundary between experiments and the live twin. Historical branching can be added later after state snapshots are mature.

## Consequences

- Scenario execution must never mutate active state.
- Scenario metadata, inputs, and result points should be persisted.
- The UI must distinguish live state, forecasts, and scenario outputs.

