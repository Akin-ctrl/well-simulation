# ADR 0014: Run What-If Scenarios Against Isolated Cloned State

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Current and verified historical snapshot scenarios implemented

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

## Implementation on 2026-10-13

Milestone 8 copies the current state, controls, and parameters under one registry
lock. It applies the requested controls only to a private model run. An unchanged
private baseline runs beside it, and both sets of points are saved in one
transaction. The live registry never receives the scenario controls. Starting
from a historical snapshot remains deferred under ADR 0027.

## Amendment 2026-10-17: Historical scenarios are implemented

The deferral in the dated note above ended with the historical-scenario work.
A caller can now select an exact retained snapshot using its ID and simulated
time. Twin-core checks the model version, parameter fingerprint and physical
state, then creates private scenario and unchanged-baseline copies. An invalid
selection never falls back to current state.

The saved result records its starting-state identity. It uses alarm rules active
at execution; it does not reconstruct past rule settings. Chart retention follows
simulated time. Current-state scenarios keep their existing behavior.

**Good.** Users can reopen an attributed comparison from an earlier model state.

**Costs.** Snapshot compatibility and exact timestamp handling add validation.

**Accepted risks.** Parameter changes can make old snapshots incompatible, and
historical points can expire sooner than points from a current-state scenario.

See [ADR 0027](0027-historical-branching.md) for the amended boundary and
[historical scenarios](../design/historical-scenarios.md) for design and verification.

