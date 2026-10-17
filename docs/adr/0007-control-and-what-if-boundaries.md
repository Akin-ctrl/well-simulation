# ADR 0007: Separate Live Control From What-If Simulation

- Status: Accepted
- Date: 2026-05-31
- Implementation: Separate scenario, live control and parameter screens implemented

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


## Implementation on 2026-10-13

Milestone 8 runs scenarios against private copies of current model state.
They cannot mutate the running model. Live control remains proposed in
[ADR 0038](0038-live-control-command-lifecycle.md).

## Implementation on 2026-10-13: Live synthetic controls

Milestone 8B adds a separate draft and activation API. Twin-core applies a
confirmed command at a model tick and restores its durable controls after a
restart. An integration test runs a scenario after a live change and confirms
that the active controls and revision remain unchanged. Dashboard controls
remain outside this milestone.

## Implementation on 2026-10-16: Dashboard analysis

Milestone 9 presents installed controls as read-only values in Monitoring and
places scenario inputs on a separate Scenarios page. Saved results show the
original baseline and schedule. Editing a new draft does not alter a saved
result. Live command entry still uses the separate draft and activation API;
this dashboard does not add live command forms.

## Amendment, 2026-10-17: separate operating views

The owner requested live control and parameter screens after the usability pass.
Controls now uses the durable proposal and activation API. Parameters uses the
administrator proposal and publication API. Both require an explicit review step.
Scenarios still run against isolated state. The [operating design](../design/operating-workspace.md)
records their layouts, outcome recovery and role restrictions.
