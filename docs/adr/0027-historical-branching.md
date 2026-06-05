# ADR 0027: Stage Historical Scenario Branching From Model Snapshots

- Status: Accepted
- Date: 2026-05-31

## Context

What-if scenarios can start from the current live state or from a past state. Historical branching enables incident review and alternate-action analysis, but it depends on reliable persisted model snapshots.

## Options Considered

### Option A: Current state only

- Pros: Simplest and enough for the first version.
- Cons: Does not support post-incident review or historical alternate-action analysis.

### Option B: Historical branching from model snapshots

- Pros: Powerful and feasible because the system persists internal model state.
- Cons: Depends on snapshot quality, cadence, and retention.

### Option C: Historical branching from raw readings

- Pros: Uses historian data directly.
- Cons: Raw readings do not contain full internal model state, making reconstruction harder.

### Option D: Full replay engine

- Pros: Strongest reconstruction capability.
- Cons: Too complex for this stage.

## Decision

Use a staged approach:

1. Version 1 scenarios branch from current live state.
2. Version 2 scenarios may branch from persisted `model_state_snapshot` records.
3. Raw telemetry replay and full reconstruction are deferred.

## Rationale

Current-state scenarios are enough to ship the first digital twin capability. Historical snapshot branching is a credible next step because it builds on the planned state snapshot persistence without requiring a full replay engine.

## Requirements For Historical Branching

Historical scenario runs should record:

- `base_timestamp`
- `base_snapshot_id`
- `branch_type`

The UI and API must clearly show whether a scenario starts from current state or historical state. If no suitable snapshot exists near the requested timestamp, the system should reject the request or ask the user to choose another timestamp.

## Consequences

- Snapshot cadence and retention affect future branching quality.
- Historical records must never be mutated by scenario execution.
- Incident-review workflows can be added after current-state scenarios are stable.

