# ADR 0024: Use Relational Twin Metadata With Time-Series Run Outputs

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Milestone 7 tables complete; later tables remain planned

## Context

The existing database stores asset metadata, parameter readings, alarm rules, and alarm events. The lightweight digital twin needs additional persistence for model definitions, per-well configuration, model parameters, state snapshots, control commands, forecasts, scenarios, and recovery events.

The schema must support operational querying while remaining flexible enough for the model to evolve.

## Options Considered

### Option A: Store all twin data as JSONB blobs

- Pros: Flexible and fast to evolve.
- Cons: Weak queryability, weaker constraints, and less useful for dashboards.

### Option B: Fully normalize every model field

- Pros: Strong constraints and SQL queryability.
- Cons: Brittle; every model change requires schema churn.

### Option C: Use explicit columns for high-value fields plus JSONB for full state and flexible payloads

- Pros: Good balance of queryability, flexibility, and evolvability.
- Cons: Requires discipline to decide which fields deserve columns.

## Decision

Use **Option C: explicit columns for commonly queried fields plus JSONB for complete model state, inputs, and outputs**.

## Rationale

The dashboard and analytics layer need efficient access to high-value fields such as pressure, flow, temperature, water cut, model status, alarm risk, and timestamps. At the same time, the model will evolve, and storing the full state/payload in JSONB prevents unnecessary schema changes for every added variable.

## Required Tables

### `twin_model_definition`

Stores model identity and lifecycle metadata.

Suggested fields:

- `model_id`
- `name`
- `version`
- `description`
- `status`
- `created_at`

### `wellhead_model_config`

Assigns a model version to a wellhead.

Suggested fields:

- `config_id`
- `wellhead_id`
- `model_id`
- `enabled`
- `created_at`
- `updated_at`

### `wellhead_model_parameter`

Stores per-well model parameter overrides.

Suggested fields:

- `parameter_id`
- `wellhead_id`
- `model_id`
- `key`
- `value`
- `unit`
- `source`
- `valid_from`
- `created_at`

### `model_state_snapshot`

Stores time-indexed internal model state. This should be a TimescaleDB hypertable.

Suggested fields:

- `snapshot_id`
- `timestamp_utc`
- `wellhead_id`
- `model_id`
- `status`
- `state_json`
- `tubing_pressure`
- `casing_pressure`
- `flow_rate`
- `temperature`
- `water_cut`
- `choke_position`
- `blockage_factor`
- `corrosion_factor`

### `control_command`

Stores audited live control commands.

Suggested fields:

- `command_id`
- `wellhead_id`
- `issued_by`
- `command_type`
- `payload_json`
- `status`
- `reason`
- `issued_at`
- `applied_at`

### `prediction_run`

Stores forecast run metadata.

Suggested fields:

- `prediction_run_id`
- `wellhead_id`
- `model_id`
- `started_at`
- `base_timestamp`
- `horizon_seconds`
- `step_seconds`
- `status`
- `input_json`

### `prediction_point`

Stores time-indexed forecast outputs. This should be a TimescaleDB hypertable.

Suggested fields:

- `prediction_point_id`
- `prediction_run_id`
- `timestamp_utc`
- `wellhead_id`
- `values_json`
- `tubing_pressure`
- `flow_rate`
- `temperature`
- `water_cut`
- `alarm_risk`

### `scenario_run`

Stores what-if scenario metadata.

Suggested fields:

- `scenario_run_id`
- `wellhead_id`
- `model_id`
- `created_by`
- `name`
- `base_timestamp`
- `horizon_seconds`
- `step_seconds`
- `input_json`
- `status`
- `created_at`

### `scenario_point`

Stores time-indexed what-if outputs. This should be a TimescaleDB hypertable.

Suggested fields mirror `prediction_point` with a `scenario_run_id` foreign key.

### `twin_recovery_event`

Stores restart, recovery, and downtime-gap events.

Suggested fields:

- `event_id`
- `wellhead_id`
- `event_type`
- `last_snapshot_at`
- `recovered_at`
- `gap_seconds`
- `status`
- `details_json`

## Consequences

- SQL migrations should define the canonical schema.
- Drizzle schema should mirror these tables for typed API access.
- Time-indexed model outputs should use TimescaleDB hypertables.
- JSONB payloads should not replace high-value query columns.
- Retention and compression policies still need separate decisions.


## Amendment 2026-10-03: Milestone 7 creates only the tables it writes

### The problem

This record lists nine tables. In Milestone 7 only three would be written: the
state snapshots, the recovery events, and the per-well parameter overrides. The
prediction and scenario tables would stay empty until Milestone 8. The control
table would stay empty with no milestone planned to write it, because no
milestone built the control API. Phase 3.9 of the repair work removed database
objects that nothing used, for this reason.

### Decision

- Milestone 7 creates `twin_state_snapshot`, `twin_recovery_event`, and
  `wellhead_model_parameter`. Their columns are in the
  [Milestone 7 design](../design/twin-core-milestone-7.md).
- `prediction_run`, `prediction_point`, `scenario_run`, and `scenario_point`
  arrive in Milestone 8, with the code that writes them.
- `control_command` arrives with the live control API, in a new milestone after
  Milestone 8.
- `twin_model_definition` and `wellhead_model_config` are not created while the
  code runs one model version. Snapshots, recovery events, overrides, and
  readings each record their model version as text, which a definition table
  can join to later.
- The snapshot table names its columns with units, such as
  `tubing_pressure_psi`, following the reading and model payload conventions,
  rather than the bare names suggested above.

The suggested tables above remain the intended shape for later milestones.

## Implementation on 2026-10-12

Migration 0010 creates only the three Milestone 7 tables listed in the
amendment. Snapshots and recovery events are TimescaleDB hypertables. The
snapshot stores queryable pressure, flow, temperature, damage, and status
columns next to the complete JSONB process state. The Drizzle schema mirrors
these three tables. Forecast, scenario, and control tables remain for their
own milestones.
