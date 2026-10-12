# ADR 0021: Use Version-Controlled Defaults With Database Overrides

- Status: Accepted, amended
- Date: 2026-05-31
- Implementation: Complete for the current synthetic model

## Context

The reduced-order mechanistic model needs a set of parameters. These include productivity index, choke coefficient, downstream pressure, pressure gain coefficient, thermal response rate, water cut drift rate, and degradation coefficients.

These values must be explicit, reviewable, tunable, and validated. Hardcoded magic numbers would make the model less credible and harder to operate.

## Options Considered

### Option A: Hardcode parameters in Python

- Pros: Fastest implementation path.
- Cons: Not production-grade; hard to tune, audit, or explain.

### Option B: Use config files only

- Pros: Versionable and easy to review.
- Cons: Awkward for per-well overrides and runtime inspection.

### Option C: Store all parameters in the database

- Pros: Strong per-well tuning and runtime visibility.
- Cons: Defaults become less visible in version control; bootstrap becomes heavier.

### Option D: Use version-controlled defaults with database-backed overrides

- Pros: Safe defaults in code/config plus tunable per-well overrides.
- Cons: Requires clear merge precedence and validation rules.

## Decision

Use **Option D: version-controlled defaults with database-backed per-well overrides**.

## Rationale

The model needs safe defaults that reviewers can inspect, while still supporting realistic per-well behavior and future calibration. Database overrides allow tuning without code changes, while version-controlled defaults keep the model understandable and reproducible.

## Parameter Loading Order

The twin-core service should load parameters in this order:

1. built-in version-controlled defaults
2. environment or config-file overrides
3. database-backed per-well overrides

Later sources override earlier sources only if validation succeeds.

## Parameter Metadata

Each model parameter should define:

- key
- description
- unit
- default value
- valid range
- whether it can change live
- whether it requires model restart

## Suggested Tables

- `twin_model_definition`
  - model name, version, description, status
- `wellhead_model_config`
  - wellhead id, model version, enabled status
- `wellhead_model_parameter`
  - wellhead id, parameter key, value, unit, source, valid-from timestamp
- `model_parameter_audit`
  - optional future audit trail for parameter changes

## Consequences

- No unexplained model constants should be scattered through model logic.
- Parameter validation must run before model execution.
- Invalid per-well parameters should prevent that wellhead model from running or mark it unavailable.
- Future calibration can write proposed parameter updates without changing model code.

## Implementation on 2026-09-29

Versioned defaults and validated global overrides load now. A typed per-well override source is ready, but the database table and adapter belong to Milestone 7.

## Implementation on 2026-09-29

The model now consumes validated typed parameters. Stable per-well variation is applied before global and future database overrides. Runtime parameter changes are rejected with `restart_required`. The database override table remains Milestone 7 work.

## Amendment 2026-10-03: Overrides are edited by SQL until the control milestone

### The problem

Milestone 7 builds the per-well override table and its loader. Something has to
write the rows. An API in the TypeScript service would either copy the valid
ranges from `twin_core/model_defaults.json`, so the rule would live in two
places, or ask twin-core to validate through a new internal write endpoint.
That endpoint is the same kind of authenticated, audited write path the live
control API needs.

### Decision

- The table is `wellhead_model_parameter`, defined in the
  [Milestone 7 design](../design/twin-core-milestone-7.md). Rows are only ever
  added. A change is a new row; a row with `value` NULL returns the parameter
  to its default. Each row records `source`, `changed_by`, and `reason`.
- An administrator edits it with SQL. There is no API or screen in Milestone 7.
- Twin-core loads the rows on every fleet refresh and validates them with the
  existing resolver: known key, matching unit, named source, value in range.
  An invalid value marks only that well unavailable. A changed value on a
  running well marks it `restart_required`, so live behaviour never changes
  silently.

### Accepted risks

- `changed_by` and `reason` are typed by hand. They are a record, not a
  verified actor, so parameter changes are outside the audit trail of
  [ADR 0034](0034-audit-trail-scope.md) until the control milestone builds an
  authenticated write path.
- A wrong value is caught when twin-core loads it, not when it is written. The
  well shows as unavailable instead of the write being refused.

Calibration, the reason overrides exist, has no real well data to work from
yet, so overrides are expected to be rare until then.

## Implementation on 2026-10-12

Migration 0010 adds the append-only `wellhead_model_parameter` table. Twin-core
loads the newest effective row for each well and key during fleet refresh.
A NULL value removes the per-well override and returns that key to the lower
precedence environment value or model default.
The existing resolver checks key, unit, source, and numeric range. Changed
parameters on a running well require a restart. SQL examples and the limits of
hand-entered actor names are in [the data guide](../../data/README.md).
