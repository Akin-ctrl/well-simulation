# ADR 0021: Use Version-Controlled Defaults With Database Overrides

- Status: Accepted
- Date: 2026-05-31

## Context

The reduced-order mechanistic model needs parameters such as productivity index, choke coefficient, downstream pressure, pressure gain coefficient, thermal response rate, water cut drift rate, and degradation coefficients.

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

