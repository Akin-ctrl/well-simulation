# ADR 0032: Implement Active Alarm Inspection Before Alarm Lifecycle Workflows

- Status: Accepted
- Date: 2026-06-21

## Context

The dashboard needs a credible alarm experience. Industrial dashboards normally expose active alarms, severity, affected asset, trigger context, alarm age, acknowledgement state, shelving, comments, and history.

The project already evaluates alarm rules and exposes active alarm data. It does not yet have a full alarm lifecycle model for acknowledge, shelve, suppress, operator comments, or alarm return-to-normal state transitions.

## Options Considered

### Option A: Add only alarm counts to the overview

- Pros: Fast to implement.
- Cons: Too shallow for an operational dashboard and does not show why an alarm exists.

### Option B: Add a full alarm lifecycle immediately

- Pros: Closest to mature SCADA/DCS systems.
- Cons: Requires new persistence, operator action semantics, audit logging, and UI workflows before the current project needs them.

### Option C: Add an Active Alarm Center first

- Pros: Realistic, explainable, useful for reviewers, and aligned with the existing alarm data.
- Cons: Acknowledge and shelving workflows remain future work.

## Decision

Use **Option C: add an Active Alarm Center first**.

## Scope

The Alarm Center should show:

- severity totals
- affected wellhead and location
- parameter name and engineering unit
- triggering value
- threshold/operator context
- trigger timestamp
- alarm age
- link to the affected wellhead detail page

The Alarm Center should not expose acknowledge, shelving, suppression, or operator-comment controls until the backend stores those states explicitly.

## Rationale

This keeps the project honest. It improves dashboard credibility using real existing data, while avoiding fake industrial workflows that would make the project less explainable.

## Consequences

- The next dashboard feature is operationally meaningful without overclaiming maturity.
- The API may need a richer alarm DTO for threshold and asset context.
- Future alarm lifecycle work should get its own ADR before implementation.
- The demo can say: active alarm inspection is implemented; operator alarm management is planned.
