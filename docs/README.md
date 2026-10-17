# Documentation

Product framing, architecture notes, the production readiness checklist, and the
architecture decision records.

## What to call this project

Call it a wellhead monitoring simulation, or a SCADA-style industrial telemetry
platform.

Do not call it a finished digital twin yet. It has a stateful synthetic process
model, durable snapshots, and forecast and what-if APIs. It has no field
calibration or physical control connection.

## What is built

- Synthetic wellhead telemetry
- Modbus ingestion
- A PostgreSQL and TimescaleDB historian
- Alarm evaluation in SQL
- A public TypeScript API
- A separate twin-core service with an uncalibrated per-well process model
- Durable model snapshots and recorded restart recovery
- Saved on-demand forecasts and isolated what-if scenarios through the API
- Durable live synthetic control commands and reviewed parameter publication
- A React dashboard with fleet monitoring, analytics, an Alarm Center, model
  status, forecast and scenario forms, comparison charts, and saved-run history

The Alarm Center shows active alarms and their thresholds. It does not offer
acknowledge or shelve actions because the data model cannot store those states.

## Contents

- `architecture/overview.md`: the current architecture and the target one
- `architecture/production-readiness.md`: what production-grade means here
- `adr/README.md`: the ADR format and the index
- `demo/demo-script.md`: the demo flow for a portfolio review
- `design/twin-core-milestone-4.md`: the service boundary and its limits
- `design/twin-core-milestone-5.md`: the process model equations and limits
- `design/twin-core-milestone-6.md`: model telemetry through Modbus
- `design/twin-core-milestone-7.md`: twin persistence and restore
- `design/twin-core-milestone-8.md`: forecasts, scenarios, and saved results
- `design/twin-core-milestone-8b.md`: live model controls, authorization, and recovery
- `design/twin-core-milestone-9.md`: the wellhead workspace and browser verification
- `design/milestone-9/visual-specification.md`: shared visual rules, state matrix, and mockup gallery
- `design/milestone-9/accessibility-verification.md`: completed checks and accessibility limits
- `design/consolidated-schema.md`: single schema source and checked adoption of existing data
- `design/database-engine-upgrade.md`: engine conversion, backup restoration, and recorded acceptance
- `design/reference-well-physics.md`: phase and diagnostic equations
- `openapi/README.md`: the API contracts and the rules for changing them
- `roadmap.md`: the local feature plan (ignored by Git)
- `remediation-roadmap.md`: the local repair record (ignored by Git)

The root README and ADR index provide published implementation status. The two
local roadmaps are optional working notes and are absent from a clean checkout.

## Documentation backlog

- Keep the README matching what is actually built.
- Write the ADR before adding major twin behaviour, not after.
- Update the OpenAPI spec with every public API change.
- Keep model assumptions visible wherever synthetic readings appear.
- Keep the demo honest. Show what works, then say what is planned.
- Complete relevant screen mockups, loading and failure states, shared visual
  rules, and responsive behaviour before new UI implementation. Follow the
  [design completion gate](design/twin-core-milestone-9.md#design-completion-gate).
  Validate accessibility during implementation and record the checks actually run.

## How we write docs here

- Every major architectural choice gets an ADR.
- Each ADR lists the options that were considered.
- Each ADR says why the chosen option won.
- Rejected options stay on the record. They are the reasoning.
- Docs say plainly what is built and what is only planned.
- Prose follows the plain English standard in
  the project's coding standards: short sentences, everyday words, active
  voice, and no em dashes. `scripts/check_prose.py` enforces the checkable
  parts.

The [dashboard usability design](design/dashboard-usability.md) records the
revised workflow, design checks and delivery sequence.

The [operating workspace design](design/operating-workspace.md) covers the
control and parameter screens, role boundaries and uncertain-response recovery.
