# Documentation

Product framing, architecture notes, the production readiness checklist, and the
architecture decision records.

## What to call this project

Call it a wellhead monitoring simulation, or a SCADA-style industrial telemetry
platform.

Do not call it a finished digital twin yet. It has a stateful synthetic process
model, but no field calibration, durable model snapshots, forecast API, or
what-if scenarios.

## What is built

- Synthetic wellhead telemetry
- Modbus ingestion
- A PostgreSQL and TimescaleDB historian
- Alarm evaluation in SQL
- A public TypeScript API
- A separate twin-core service with an uncalibrated per-well process model
- A React dashboard with an overview, analytics, per-wellhead detail pages, and
  an Alarm Center

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
- `design/reference-well-physics.md`: phase and diagnostic equations
- `openapi/README.md`: the API contracts and the rules for changing them
- `roadmap.md`: the local feature plan (ignored by Git)
- `remediation-roadmap.md`: the local repair record (ignored by Git)

## Documentation backlog

- Keep the README matching what is actually built.
- Write the ADR before adding major twin behaviour, not after.
- Update the OpenAPI spec with every public API change.
- Keep model assumptions visible wherever synthetic readings appear.
- Keep the demo honest. Show what works, then say what is planned.

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
