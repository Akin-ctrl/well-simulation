# Documentation

Product framing, architecture notes, the production readiness checklist, and the
architecture decision records.

## What to call this project

Call it a wellhead monitoring simulation, or a SCADA-style industrial telemetry
platform.

Do not call it a digital twin yet. It becomes one when it has a stateful process
model, control inputs, prediction, calibration, and what-if simulation. It has
none of those.

## What is built

- Synthetic wellhead telemetry
- Modbus ingestion
- A PostgreSQL and TimescaleDB historian
- Alarm evaluation in SQL
- A public TypeScript API
- A separate twin-core service skeleton with no physical model yet
- A React dashboard with an overview, analytics, per-wellhead detail pages, and
  an Alarm Center

The Alarm Center shows active alarms and their thresholds. It does not offer
acknowledge or shelve actions because the data model cannot store those states.

## Contents

- `architecture/overview.md`: the current architecture and the target one
- `architecture/production-readiness.md`: what production-grade means here
- `adr/README.md`: the ADR format and the index
- `demo/demo-script.md`: the demo flow for a portfolio review
- `design/twin-core-milestone-4.md`: the new service design and its limits
- `openapi/README.md`: the API contracts and the rules for changing them
- `roadmap.md`: the tracked feature plan
- `remediation-roadmap.md`: the repair record

## Documentation backlog

- Keep the README matching what is actually built.
- Write the ADR before adding major twin behaviour, not after.
- Update the OpenAPI spec with every public API change.
- Keep the simulator's assumptions visible while it produces random readings.
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
