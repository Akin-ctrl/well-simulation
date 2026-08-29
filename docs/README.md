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
- A React dashboard with an overview, analytics, and per-wellhead detail pages

The next piece of work is the Alarm Center. It should make active alarms
inspectable and explainable. It should not add acknowledge or shelve buttons
until the data model can store those states, because a button that pretends to
do something is worse than no button.

## Contents

- `architecture/overview.md`: the current architecture and the target one
- `architecture/production-readiness.md`: what production-grade means here
- `adr/README.md`: the ADR format and the index
- `demo/demo-script.md`: the demo flow for a portfolio review
- `openapi/README.md`: the API contracts and the rules for changing them
- `remediation-roadmap.md`: the plan for fixing what the audit found

## Documentation backlog

- Keep the README matching what is actually built.
- Write the ADR before adding major twin behaviour, not after.
- Update the OpenAPI spec with every public API change.
- Write down the simulator's assumptions before showing anyone a prediction.
- Keep the demo honest. Show what works, then say what is planned.

## How we write docs here

- Every major architectural choice gets an ADR.
- Each ADR lists the options that were considered.
- Each ADR says why the chosen option won.
- Rejected options stay on the record. They are the reasoning.
- Docs say plainly what is built and what is only planned.
- Prose follows the plain English standard in
  `CODING_STANDARDS_COMMITMENT.md`. Short sentences, everyday words, active
  voice.
