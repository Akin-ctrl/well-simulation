# Documentation

This directory is the project control room: product framing, architecture notes, production-readiness criteria, and architecture decision records.

## Project Framing

This project should currently be described as a **wellhead monitoring simulation** or **SCADA-style industrial telemetry platform**.

It should not yet be described as a full digital twin. It becomes a digital twin when it includes a stateful process model, control inputs, prediction, calibration, and what-if simulation.

## Current Product State

The implemented system now supports:

- synthetic wellhead telemetry
- Modbus-style ingestion
- PostgreSQL/TimescaleDB historian storage
- SQL alarm evaluation
- public TypeScript API
- React dashboard overview
- dashboard analytics
- per-wellhead detail pages

The next product slice is the **Alarm Center**. It should make active alarms inspectable and explainable without adding fake acknowledge/shelve behavior before the data model supports it.

## Contents

- `architecture/overview.md` — current architecture and target digital twin architecture.
- `architecture/production-readiness.md` — engineering checklist for production-grade quality.
- `adr/README.md` — ADR format and index.
- `demo/demo-script.md` — intended portfolio-review demo flow.
- `openapi/README.md` — API contract index and rules.

## Near-Term Documentation Backlog

- Keep README language aligned with the current implementation.
- Add ADRs before introducing major digital-twin behavior.
- Update OpenAPI specs with every public API change.
- Document simulator assumptions before presenting twin predictions.
- Keep demo instructions realistic: show implemented behavior first, then target behavior.

## Documentation Principles

- Every major architectural choice gets an ADR.
- Each ADR must list considered options.
- Each ADR must explain why the selected option won.
- Rejected options should be recorded, not silently forgotten.
- Docs should distinguish current implementation from target architecture.
