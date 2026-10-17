# Documentation

This project simulates wellhead monitoring, model analysis and reviewed operating
actions. Its process model is synthetic and uncalibrated. It has no physical
control connection. Call it a wellhead monitoring simulation, not a finished
field digital twin.

## Start here

- [Run the application](../README.md#quick-start): setup and demo account.
- [Follow the demo](demo/demo-script.md): monitoring, saved comparisons and optional operating workflows.
- [Read the architecture](architecture/overview.md): service boundaries and current diagrams.
- [Check readiness and open work](architecture/production-readiness.md): evidence, limitations and delivery gates.
- [Develop the web application](../monorepo/README.md): local environment and verification commands.

## What is built

Twin-core calculates all 18 seeded signals, serves them through Modbus and saves
its state for restart recovery. Ingestion stores source-labelled readings in
TimescaleDB. SQL evaluates alarm conditions.

The dashboard connects fleet monitoring with forecasts, historical scenarios,
saved results and administrator-owned forecast schedules. OPERATIONS users can
review live synthetic controls, acknowledge exact alarm events and manage timed
shelves. ADMIN users can publish model parameters and create accounts. Parameter
publication requires a model-service restart. Optional Prometheus and Grafana
services show application health and metrics.

## Architecture and decisions

- [Architecture overview](architecture/overview.md)
- [Production readiness](architecture/production-readiness.md)
- [ADR index](adr/README.md): accepted decisions, amendments and implementation status.
- [API contracts](openapi/README.md): public Hono API and internal twin-core HTTP API.
- [Synthetic reference-well physics](design/reference-well-physics.md): equations, units and limits.

## Model and feature designs

| Design | What it covers |
| --- | --- |
| [Milestone 4](design/twin-core-milestone-4.md) | Service boundary and initial lifecycle |
| [Milestone 5](design/twin-core-milestone-5.md) | Original process equations and units |
| [Milestone 6](design/twin-core-milestone-6.md) | Model telemetry through Modbus |
| [Milestone 7](design/twin-core-milestone-7.md) | Snapshots, parameters and restart recovery |
| [Milestone 8](design/twin-core-milestone-8.md) | Forecasts, scenarios and saved results |
| [Milestone 8B](design/twin-core-milestone-8b.md) | Reviewed control commands and parameter publication |
| [Historical scenarios](design/historical-scenarios.md) | Exact snapshot selection and compatible branches |
| [Scheduled forecasts](design/scheduled-forecasts.md) | Ownership, bounded jobs and automatic saved results |
| [Alarm lifecycle](design/alarm-lifecycle.md) | Acknowledgement, shelving, receipts and condition state |

Milestone documents record their original scope and dated verification. Later
designs extend that scope. Use the current architecture and ADR index to establish
what runs now; a test result from an earlier milestone does not cover later work.

## Interface design and verification

- [Milestone 9 workspace](design/twin-core-milestone-9.md): design completion gate and browser workflow.
- [Visual specification](design/milestone-9/visual-specification.md): spacing, type, colours, charts and responsive states.
- [Original mockup gallery](design/milestone-9/mockups/index.html): illustrative screen states.
- [Accessibility verification](design/milestone-9/accessibility-verification.md): checks run and combinations not tested.
- [Usability follow-up](design/dashboard-usability.md): Guide, page explanations and local delivery checks.
- [Operating workspace](design/operating-workspace.md): control and parameter forms, reviews and outcome recovery.

Historical-scenario, schedule, alarm and observability designs link their own
mockups and verification. Application screenshots live in `screenshots/`;
each design identifies captures that use browser-test fixtures.

## Database and service operation

- [Data layer](../data/README.md): telemetry, schema ownership and service accounts.
- [Existing-database update guide](operations/database-updates.md): choose the correct adoption, engine or extension path.
- [Schema consolidation](design/consolidated-schema.md): the single SQL source and checked legacy adoption.
- [Engine upgrade](design/database-engine-upgrade.md): preserved volumes and recovery verification.
- [Schema extension](design/schema-extension.md): verified additions to existing installations.
- [Retention policy](adr/0026-retention-and-compression.md#amendment-2026-10-17-record-the-installed-retention-policy): actual retention and durable records.
- [Service metrics design](design/observability.md) and [runbook](../observability/README.md): optional Prometheus and Grafana.
- [Security policy](../.github/SECURITY.md): private vulnerability reporting.

## Local planning notes

`roadmap.md` records feature work and `remediation-roadmap.md` records the repair
history. Both remain ignored by Git and are absent from a clean checkout.
The committed README, readiness record and ADR index provide current status
without requiring those local notes. `github-hardening-checklist.md` is also an
ignored reference from another project, not this repository's delivery status.

## Maintaining these documents

Write for someone who has not seen the project. Use active voice, ordinary words
and concrete examples. Define technical terms when first needed. Avoid em dashes.
`scripts/check_prose.py` checks the mechanical parts of that style.

Keep setup commands consistent with the current scripts and configuration.
Update API specifications with route changes. Append dated ADR amendments when
a decision changes, preserve the original reasoning and update the index status.
State which checks actually ran and keep local results separate from remote CI.

Complete relevant mockups, loading and failure states, shared visual rules and
responsive behavior before implementing a new screen. Follow the
[design completion gate](design/twin-core-milestone-9.md#design-completion-gate)
and record accessibility checks during implementation.
