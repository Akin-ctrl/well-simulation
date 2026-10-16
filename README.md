# Wellhead Monitoring Simulation

A simulation of an industrial wellhead monitoring system. It makes up wellhead
telemetry, serves it over a Modbus TCP gateway, stores the readings in
PostgreSQL and TimescaleDB, checks them against alarm rules, and shows the
result on a React dashboard.

The Modbus gateway now serves all 18 seeded signals from the uncalibrated,
stateful reference-well model in twin-core. Ingestion stores those readings in
TimescaleDB with the model tick time and source label.

## Why This Exists

Industrial digital twin systems need more than dashboards. They need a reliable telemetry pipeline, asset metadata, historical storage, alarm logic, operational interfaces, and a process model that can explain and predict behavior.

This repository implements monitoring, a synthetic process model, restart recovery,
and saved forecasts and scenarios. The documentation in `docs/` records the
implemented decisions and the work still needed for a field-calibrated twin.

## Current Capabilities

- Metadata-driven wellhead and parameter configuration.
- Deterministic synthetic wellhead telemetry generation.
- Modbus TCP gateway for industrial protocol simulation.
- TimescaleDB/PostgreSQL historian for time-series readings.
- PostgreSQL alarm rules and alarm event generation.
- TypeScript API with OpenAPI documentation.
- React dashboard with overview, analytics, per-wellhead detail views, and an
  Alarm Center.
- Health, readiness, and metrics endpoints for the API, telemetry services,
  and the new twin-core service boundary.
- A twin-core service with one-second process steps, deterministic per-well
  variation, bounded model state, and source-labelled historian reads.
- Durable model snapshots, parameter overrides, and recorded restart recovery.
- Authenticated APIs for saved forecasts and isolated what-if scenarios.
- Draft, activate, cancel, and status APIs for live synthetic controls, with
  durable revisions and a separate administrator parameter publication path.
- Docker-based local orchestration.

## Current Dashboard Scope

The dashboard currently focuses on credible monitoring rather than pretending to be a finished digital twin:

- Fleet overview for all simulated wellheads.
- Current readings grouped by wellhead and parameter.
- Pressure, temperature, flow, water cut, and GOR trend views.
- Per-wellhead detail pages with active alarms and parameter history.
- An Alarm Center with severity, threshold, age, and links to wellheads.

The Alarm Center has no acknowledge or shelve buttons because the backend cannot
store those states yet.

## Target Direction

The target system is a lightweight wellhead digital twin with:

- Stateful process simulation per wellhead.
- Control inputs such as choke position, valve state, and pump state.
- Prediction and what-if simulation APIs.
- Historical comparison between observed and predicted behavior.
- Production-grade deployment, observability, security, and documentation.

## What happens next

An audit of this project found problems in the pipeline, the schema, access
control, and the documentation. Repair phases 0 to 9 are complete as of
2026-10-13. Phase 9 reconciled the documentation and verified a fresh installation.
The local `docs/roadmap.md` records feature work, and the local
`docs/remediation-roadmap.md` records the repair. Both are ignored by Git.
The committed design documents and ADRs record the implemented decisions.

Milestones 4 through 8 are implemented: the model feeds Modbus and the historian,
recovers saved state, and runs forecasts and private scenarios. The
[8B design](docs/design/twin-core-milestone-8b.md) covers the implemented live
synthetic control lifecycle and its verification.
Dashboard forecast and scenario features belong to the deferred Milestone 9.
The published [ADR index](docs/adr/README.md) records decision status; local
roadmaps are working notes, not dependencies for understanding the project.

## Quick Start

```bash
cp .env.example .env
# Set POSTGRES_PASSWORD, TWIN_DB_PASSWORD, API_DB_PASSWORD, JWT_SECRET,
# and TWIN_INTERNAL_TOKEN to separate random values before starting.
docker compose up --build
```

The `migrator` service installs `data/sql/schema.sql` on a new database and
verifies its recorded baseline on later starts. A database from the numbered
schema runner needs a one-time checked adoption. Follow the
[data-layer instructions](data/README.md#existing-database-adoption) before
starting the updated installer against an existing volume. Adoption preserves
the volume and its data; later structural changes need a reviewed conversion.

For an existing `.env`, add `API_DB_USER=well_api` and a separate URL-safe
`API_DB_PASSWORD` before upgrading. The migrator creates this restricted
application account. The API no longer uses the database administrator login.

The dashboard is served at <http://localhost:8090>. Sign in with the demo
account:

| Account | Role | Password |
| --- | --- | --- |
| `demo@example.com` | `USER` | `demo-operator-2026` |

This account can view the dashboard and run what-if scenarios through the API.
The dashboard has no scenario form yet.
The password is in the repository on purpose. It only gives read access to
made-up telemetry on a local stack.

You cannot sign yourself up. Only an `ADMIN` can create a user, as set out in
[ADR 0033](docs/adr/0033-role-model-and-access-control.md). The migrator creates
the first administrator when it first runs, and logs the generated password
once:

```
"message": "Created bootstrap administrator; record this password now",
"email": "admin@example.com", "password": "..."
```

Get it with `docker compose logs migrator` and save it. There is no way to
recover it later.

## Access Control

Running the plant and administering the system are separate jobs. The table
shows the intended permissions, including capabilities that are not built yet.
An administrator cannot send a control command, and an operator cannot create
users.

| Capability | USER | OPERATIONS | ADMIN |
| --- | :---: | :---: | :---: |
| View dashboard, trends, alarms | yes | yes | yes |
| Run what-if scenarios | yes | yes | yes |
| Acknowledge or shelve an alarm | no | yes | no |
| Issue a live control command | no | yes | no |
| Manage users | no | no | yes |
| Edit alarm rules and asset metadata | no | no | yes |

Dashboard access, forecasts, scenarios, and administrator user creation are
implemented. OPERATIONS users can issue live synthetic controls through the
draft and activation API. ADMIN users can publish model parameter overrides
that require a twin-core restart. Alarm acknowledgement and other metadata
editing remain planned. Logout revokes all existing sessions and prevents
commands queued by those sessions from applying.

## Documentation

- `docs/README.md`: index of everything in `docs/`
- `docs/architecture/overview.md`: the current architecture and the target one
- `docs/architecture/production-readiness.md`: what production-grade means here
- `docs/adr/README.md`: the architecture decision records
- `docs/openapi/README.md`: the API contracts
- `docs/design/twin-core-milestone-4.md`: the twin-core skeleton design
- `docs/design/twin-core-milestone-5.md`: the process model equations and limits
- `docs/design/twin-core-milestone-6.md`: the model-to-Modbus data path
- `docs/design/twin-core-milestone-7.md`: twin persistence design and verification
- `docs/design/twin-core-milestone-8.md`: forecasts, scenarios, and saved results
- `docs/design/twin-core-milestone-8b.md`: live control lifecycle and operating steps
- `docs/design/reference-well-physics.md`: the phase and diagnostic equations
- `docs/roadmap.md`: the local feature plan (ignored by Git)
- `docs/remediation-roadmap.md`: the local repair record (ignored by Git)

## What the numbers mean

Every reading is invented. New values are calculated by the reduced-order
model and passed through Modbus. Older random readings remain labelled in the
historian until retention removes them. The model links flow, pressure, temperature, water cut, gas, sand, corrosion,
annulus pressure, and vibration, but it has not been calibrated against a real well.
Seven of these (annulus pressure, gas-oil ratio, sand, corrosion rate, H2S, CO2,
and vibration) come from simpler diagnostic equations added in model version
0.3.0. They are less grounded than the pressure and flow equations.

Two more things are compressed for a demo rather than set the way a plant would
set them:

- The trend aggregates bucket at 2 and 5 minutes, not hours and days. The view
  names say so.
- Alarm thresholds sit at the edge of each normal range, so alarms actually
  occur. They are illustrative, not engineering values.

Raw readings are kept for 7 days and alarm events for 90.

## Screenshots

Captured on 2026-10-13 from the running local stack after continuous telemetry
collection. The current synthetic operating point has no active alarms.

- [Fleet overview](docs/screenshots/overview.png)
- [Analytics with continuous trends](docs/screenshots/analytics.png)
- [Wellhead detail](docs/screenshots/wellhead-detail.png)
- [Alarm Center](docs/screenshots/alarm-center.png)

## Where this project actually stands

On 2026-10-13, a clean export of commit `e3d44c7` passed
`docker compose up -d --build` with a fresh volume. All six images built,
the migrator exited with code 0, and all six long-running services became
healthy. The API and twin-core returned ready, with all 12 model wells running.
The verification used separate container names, loopback ports, network, and
volume to preserve the existing stack. The test export contained committed
application files; only those isolation settings and generated test secrets
differed.

The same isolated stack then passed an upgrade to the working Milestone 8B code.
A live synthetic command flowed through the model, Modbus, and historian;
restart preserved its controls. Parameter publication adopted a new parameter
set after restart. The four existing dashboard pages passed browser checks.
See the [8B verification record](docs/design/twin-core-milestone-8b.md#verification-on-2026-10-13).
The API contract is written down under `docs/openapi`.

It is not production ready. The twin-core model uses synthetic, uncalibrated
parameters and has no field calibration or physical device control. The API, gateway,
ingestion, and twin-core services expose metrics, but no metrics collection stack
is deployed.
Several accepted ADRs are still unimplemented. The
[ADR index](docs/adr/README.md) tracks the gap between decisions and code.
