# Wellhead Monitoring Simulation

A simulation of an industrial wellhead monitoring system. It makes up wellhead
telemetry, serves it over a Modbus TCP gateway, stores the readings in
PostgreSQL and TimescaleDB, checks them against alarm rules, and shows the
result on a React dashboard.

The Modbus readings still come from a random simulator. A separate twin-core
service now runs an uncalibrated, stateful process model for each well. Its output
is available through an internal read API and does not yet feed Modbus, the
historian, or the dashboard.

## Why This Exists

Industrial digital twin systems need more than dashboards. They need a reliable telemetry pipeline, asset metadata, historical storage, alarm logic, operational interfaces, and a process model that can explain and predict behavior.

This repository currently implements the telemetry and monitoring foundation. The documentation in `docs/` records the decisions required to evolve it into a production-grade lightweight digital twin.

## Current Capabilities

- Metadata-driven wellhead and parameter configuration.
- Synthetic wellhead telemetry generation.
- Modbus TCP gateway for industrial protocol simulation.
- TimescaleDB/PostgreSQL historian for time-series readings.
- PostgreSQL alarm rules and alarm event generation.
- TypeScript API with OpenAPI documentation.
- React dashboard with overview, analytics, per-wellhead detail views, and an
  Alarm Center.
- Health, readiness, and metrics endpoints for the API, telemetry services,
  and the new twin-core service boundary.
- A twin-core service with one-second process steps, deterministic per-well
  variation, bounded model state, and separate source-labelled historian reads.
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
control, and the documentation. Repair phases 0 to 8 are complete. Phase 9
reconciles the remaining documentation and checks startup from a clean copy.
The local `docs/roadmap.md` records feature work, and the local
`docs/remediation-roadmap.md` records the repair. Both are ignored by Git.
The committed design documents and ADRs record the implemented decisions.

The reduced-order process model now responds to synthetic choke, valve, and
pump inputs. Milestone 6 will connect its output to the Modbus gateway and
historian. Snapshots, forecasts, and what-if APIs follow in later milestones.

## Quick Start

```bash
cp .env.example .env      # then set POSTGRES_PASSWORD, TWIN_DB_PASSWORD, and JWT_SECRET
docker compose up --build
```

The `migrator` service applies the ordered SQL migrations in `data/sql/migrations`
before the API or the telemetry services start. Schema changes do **not** require
recreating the volume.

The dashboard is served at <http://localhost:8090>. The current production
image returns HTTP 200 but renders a blank page in a real browser because its
Content Security Policy blocks React Router's inline startup scripts. Sign-in
will work after that deployment issue is fixed:

| Account | Role | Password |
| --- | --- | --- |
| `demo@example.com` | `USER` | `demo-operator-2026` |

This account can view the dashboard. What-if scenarios are planned, not built.
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

Only dashboard access and administrator user creation are implemented in this
table. `OPERATIONS` guards nothing yet. Scenarios, alarm acknowledgement, the
control API, and metadata editing are not built.

## Documentation

- `docs/README.md`: index of everything in `docs/`
- `docs/architecture/overview.md`: the current architecture and the target one
- `docs/architecture/production-readiness.md`: what production-grade means here
- `docs/adr/README.md`: the architecture decision records
- `docs/openapi/README.md`: the API contracts
- `docs/design/twin-core-milestone-4.md`: the twin-core skeleton design
- `docs/design/twin-core-milestone-5.md`: the process model equations and limits
- `docs/roadmap.md`: the local feature plan (ignored by Git)
- `docs/remediation-roadmap.md`: the local repair record (ignored by Git)

## What the numbers mean

Every reading is invented. The simulator usually picks a random value inside
each parameter's configured normal range. About one in ten draws use a wider
range; only some of those values fall outside the normal range and exercise the
alarm rules.

Those stored readings are still independent noise. The separate twin-core
model links flow, pressure, choke position, temperature, water cut, and slow
degradation, but its results are not yet the stored readings.

Two more things are compressed for a demo rather than set the way a plant would
set them:

- The trend aggregates bucket at 2 and 5 minutes, not hours and days. The view
  names say so.
- Alarm thresholds sit at the edge of each normal range, so alarms actually
  occur. They are illustrative, not engineering values.

Raw readings are kept for 7 days and alarm events for 90.

## Where this project actually stands

The services run against the existing database volume. A separate clean-start
check also passed with a newly created volume: the migrator exited successfully,
all services became healthy, and the API and twin-core reported ready. Each
image was built separately after a combined Docker Buildx run stopped the
daemon. The Compose health check requires TCP database readiness, which avoids
the startup race found in an earlier clean-start check.
The API contract is written down under `docs/openapi`.

It is not production ready. The twin-core model uses synthetic, uncalibrated
parameters and has no durable snapshots or live device control. The API, gateway,
ingestion, and twin-core services expose metrics, but no metrics collection stack
is deployed.
Several accepted ADRs are still unimplemented. The
[ADR index](docs/adr/README.md) tracks the gap between decisions and code.
