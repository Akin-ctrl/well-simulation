# Wellhead Monitoring Simulation

A simulation of an industrial wellhead monitoring system. It makes up wellhead
telemetry, serves it over a Modbus TCP gateway, stores the readings in
PostgreSQL and TimescaleDB, checks them against alarm rules, and shows the
result on a React dashboard.

This is not a digital twin. It is a SCADA-style simulation and data acquisition
platform.

The next big piece of work is replacing the random telemetry generator with a
process model that holds state. That model would track the condition of each
wellhead, respond to control inputs, run what-if scenarios, and forecast.

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
- React dashboard with overview, analytics, and per-wellhead detail views.
- Docker-based local orchestration.

## Current Dashboard Scope

The dashboard currently focuses on credible monitoring rather than pretending to be a finished digital twin:

- Fleet overview for all simulated wellheads.
- Current readings grouped by wellhead and parameter.
- Pressure, temperature, flow, water cut, and GOR trend views.
- Per-wellhead detail pages with active alarms and parameter history.
- Active alarm data from the backend.

The next dashboard page is an Alarm Center. It shows active alarms with their
severity, the affected asset, the threshold that was crossed, how long the alarm
has been open, and a link to the wellhead.

It will not have acknowledge or shelve buttons. The backend cannot store those
states yet, and a button that does nothing is worse than no button.

## Target Direction

The target system is a lightweight wellhead digital twin with:

- Stateful process simulation per wellhead.
- Control inputs such as choke position, valve state, and pump state.
- Prediction and what-if simulation APIs.
- Historical comparison between observed and predicted behavior.
- Production-grade deployment, observability, security, and documentation.

## What happens next

`docs/roadmap.md` is the plan. `docs/remediation-roadmap.md` covers the repair
work that came out of the project audit, and is complete through Phase 8.

The next feature work is the twin core: a process model that holds state per
wellhead, then observed against simulated, then forecasting and scenarios.

## Quick Start

```bash
cp .env.example .env      # then set POSTGRES_PASSWORD and JWT_SECRET
docker compose up --build
```

The `migrator` service applies the ordered SQL migrations in `data/sql/migrations`
before the API or the telemetry services start. Schema changes do **not** require
recreating the volume.

Open the dashboard at <http://localhost:8090> and sign in:

| Account | Role | Password |
| --- | --- | --- |
| `demo@example.com` | `USER` | `demo-operator-2026` |

This account can view the dashboard and run what-if scenarios. It can do nothing
else. The password is in the repository on purpose. It only gives read access to
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

Running the plant and administering the system are separate jobs. An
administrator cannot send a control command, and an operator cannot create
users.

| Capability | USER | OPERATIONS | ADMIN |
| --- | :---: | :---: | :---: |
| View dashboard, trends, alarms | yes | yes | yes |
| Run what-if scenarios | yes | yes | yes |
| Acknowledge or shelve an alarm | no | yes | no |
| Issue a live control command | no | yes | no |
| Manage users | no | no | yes |
| Edit alarm rules and asset metadata | no | no | yes |

`OPERATIONS` guards nothing yet. Alarm acknowledgement and the control API are
not built. The role exists now so that when they are built, they are built
against it.

## Documentation

- `docs/README.md`: index of everything in `docs/`
- `docs/architecture/overview.md`: the current architecture and the target one
- `docs/architecture/production-readiness.md`: what production-grade means here
- `docs/adr/README.md`: the architecture decision records
- `docs/openapi/README.md`: the API contracts
- `docs/remediation-roadmap.md`: the plan for fixing what the audit found

## What the numbers mean

Every reading is invented. The simulator picks a random value inside each
parameter's configured range, and pushes about one in ten outside it so the
alarm rules have something to fire on.

Nothing is modelled. Tubing pressure is not related to flow rate, and there is
no choke to close. Anything that looks like a trend is noise.

Two more things are compressed for a demo rather than set the way a plant would
set them:

- The trend aggregates bucket at 2 and 5 minutes, not hours and days. The view
  names say so.
- Alarm thresholds sit at the edge of each normal range, so alarms actually
  occur. They are illustrative, not engineering values.

Raw readings are kept for 7 days and alarm events for 90.

## Where this project actually stands

The local Docker stack runs and the API contract is written down under
`docs/openapi`.

It is not production ready. The twin core does not exist. There are no metrics.
Several accepted ADRs are still unimplemented. `docs/remediation-roadmap.md`
tracks the gap between what the documentation claims and what the code does.
