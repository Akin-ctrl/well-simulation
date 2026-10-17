# Wellhead Monitoring Simulation

Run a local simulation of oil wells. Watch their readings, compare forecasts
and what-if scenarios, and try operating controls from a web dashboard.

A process model generates 18 signals per well. The services send those readings
through Modbus TCP, store their history in PostgreSQL and TimescaleDB, and check
for alarm conditions. All readings and controls belong to the simulation.

## Quick Start

Install and start Docker with the Compose plugin. Use a terminal on Linux,
macOS, or Windows with WSL. You do not need Python, Node or a separate database
installation.

From the project directory, run:

```bash
./simulator
```

The first start downloads and builds the services, so it can take a few minutes.
The launcher creates local passwords and tokens, saves them in `.env`, and waits
for the services to become healthy. Later starts reuse that configuration.

Open [http://localhost:8090](http://localhost:8090) and sign in:

| Account            | Password             |
| ------------------ | -------------------- |
| `demo@example.com` | `demo-operator-2026` |

Open a well to explore readings, run a forecast or compare a scenario. Turn on
the Guide for explanations of each page. The demo account can save analyses;
it cannot change the running simulation's controls or manage accounts.

### Stop and resume

Stop the simulator and keep your saved work:

```bash
./simulator stop
```

Run `./simulator` again to resume.

### Start fresh

**Reset deletes this stack's readings, saved runs, accounts and settings.** It
keeps `.env` and starts a fresh simulation with the demo account:

```bash
./simulator reset --yes
```

### If startup fails

Make sure Docker is running. Check that another application is not using ports
8090, 5434 or 5020. Read the service logs with:

```bash
./simulator logs
```

Fix the reported problem and run `./simulator` again. Use reset when you want to
throw away the saved simulation and start over.

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
- React dashboard with a fleet overview, analytics, an Alarm Center, and per-well
  monitoring, forecasts, scenario comparisons, and saved-run history.
- Health, readiness, and metrics endpoints for the API, telemetry services,
  and twin-core.
- A twin-core service with one-second process steps, deterministic per-well
  variation, bounded model state, and source-labelled historian reads.
- Durable model snapshots, parameter overrides, and recorded restart recovery.
- Saved on-demand and scheduled forecasts, plus isolated what-if scenarios
  from current state or an exact compatible historical snapshot.
- Draft, activate, cancel, and status APIs for live synthetic controls, with
  durable revisions and a separate administrator parameter publication path.
- Reviewed control and parameter screens, and audited alarm acknowledgement
  and timed shelving for OPERATIONS users.
- An optional Prometheus and Grafana profile for service metrics.
- Docker-based local orchestration.

## Current Dashboard Scope

The dashboard connects monitoring with the existing model analysis:

- Fleet overview for all simulated wellheads.
- Current readings grouped by wellhead and parameter.
- Pressure, temperature, flow, water cut, and GOR trend views.
- Per-wellhead detail pages with active alarms and parameter history.
- An Alarm Center with condition, acknowledgement, timed shelves and action history.
- Model status, successful process time, recovery details, and installed controls.
- Forecast and scenario forms with shared input validation.
- Saved comparison charts, original inputs, confidence reasons, and value tables.
- Per-well history with direct links that reopen results without running the model.
- Historical scenarios from exact compatible model snapshots, with saved origin metadata.
- Administrator-owned forecast schedules, with the latest attempt and latest
  completed result shown separately.

OPERATIONS can acknowledge an exact event, shelve a well’s rule for 5–480 minutes,
or end a shelf early. Each action records a reason and an audited receipt. Active
counts still include acknowledged and shelved conditions. See the
[alarm workflow](docs/design/alarm-lifecycle.md).

## Project status

The application supports well monitoring, saved-state recovery, forecasts,
historical scenarios, scheduled forecasts, reviewed synthetic controls and alarm
management.

The model uses a synthetic reference well and has not been calibrated against
field measurements. The project supports local demonstration and development.
Physical equipment control requires separate engineering and validation.

See the [architecture overview](docs/architecture/overview.md) and
[verification and deployment limits](docs/architecture/production-readiness.md)
for details.

## Access Control

The demo account has the USER role. OPERATIONS accounts can change the simulated
controls and manage alarms. ADMIN accounts can create users, publish model
parameters and manage forecast schedules.

| Capability                                          | USER | OPERATIONS | ADMIN |
| --------------------------------------------------- | :--: | :--------: | :---: |
| View dashboard, trends, alarms                      | yes  |    yes     |  yes  |
| Run what-if scenarios                               | yes  |    yes     |  yes  |
| Acknowledge or shelve an alarm                      |  no  |    yes     |  no   |
| Issue a live control command                        |  no  |    yes     |  no   |
| Create users                                        |  no  |     no     |  yes  |
| Publish model parameters or edit forecast schedules |  no  |     no     |  yes  |

Dashboard access, forecasts, scenarios, and administrator user creation are
implemented. OPERATIONS users can issue live synthetic controls through the
draft and activation API. ADMIN users can publish model parameter overrides
that require a twin-core restart. OPERATIONS can acknowledge events and manage
timed shelves in the Alarm Center. General alarm-rule and asset editing remain
outside the implemented administration screens. Logout revokes all existing sessions and prevents
commands queued by those sessions from applying.

### Administrator access (optional)

A fresh simulation also creates `admin@example.com`. Find its generated password
in `./simulator logs` after the first start and save it if you want to create
accounts or manage model parameters and forecast schedules.

## Documentation

- [Demo walkthrough](docs/demo/demo-script.md): try monitoring and saved analyses.
- [Documentation index](docs/README.md): architecture, models and feature guides.
- [Architecture overview](docs/architecture/overview.md): services and data flow.
- [Model equations and limits](docs/design/reference-well-physics.md).
- [API contracts](docs/openapi/README.md).
- [Web development](monorepo/README.md): run and test the web application locally.
- [Service metrics](observability/README.md): optional Prometheus and Grafana.

## What the numbers mean

Every reading is simulated. The model calculates values and passes them through
Modbus. It links flow, pressure, temperature, water cut, gas, sand, corrosion,
annulus pressure, and vibration, but it has not been calibrated against a real well.
Seven of these (annulus pressure, gas-oil ratio, sand, corrosion rate, H2S, CO2,
and vibration) come from simpler diagnostic equations. They are less grounded
than the pressure and flow equations.

Two more things are compressed for a demo rather than set the way a plant would
set them:

- The trend aggregates bucket at 2 and 5 minutes, not hours and days. The view
  names say so.
- Alarm thresholds sit at the edge of each normal range, so alarms actually
  occur. They are illustrative, not engineering values.

Raw readings are kept for 7 days and alarm events for 90.

## Screenshots

Captured on 2026-10-17 from the running dashboard. Forecast and
scenario images show actual saved model results. The synthetic operating point
shown in the monitoring views has no active alarms.

- [Fleet overview](docs/screenshots/overview.png)
- [Analytics with continuous trends](docs/screenshots/analytics.png)
- [Wellhead detail](docs/screenshots/wellhead-detail.png)
- [Alarm Center](docs/screenshots/alarm-center.png)
- [Saved forecast](docs/screenshots/forecast.png)
- [Scenario comparison](docs/screenshots/scenario.png)
- [Saved-run history](docs/screenshots/saved-runs.png)
- [Historical scenario](docs/screenshots/historical-scenario.png)
- [Historical scenario on a phone](docs/screenshots/historical-scenario-mobile.png)
- [Scenario on a phone-sized viewport](docs/screenshots/scenario-mobile.png)

## Browser verification

Start a healthy local stack, then run from the monorepo:

```bash
cd monorepo
pnpm install --frozen-lockfile
pnpm --filter @well-simulation/website exec playwright install chromium
pnpm --filter @well-simulation/website test:browser
```

The test signs in with the demo account and saves isolated model runs. It checks
that the installed controls and revision do not change. It also checks mobile
layouts, keyboard focus, saved links, stale data, and failed requests. Set
UPDATE_SCREENSHOTS=1 when intentionally refreshing the committed screenshots;
ordinary test runs write their images under the ignored test-results directory.
CI installs Chromium and its system dependencies before running this workflow.

## Licence

Licensed under the [Apache License 2.0](LICENSE). Copyright notices are in
[NOTICE](NOTICE).
