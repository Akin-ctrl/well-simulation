# ADR 0011: Use Internal HTTP Plus Durable Database Persistence

- Status: Accepted
- Date: 2026-05-31
- Implementation: Implemented

## Context

Other services need to communicate with `twin-core` for current state, telemetry reads, control commands, forecasts, and what-if scenarios. The system also needs durable history for auditability and analysis.

## Options Considered

### Option A: Database-mediated communication

- Pros: Simple persistence; fewer service APIs.
- Cons: Turns the database into a message bus; awkward for low-latency commands and scenarios.

### Option B: HTTP API only

- Pros: Simple and explicit service boundary.
- Cons: Does not by itself provide durable history or audit records.

### Option C: Message/event stream

- Pros: Scalable and realistic for high-throughput industrial telemetry.
- Cons: Adds infrastructure too early.

### Option D: Hybrid internal HTTP plus database persistence

- Pros: Clear command/query boundary with durable records.
- Cons: Slightly more architecture than one mechanism.

## Decision

Use **Option D: internal HTTP for service interaction and PostgreSQL/TimescaleDB for durable persistence**.

## Rationale

HTTP keeps control, forecast, scenario, and latest telemetry calls explicit and easy to test. PostgreSQL/TimescaleDB remains the durable store for state snapshots, readings, predictions, scenarios, and audits.

## Consequences

- `twin-core` should expose internal endpoints for health, state, telemetry, control, forecast, and scenario operations.
- The Modbus gateway should read latest telemetry from `twin-core` over HTTP.
- The public dashboard should call the TypeScript API, not `twin-core` directly.

## Implementation on 2026-09-29

The service exposes internal read-only HTTP for health, readiness, metrics, state, and current telemetry. Durable twin snapshots, commands, forecasts, and scenarios are still planned.

## Implementation on 2026-09-30

The Modbus gateway now reads a complete, read-only model fleet snapshot over
internal HTTP. It converts the supported model fields into Modbus registers;
ingestion still polls Modbus and writes source-labelled readings to TimescaleDB.
Durable model state snapshots, commands, forecasts, and scenarios remain planned.

## Implementation on 2026-10-13

Milestones 7 and 8 now persist model snapshots and complete forecast and scenario
runs. Internal HTTP accepts bounded analysis requests using a service token;
the public API checks the human session. Live commands remain proposed in
[ADR 0038](0038-live-control-command-lifecycle.md).

## Implementation on 2026-10-13: Command delivery

Milestone 8B uses authenticated internal HTTP for command wake-up hints and
parameter validation. A command UUID identifies a committed database record;
the hint does not carry controls or human authority. The inbox also polls the
durable queue, so a missed hint does not lose an accepted command. Database
transactions own activation, application, and their matching audit or event rows.
