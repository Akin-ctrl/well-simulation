# ADR 0011: Use Internal HTTP Plus Durable Database Persistence

- Status: Accepted
- Date: 2026-05-31

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

