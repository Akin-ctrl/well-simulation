# ADR-0036: Send model telemetry through the existing Modbus path

**Status**: Accepted, amended

**Date**: 2026-09-30

**Decision**: The gateway reads one complete synthetic model fleet snapshot from twin-core and maps only values the model calculates into Modbus holding registers. Ingestion keeps polling Modbus and stores complete batches with their model tick time and source label. This preserves the industrial read path without presenting unsupported or stale values as fresh measurements.

Roadmap: [Milestone 6](../roadmap.md#milestone-6-integrate-modbus-with-twin-core)

---

## Context

The gateway used to start a random simulator as a child process. Its pressure
and flow had no relationship even though twin-core now calculates linked values.
The seeded parameter map has 18 signals, while the model calculates only 11.
Writing zero for the others would invent measurements.

### What already exists

`modbus_gateway.py` serves Modbus TCP holding registers.
`database_ingestion.py` polls them and inserts `parameterReading` rows.
`twin_core/state.py` owns one process state per active well. The register map
and word order already have tests and stored history.

---

## Options considered

### A. One fleet HTTP snapshot through Modbus (chosen)

A single read gives the gateway states from one model tick. The gateway
validates them and updates the existing register image. This preserves Modbus,
ingestion, and SQL alarms, at the cost of an HTTP request and validation.

### B. Write model values directly to the historian

This is a shorter data path. It removes the Modbus read path the project
intentionally demonstrates and adds a second writer alongside ingestion.

### C. Read one well at a time over HTTP

This reuses the existing per-well endpoint. A fleet poll could combine different
model ticks, so the gateway would need a retry protocol to assemble one batch.

---

## Decision

### Schema

Migration `0007_model_reading_source.sql` adds non-null text column
`parameterReading.source_kind`. Existing rows receive
`synthetic_random_simulator` as the default. Ingestion writes
`synthetic_reduced_order_model` for new rows. A check constraint allows only
those two values. `timestamp_utc` is the model tick time; `inserted_at` remains
database arrival time.

### Logic

`model_source.py` is the one mapping from model fields to supported parameter
codes. Gateway and ingestion both filter database mappings through it. The
mapping rejects incompatible types, overlapping register pairs, reserved
heartbeat addresses, and invalid Modbus unit IDs. It does not turn the model's
dimensionless corrosion index into a corrosion rate measured in mils per year.

The gateway accepts only a current, complete, running, synthetic fleet tick.
Before changing registers, it clears the heartbeat to zero. It publishes the
new tick time after all values are written. Ingestion reads the heartbeat before
and after its poll and rejects missing, unchanged, old, future, or changing
ticks. A failed register read rejects the whole batch.

### API surface

Twin-core serves read-only `GET /model-snapshot` on the internal Compose
network. It returns a complete fleet or HTTP 503 when the model is not ready.
The public dashboard still calls the TypeScript API. This change adds no
control write route.

The latest historian read includes `source` on each value. Its overall source
is `mixed` while old and new readings coexist.

### Migration

The ordered migrator applies the column before gateway and ingestion start.
Older readings keep their values and timestamps. The legacy random simulator
script stays in the repository for history but is no longer started by Compose.

## Things this touches that look like they should be reconciled

Seven seeded parameters have no model equation: annulus pressure, gas-oil ratio,
sand, corrosion rate, H2S, CO2, and vibration. Do not reintroduce random values
or map an unrelated model factor to fill these gaps. Add a measured source or a
validated model when its physical inputs and units are defined.

---

## Consequences

**Good.**

- Stored pressure and flow now come from one model state.
- A failed or incomplete poll cannot become a fresh historian batch.
- Readers can distinguish older random data from current model values.

**Costs.**

- The gateway makes one bounded HTTP request per telemetry interval.
- Only 11 parameter types receive new readings. The unchanged dashboard may
  show gaps for the other seven.
- The heartbeat handshake and metadata validation add code that must stay in
  step with the register mapping. The exact-word codec test guards word order.

**Accepted risks.**

- Modbus has no authentication, timestamp, or quality field. Network isolation
  and the application heartbeat are adequate for this local simulation, not a
  plant control deployment.
- A gateway crash can lose an in-progress model tick. Ingestion records no
  partial batch and resumes from the next tick, leaving a visible time gap.
- Float32 encoding loses some precision. The historian keeps the decoded value
  that the Modbus client actually saw.

---

## Related

- [ADR 0004](0004-industrial-interface.md): keeps Modbus as the industrial read path.
- [ADR 0011](0011-twin-core-communication.md): selects internal HTTP and durable database storage.
- [Milestone 6 design](../design/twin-core-milestone-6.md): gives the register, timing, and failure details.

## Amendment 2026-09-30: Map the seven reference-well diagnostics

### The problem

The first version correctly omitted seven seeded registers because the process
model had no equations or defined inputs for them. The synthetic reference-well
extension now provides those quantities with declared units and bounds.

### Decision: complete the existing mapped fleet

The gateway now maps all 18 seeded parameter types from one version 0.3.0 model
tick. The added fields are annulus pressure, gas-oil ratio, sand concentration,
uniform corrosion rate, produced-gas H2S and CO2 fractions, and vibration RMS
velocity. The same complete-batch, heartbeat, and source-label rules apply.
When mappings span more than one Modbus unit, ingestion checks every heartbeat
before and after polling. A mismatch rejects the entire batch.
`corrosionFactor` remains an internal dimensionless index and must not be
reintroduced as a rate. The new corrosion rate comes from the separate current
and Faraday-law calculation described in the
[reference-well design](../design/reference-well-physics.md).

The outputs are synthetic estimates, not field measurements. Model source
labelling and `fieldCalibrated: false` remain required. Old random readings keep
their original source until retention removes them.
