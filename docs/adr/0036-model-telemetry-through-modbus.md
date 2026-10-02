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

## Amendment 2026-10-02: Publish the model version with the heartbeat

### The problem

A stored reading recorded its tick time and its source kind, but not which
model version calculated it. Twin-core already reports `modelVersion` in every
fleet snapshot. After a model change, the historian could not tell readings
from version 0.3.0 apart from readings made by the next version. The
[reference-well design](../design/reference-well-physics.md) listed this as a
gap for Milestone 7.

### Options considered

**Carry the version through Modbus (chosen).** The gateway already publishes a
status value (the heartbeat) that ingestion checks before and after every
poll. The version can travel in the same block. This keeps Modbus as the only
path from the model to the historian. It costs three registers, a migration,
and a register-map change that both services must follow.

**Ingestion asks twin-core over HTTP.** This needs no register change. But it
gives ingestion a second source of truth beside Modbus. While twin-core
restarts, the HTTP answer and the registers can describe different models,
and ingestion would gain a new dependency on twin-core.

**Leave it for Milestone 7.** [ADR 0024](0024-twin-persistence-schema.md)
plans model definition tables with a version. Linking readings to them there
would design the link once. Every reading stored until then would carry no
version, so the gap would keep growing.

### Decision

**Registers.** The heartbeat and the version form one status block of five
holding registers on every mapped unit:

| Offset | Content | Encoding |
| --- | --- | --- |
| 1900 to 1901 | Model tick, UTC epoch seconds | unsigned 32 bit, shared `WORD_ORDER` |
| 1902 | Major version | unsigned 16 bit |
| 1903 | Minor version | unsigned 16 bit |
| 1904 | Patch version | unsigned 16 bit |

Version text such as `0.3.0` becomes the registers `0, 3, 0`. An all-zero
version means "not published", just as a zero heartbeat does, so version
`0.0.0` is reserved and rejected. Each part must fit in 16 bits.

**Logic.** `telemetry_common.ModelVersion` is the one definition of the text
and register forms. The gateway parses `modelVersion` from the snapshot and
rejects a batch whose version cannot be encoded. It zeroes the whole status
block before changing any value, then writes the heartbeat and version in one
five-register write after all values are in place. Ingestion reads the block
with one Read Holding Registers request (function code 03, offset 1900,
quantity 5), so both parts come from one moment on the server. Every unit must
report the same tick and the same version, before and after the poll. A
heartbeat with a zero or malformed version rejects the batch, and a version
that changes between the two reads discards it.

**Schema.** Migration `0008_reading_model_version.sql` adds
`parameterReading.model_version` with the domain type `model_version_text`. A
domain is a named type that carries its own check; this one accepts only
`major.minor.patch` digits. Rows written before the migration keep `NULL`,
meaning "not recorded". Their version cannot be recovered, so it is not
backfilled.

### Trap: do not turn the domain into a table constraint

TimescaleDB 2.19 refuses to add any table CHECK constraint, even one marked
`NOT VALID`, once a hypertable holds compressed chunks. `parameterReading`
compresses readings older than one day (migration 0005), so a constraint
migration fails on any volume that has run for more than a day. This was tested
on 2026-10-02. Adding a column of a domain type is allowed and still rejects a
malformed value on insert. **Do not reintroduce a table constraint for this
column.** For the same reason, the rule "random simulator rows have no
version" is not enforced in the database. It spans two columns, which a domain
cannot check. Ingestion is the only writer and sets the version on every model
row.

### Consequences

**Good.**

- Every new reading names the model version that produced it.
- Milestone 7 can join `model_version` to the version in
  `twin_model_definition` without changing the reading format again.

**Costs.**

- The status read grows from two registers to five per unit, still one request.
- The register map gains a reserved range (1900 to 1904) that metadata must
  never map. The existing mapping check already rejects signal registers at
  1899 and above.

**Accepted risks.**

- Readings stored before 2026-10-02 have no recorded version. Retention removes
  them within seven days.
- A twin-core redeploy discards the batch in progress. Ingestion resumes at the
  next tick under the new version, leaving a visible gap.
