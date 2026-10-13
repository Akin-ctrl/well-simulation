# Data Layer

The telemetry pipeline and the database schema. The dashboard and API live in
`monorepo/`, and the root `README.md` covers running the whole stack.

## What is here

| Path | What it does |
| --- | --- |
| `src/wellhead_simulator.py` | Legacy random simulator, no longer started by Compose |
| `src/modbus_gateway.py` | Fetches model snapshots and serves supported values as holding registers |
| `src/database_ingestion.py` | Polls Modbus and writes model readings to the historian |
| `src/schema.py` | Applies the ordered SQL migrations |
| `src/model_source.py` | Validates model snapshots and maps all 18 seeded signals |
| `src/telemetry_common.py` | Config, logging, database access, and the health endpoints |
| `src/telemetry_metrics.py` | Prometheus metric definitions |
| `sql/migrations/` | The schema. Canonical, per ADR 0006 |
| `sql/seeds/` | Demo fleet and accounts, applied after the migrations |

## How the data flows

Twin-core advances each active well once per second. The gateway requests one
complete fleet snapshot every telemetry interval, validates it, and writes the
18 seeded values to Modbus holding registers. Ingestion polls those registers
and stores the model tick time and source label in TimescaleDB. The former random
simulator script remains as historical code but is not started by the gateway.

Twin-core saves its process state about every 30 model seconds in
`twin_state_snapshot`. A restart checks the saved model version and parameter
fingerprint, then restores each compatible well. It records the decision in
`twin_recovery_event`. Gaps longer than five seconds are counted, and the
model does not invent readings for missing time. The
[Milestone 7 design](../docs/design/twin-core-milestone-7.md) gives the
recovery equations and failure rules.

The full mapping and failure rules are in the [Milestone 6 design](../docs/design/twin-core-milestone-6.md). The [reference-well design](../docs/design/reference-well-physics.md) defines the synthetic equations and their limits.

## Metadata drives everything

The services do not hardcode which wellheads or parameters exist. Each queries
the database on startup and configures itself from what it finds.

Each also re-reads that metadata every telemetry interval, so adding a wellhead,
moving a Modbus address, or changing an alarm threshold takes effect within a
few seconds without a restart. ADR 0035 records why a timer was chosen over a
notification or a reload endpoint.

When a register mapping moves, the gateway clears its whole register image. A
value left at an address nothing maps any more would be a stale reading on a
live interface.

## Telling stale data from fresh

The gateway writes zero to the status registers 1900 to 1904 before changing
values. After the complete register update it writes the model tick's UTC epoch
second (1900 and 1901) and the model version (1902 to 1904, major, minor, and
patch) in one write. Ingestion reads all five before and after a poll. A
missing, unchanged, too-old, or changed-mid-poll heartbeat or version prevents
an insert. When an update lands mid-poll, ingestion polls the new tick at once
instead of waiting a whole interval. The complete read must finish before
half the polling interval has elapsed, even if its start and end status match.
A late batch is discarded and the next scheduled poll proceeds normally.
`timestamp_utc` is the model tick, while `inserted_at` is the database arrival
time. Both are UTC. `model_version` records which model calculated the reading.

## Alarms

Alarm rules live in `alarmRule`. A statement-level trigger on `parameterReading`
evaluates each batch.

Alarms have hysteresis: three consecutive breaching readings to open one, three
consecutive normal ones to clear it. Without that, randomised telemetry made
alarms flap continuously and `alarmEvent` filled with events that lasted a
single sample.

## Operational endpoints

The gateway and ingestion each serve `/health`, `/ready`, and `/metrics` on the
port in `SERVICE_PORT`. The ports are not published to the host; reach them from
inside the compose network.

`/ready` means the service is doing its job, not merely running. Ingestion is
ready once it has written readings, and not ready while it is reconnecting.

## Running it

The root `README.md` has the quick start. The migrator applies
`sql/migrations/` on every `up`, so a schema change does not need the volume
recreated.

To inspect the data:

```sh
docker exec -it wellhead_db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

```sql
SELECT timestamp_utc, raw_value FROM parameterReading
WHERE wellhead_id = 1 AND parameter_type_id = 1
ORDER BY timestamp_utc DESC LIMIT 10;
```

## Changing one model parameter

The model defaults and valid units are in
`twin_core/model_defaults.json`. An administrator can add one override row,
for example a synthetic downstream pressure of 260 psi for well 1:

```sql
INSERT INTO wellhead_model_parameter
    (wellhead_id, model_version, parameter_key, value, unit, source,
     changed_by, reason, valid_from)
VALUES
    (1, '0.3.0', 'downstream_pressure', 260, 'psi', 'manual',
     'operator-name', 'Synthetic reference-well tuning', clock_timestamp());
```

The name and reason are entered by hand, so this is a record of the change,
not proof of who made it. Twin-core reads the newest effective row for each
well and key on its fleet refresh. A valid change on a running well reports
`restart_required`; restart twin-core after reviewing it. An invalid value
marks that well unavailable. To remove the per-well override, add a
new row with `value` and `unit` both NULL and a later `valid_from`:

```sql
INSERT INTO wellhead_model_parameter
    (wellhead_id, model_version, parameter_key, value, unit, source,
     changed_by, reason, valid_from)
VALUES
    (1, '0.3.0', 'downstream_pressure', NULL, NULL, 'manual',
     'operator-name', 'Return to model default', clock_timestamp());
```

Old rows remain as the change history. Keep `model_version` equal to the
currently running model version. This SQL write needs an administrator role;
the twin service role can only read overrides. Removing an override returns
the key to its lower-precedence value: a global environment override if one
exists, otherwise the model default (including stable per-well variation).

## What the numbers are

The new readings come from a reduced-order synthetic model, not a field device.
Pressure, flow, temperature, water cut, valve state, and pump state now share
one model tick. They are uncalibrated and unsuitable for plant decisions.
Rows written before this change retain `synthetic_random_simulator` in
`source_kind`; new rows carry `synthetic_reduced_order_model`.
