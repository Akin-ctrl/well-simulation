# Data Layer

The telemetry pipeline and the database schema. The dashboard and API live in
`monorepo/`, and the root `README.md` covers running the whole stack.

## What is here

| Path | What it does |
| --- | --- |
| `src/wellhead_simulator.py` | Legacy random simulator, no longer started by Compose |
| `src/modbus_gateway.py` | Fetches model snapshots and serves supported values as holding registers |
| `src/database_ingestion.py` | Polls Modbus and writes model readings to the historian |
| `src/schema.py` | Installs, checks, adopts, or extends the current schema |
| `src/model_source.py` | Validates model snapshots and maps all 18 seeded signals |
| `src/telemetry_common.py` | Config, logging, database access, and the health endpoints |
| `src/telemetry_metrics.py` | Prometheus metric definitions |
| `sql/schema.sql` | The complete current schema, per ADR 0006 |
| `sql/schema-contract.json` | Generated comparison record and legacy checksums |
| `sql/seeds/` | Demo fleet and accounts, applied after schema verification |

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

The gateway and ingestion re-read mappings every telemetry interval (five
seconds by default). Twin-core refreshes its active fleet and parameter
overrides every 30 seconds. Alarm evaluation reads current rules in the
database. ADR 0035 records why timed refresh was chosen over a notification
or a reload endpoint.

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

The root `README.md` has the quick start. The service still named `migrator`
runs the installer. It installs `sql/schema.sql` on an empty database or checks
an already adopted database. It refuses an unexpected or incomplete schema.
Compose and CI pin PostgreSQL 16.15 with TimescaleDB 2.30.2. An existing
PostgreSQL 14 volume needs the [engine upgrade procedure](../docs/design/database-engine-upgrade.md)
before it can run with this image. A new empty installation uses the current
image directly.

### Existing database adoption

Back up the database and test restoring that backup into a disposable database
before adoption. An old TimescaleDB 2.19.3 database needs the physical-backup
procedure because its compressed-null logical restore is broken. Upgrade its
engine before comparing it with the current generated reference.
Follow the [adoption design](../docs/design/consolidated-schema.md)
for the full checks. The original volume stays in place.

```sh
docker compose build migrator
docker compose run --rm --no-deps migrator python schema.py adopt
docker compose run --rm --no-deps migrator python schema.py check
```

`adopt` checks all 13 legacy schema checksums and the actual database structure.
It records the new baseline without replaying schema SQL. Existing ledger rows,
readings, controls, and account records remain. Configured service grants are
then provisioned. The usual startup command defaults to `init`.

`check` reads identity, history, and structure without applying SQL or grants.
Use `--without-seeds` with `init` to omit demo records and the bootstrap account.
That flag does not remove existing records.

### Changing the schema

Edit `sql/schema.sql`. Generate `sql/schema-contract.json` from a fresh disposable
database using `python data/src/schema.py build-reference --contract-output PATH`,
with `POSTGRES_*` pointing to that disposable database. The command refuses a
populated application schema. Review the generated differences and run
`SCHEMA_DATABASE_TEST=1 python -m pytest data/tests/test_schema_database.py`.
The tests use `TEST_POSTGRES_HOST`, `TEST_POSTGRES_PORT`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD`, creating and deleting only their own random database names.

Do not edit the generated record by hand. It records structure and the source
checksum, not a second schema to maintain. Keep the Drizzle mirror consistent.
An existing database whose baseline differs needs a separately reviewed
conversion plan. Editing the file does not automatically upgrade stored data.

An interrupted installation stays marked incomplete. Automatic retries do not
resume statements or delete objects. Inspect the saved progress and recover
from a verified backup, or explicitly rebuild an expendable test database.

### Changing the database engine

Follow the [engine upgrade design](../docs/design/database-engine-upgrade.md).
It separates a PostgreSQL major-version upgrade from the TimescaleDB extension
update and verifies both on a copy before changing the running stack.
Save the previous generated contract before building the new one.
After the upgrade, use accept-engine --previous-contract PATH to verify and
record the changed engine identity. Normal startup never accepts it implicitly.

TIMESCALE_DATA_VOLUME selects the verified converted Docker volume. Leave it
empty for a new stack to use its normal project-specific volume. Keeping the
old default name makes an unconverted PostgreSQL 14 volume fail to start under
PostgreSQL 16, rather than silently starting an empty historian.

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
`twin_core/model_defaults.json`. An ADMIN can now create a parameter proposal through
POST /api/twin/parameter-changes and explicitly publish it. The API validates
the proposed value and unit through twin-core and records the authenticated
actor. Publication inserts an append-only wellhead_model_parameter row and
its audit event in one transaction. See the
[8B operating steps](../docs/design/twin-core-milestone-8b.md#operating-the-api).

A published parameter does not change a running model during a tick. Fleet
refresh marks a changed model restart_required. Restart twin-core after
reviewing the publication. To remove an override, publish null for both value
and unit. The lower-precedence environment or default value then applies.

Manual SQL remains possible for the database administrator, but bypasses the
application's verified identity and review steps. Normal operation uses the
proposal API. Historical manual records keep their original actor text.

## What the numbers are

The new readings come from a reduced-order synthetic model, not a field device.
Pressure, flow, temperature, water cut, valve state, and pump state now share
one model tick. They are uncalibrated and unsuitable for plant decisions.
Rows written before this change retain `synthetic_random_simulator` in
`source_kind`; new rows carry `synthetic_reduced_order_model`.

### Add ordinary tables to an existing installation

The `extend` command checks the installed schema against its previous committed
SQL and contract, then applies only new ordinary tables and their indexes from
the current canonical SQL. It verifies and audits the new identity in the same
transaction. Keep temporary copies of the installed SQL and contract, take a
private backup, and verify a restored copy first. See the [extension workflow](../docs/design/schema-extension.md) and [command example](../docs/design/consolidated-schema.md#adding-new-ordinary-tables).

### Alarm lifecycle records

`alarm_action` stores immutable reviewed input and its result.
`alarm_acknowledgement` identifies one event by its bigint ID and exact trigger
time. `alarm_shelf` stores the current well/rule revision and deadline. All three
come from the single canonical SQL file. The API can insert receipts and
acknowledgements but cannot change or delete existing ones. Its shelf UPDATE
grant names only the revision, deadline, updated time and receipt columns.

Operator actions never update `alarmevent`. Its 90-day retention remains in
place, while action receipts and acknowledgement metadata remain durable. See
the [alarm design](../docs/design/alarm-lifecycle.md) for expiry and audit behavior.
