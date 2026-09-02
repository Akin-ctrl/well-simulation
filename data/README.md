# Data Layer

The telemetry pipeline and the database schema. The dashboard and API live in
`monorepo/`, and the root `README.md` covers running the whole stack.

## What is here

| Path | What it does |
| --- | --- |
| `src/wellhead_simulator.py` | Makes up readings for each wellhead and writes them to stdout |
| `src/modbus_gateway.py` | Serves those readings as Modbus holding registers |
| `src/database_ingestion.py` | Polls the gateway and writes readings to the historian |
| `src/schema.py` | Applies the ordered SQL migrations |
| `src/telemetry_common.py` | Config, logging, database access, and the health endpoints |
| `src/telemetry_metrics.py` | Prometheus metric definitions |
| `sql/migrations/` | The schema. Canonical, per ADR 0006 |
| `sql/seeds/` | Demo fleet and accounts, applied after the migrations |

## How the data flows

The simulator writes a JSON batch to stdout every telemetry interval. The
gateway reads that on a pipe and encodes each value into two Modbus registers.
Ingestion polls those registers over Modbus TCP and batch-inserts the decoded
values.

The simulator runs as a child process of the gateway, so they share a container.
The gateway exits when the simulator does, because a gateway with no source
would keep serving its last registers and ingestion would record them as fresh.

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

The gateway writes the time of its last batch to register 1900. Ingestion reads
that first, and skips the insert if it has not advanced.

Without it, a stalled simulator meant the gateway kept answering reads with
frozen values and ingestion kept recording them under new timestamps. The
dashboard showed a healthy fleet on data that had stopped moving. That is worse
than an outage, because nothing downstream could detect it.

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

## What the numbers are

The readings are random values inside each parameter's configured range, with
about one in ten pushed outside it so the alarm rules have something to fire on.

There is no physics here. Nothing links tubing pressure to flow rate, and
closing a choke would change nothing, because there is no choke. Replacing this
with a model that holds state is the next major piece of work, and the point at
which the project could reasonably be called a digital twin.
