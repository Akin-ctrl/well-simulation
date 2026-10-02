"""Poll Modbus registers and persist decoded readings to TimescaleDB."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from pymodbus.client import ModbusTcpClient
from pymodbus.client.mixin import ModbusClientMixin

from model_source import supported_mappings
from telemetry_common import (
    HEARTBEAT_REGISTER,
    STATUS_BLOCK_REGISTERS,
    WORD_ORDER,
    DatabaseSettings,
    MetadataReloader,
    ModelVersion,
    ParameterMapping,
    ServiceHealth,
    as_number,
    collect_unit_ids,
    configure_logging,
    configure_pymodbus_logging,
    connect_db,
    load_parameter_mappings,
    mappings_changed,
    required_env,
    service_port,
    start_service_endpoints,
    telemetry_interval_seconds,
)
from telemetry_metrics import (
    INGESTION_RECONNECTS,
    METADATA_RELOADS,
    MODBUS_READ_ERRORS,
    POLL_DURATION,
    POLLS_SKIPPED_STALE,
    READINGS_WRITTEN,
    TELEMETRY_AGE,
)

# Backoff between reconnection attempts. Doubles on repeated failure so a
# database that is down is not hammered once every ten seconds indefinitely.
RECONNECT_DELAY_SECONDS = 5
MAX_RECONNECT_DELAY_SECONDS = 120
REGISTERS_PER_VALUE = 2
DEFAULT_SERVICE_PORT = 9101

DATATYPE = ModbusClientMixin.DATATYPE

INSERT_READING_SQL = """
INSERT INTO parameterReading (
    timestamp_utc, wellhead_id, parameter_type_id, mapping_id, raw_value, source_kind,
    model_version
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

logger = configure_logging("database_ingestion")
configure_pymodbus_logging()


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the ingestion service."""

    modbus_host: str
    modbus_port: int
    poll_interval_seconds: int
    database: DatabaseSettings

    @classmethod
    def from_env(cls) -> Settings:
        """Load and validate service configuration."""
        return cls(
            modbus_host=required_env("MODBUS_HOST", "modbus"),
            modbus_port=int(required_env("MODBUS_PORT", "5020")),
            poll_interval_seconds=telemetry_interval_seconds(),
            database=DatabaseSettings.from_env(),
        )


@dataclass(frozen=True)
class Reading:
    """One decoded parameter value ready for insertion."""

    timestamp_utc: datetime
    wellhead_id: int
    parameter_type_id: int
    mapping_id: int
    raw_value: float
    model_version: ModelVersion

    def as_row(self) -> tuple[datetime, int, int, int, float, str, str]:
        """Return the tuple expected by INSERT_READING_SQL."""
        return (
            self.timestamp_utc,
            self.wellhead_id,
            self.parameter_type_id,
            self.mapping_id,
            self.raw_value,
            "synthetic_reduced_order_model",
            str(self.model_version),
        )


@dataclass(frozen=True)
class GatewayStatus:
    """The tick time and model version the gateway published together."""

    heartbeat: int
    model_version: ModelVersion


def decode_registers(registers: list[int], data_type: str) -> float | None:
    """Decode two Modbus registers according to the configured parameter type."""
    if data_type == "float":
        return as_number(
            ModbusClientMixin.convert_from_registers(
                registers, DATATYPE.FLOAT32, word_order=WORD_ORDER
            ),
            data_type,
        )
    if data_type in {"integer", "boolean"}:
        return as_number(
            ModbusClientMixin.convert_from_registers(
                registers, DATATYPE.INT32, word_order=WORD_ORDER
            ),
            data_type,
        )
    logger.warning("Unsupported parameter data type", extra={"data_type": data_type})
    return None


def is_stale(heartbeat: int | None, last_heartbeat: int | None) -> bool:
    """Skip a missing or unchanged model tick."""
    return heartbeat is None or heartbeat == last_heartbeat


def read_status(client: ModbusTcpClient, unit_id: int) -> GatewayStatus | None:
    """Read the tick time and model version, or None while either is unpublished.

    One Read Holding Registers request covers the whole status block (offset
    1900, quantity 5), so the heartbeat and the version come from one moment on
    the server.
    """
    result = client.read_holding_registers(
        HEARTBEAT_REGISTER, count=STATUS_BLOCK_REGISTERS, device_id=unit_id
    )
    if result.isError():
        MODBUS_READ_ERRORS.labels(kind="heartbeat").inc()
        logger.warning("Modbus heartbeat read failed", extra={"unit_id": unit_id})
        return None

    heartbeat = int(
        as_number(
            ModbusClientMixin.convert_from_registers(
                result.registers[:REGISTERS_PER_VALUE],
                DATATYPE.UINT32,
                word_order=WORD_ORDER,
            ),
            "heartbeat",
        )
    )

    # An untouched register block reads as zero, meaning the gateway has not
    # published a batch yet or is too old to publish one at all. Say so plainly
    # rather than letting it look like an indefinite stall.
    if heartbeat == 0:
        MODBUS_READ_ERRORS.labels(kind="heartbeat").inc()
        logger.error(
            "Gateway is not publishing a heartbeat; freshness cannot be verified",
            extra={"unit_id": unit_id, "register": HEARTBEAT_REGISTER},
        )
        return None

    # A heartbeat without a version breaks the gateway's contract, which writes
    # both in one call. Treat it like a zero heartbeat: nothing is stored.
    try:
        model_version = ModelVersion.from_registers(
            list(result.registers[REGISTERS_PER_VALUE:])
        )
    except ValueError:
        model_version = None
    if model_version is None:
        MODBUS_READ_ERRORS.labels(kind="heartbeat").inc()
        logger.error(
            "Gateway published a heartbeat without a valid model version",
            extra={"unit_id": unit_id},
        )
        return None

    return GatewayStatus(heartbeat, model_version)


def read_fleet_status(
    client: ModbusTcpClient, mappings: list[ParameterMapping]
) -> GatewayStatus | None:
    """Accept a tick only when every mapped unit reports the same tick and version."""
    common: GatewayStatus | None = None
    for unit_id in collect_unit_ids(mappings):
        status = read_status(client, unit_id)
        if status is None:
            return None
        if common is not None and status != common:
            logger.warning("Modbus units report different model ticks or versions")
            return None
        common = status
    return common


def poll_once(
    client: ModbusTcpClient,
    mappings: list[ParameterMapping],
    timestamp: datetime,
    model_version: ModelVersion,
) -> list[Reading]:
    """Read every mapped register once and return the decoded readings."""
    readings: list[Reading] = []

    for mapping in mappings:
        result = client.read_holding_registers(
            mapping.modbus_register,
            count=REGISTERS_PER_VALUE,
            device_id=mapping.modbus_unit_id,
        )
        if result.isError():
            MODBUS_READ_ERRORS.labels(kind="register").inc()
            logger.warning(
                "Modbus read failed",
                extra={
                    "mapping_id": mapping.mapping_id,
                    "register": mapping.modbus_register,
                },
            )
            raise ConnectionError("Modbus parameter read failed")

        value = decode_registers(result.registers, mapping.data_type)
        if value is None:
            raise ValueError("Unsupported parameter type in active model mapping")
        if not math.isfinite(value):
            raise ValueError("Nonfinite Modbus model reading")
        if mapping.data_type == "boolean" and value not in (0, 1):
            raise ValueError("Invalid Modbus boolean reading")
        if mapping.parameter_code == "choke_valve_position" and not 0 <= value <= 100:
            raise ValueError("Invalid Modbus choke position")
        if mapping.parameter_code != "wellhead_temperature" and value < 0:
            raise ValueError("Negative Modbus model reading")

        readings.append(
            Reading(
                timestamp_utc=timestamp,
                wellhead_id=mapping.wellhead_id,
                parameter_type_id=mapping.parameter_type_id,
                mapping_id=mapping.mapping_id,
                raw_value=value,
                model_version=model_version,
            )
        )

    return readings


def apply_metadata_reload(
    reloader: MetadataReloader, current: list[ParameterMapping]
) -> list[ParameterMapping]:
    """Reload the mappings if due, and adopt them if they changed.

    ADR 0035. Returns the mappings now in force, which are the previous ones
    when the reload is not due, the read failed, or nothing changed.
    """
    if not reloader.is_due():
        return current

    reloaded = reloader.reload()
    if reloaded is None:
        METADATA_RELOADS.labels(result="failed").inc()
        return current

    reloaded = supported_mappings(reloaded)
    if not mappings_changed(current, reloaded):
        METADATA_RELOADS.labels(result="unchanged").inc()
        return current

    METADATA_RELOADS.labels(result="changed").inc()
    logger.info("Parameter mappings reloaded", extra={"mappings": len(reloaded)})
    return reloaded


def current_heartbeat(
    heartbeat: int | None, now: datetime, interval_seconds: int
) -> int | None:
    """Reject old or future model ticks before polling mapped registers."""
    if heartbeat is None:
        return None
    age = now.timestamp() - heartbeat
    if age > max(interval_seconds * 2, 5) or age < -2:
        return None
    return heartbeat


def ingest_forever(
    settings: Settings,
    mappings: list[ParameterMapping],
    client: ModbusTcpClient,
    health: ServiceHealth,
) -> None:
    """Poll and persist until the connection fails, then let the caller retry."""
    last_heartbeat: int | None = None
    reloader = MetadataReloader(settings.database, settings.poll_interval_seconds)

    conn = connect_db(settings.database)
    try:
        cursor = conn.cursor()

        if not client.connect():
            raise ConnectionError(
                f"Unable to connect to Modbus server at "
                f"{settings.modbus_host}:{settings.modbus_port}"
            )

        while True:
            started_at = time.time()
            now = datetime.now(timezone.utc)

            mappings = apply_metadata_reload(reloader, mappings)

            status = read_fleet_status(client, mappings)
            heartbeat = status.heartbeat if status is not None else None
            if heartbeat is not None:
                TELEMETRY_AGE.set(max(0.0, now.timestamp() - heartbeat))

            heartbeat = current_heartbeat(
                heartbeat, now, settings.poll_interval_seconds
            )
            if is_stale(heartbeat, last_heartbeat):
                health.ready = False
                POLLS_SKIPPED_STALE.inc()
                logger.warning(
                    "Skipping poll: telemetry is stale",
                    extra={
                        "heartbeat": heartbeat,
                        "age_seconds": (
                            round(now.timestamp() - heartbeat) if heartbeat else None
                        ),
                    },
                )
            else:
                if heartbeat is None or status is None:
                    raise ValueError("Missing heartbeat before Modbus poll")
                source_time = datetime.fromtimestamp(heartbeat, timezone.utc)
                with POLL_DURATION.time():
                    readings = poll_once(
                        client, mappings, source_time, status.model_version
                    )
                # The version must match too: a twin-core redeploy between the
                # two reads would otherwise label the batch with the wrong model.
                ending_status = read_fleet_status(client, mappings)
                if ending_status != status:
                    health.ready = False
                    POLLS_SKIPPED_STALE.inc()
                    logger.warning("Discarding poll changed during Modbus reads")
                elif len(readings) != len(mappings):
                    raise ValueError("Incomplete Modbus model batch")
                else:
                    execute_batch(
                        cursor, INSERT_READING_SQL, [r.as_row() for r in readings]
                    )
                    conn.commit()
                    last_heartbeat = heartbeat
                    READINGS_WRITTEN.inc(len(readings))
                    # Only now is the service demonstrably doing its job.
                    health.ready = True
                    logger.info(
                        "Inserted parameter readings", extra={"count": len(readings)}
                    )

            remaining = settings.poll_interval_seconds - (time.time() - started_at)
            if remaining > 0:
                time.sleep(remaining)
    finally:
        if not conn.closed:
            conn.close()


def run_ingestion_loop(
    settings: Settings, mappings: list[ParameterMapping], health: ServiceHealth
) -> None:
    """Run the ingestion loop, reconnecting after transport or database failures.

    The delay doubles on each consecutive failure up to a ceiling. A fixed delay
    meant a database that was down got a reconnection attempt every ten seconds
    for as long as it stayed down.
    """
    client = ModbusTcpClient(settings.modbus_host, port=settings.modbus_port)
    delay = RECONNECT_DELAY_SECONDS

    while True:
        try:
            ingest_forever(settings, mappings, client, health)
        except Exception:
            INGESTION_RECONNECTS.inc()
            # Not ready while disconnected, so readiness reflects whether the
            # service can actually ingest rather than whether it is running.
            health.ready = False
            logger.exception(
                "Ingestion loop failed; reconnecting",
                extra={"retry_in_seconds": delay},
            )
            if client.is_socket_open():
                client.close()
            time.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)
        else:
            delay = RECONNECT_DELAY_SECONDS


def main() -> int:
    """Start the database ingestion service."""
    settings = Settings.from_env()
    logger.info("Starting database ingestion service")

    try:
        mappings = supported_mappings(load_parameter_mappings(settings.database))
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while loading mappings")
        return 1

    if not mappings:
        logger.error("No ingestion metadata found in database")
        return 1

    logger.info(
        "Loaded parameter mappings",
        extra={
            "mappings": len(mappings),
            "interval_seconds": settings.poll_interval_seconds,
        },
    )

    health = ServiceHealth(detail={"mappings": len(mappings)})
    start_service_endpoints(
        "database_ingestion", health, service_port(DEFAULT_SERVICE_PORT)
    )

    run_ingestion_loop(settings, mappings, health)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
