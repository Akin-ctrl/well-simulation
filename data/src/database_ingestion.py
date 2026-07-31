"""Poll Modbus registers and persist decoded readings to TimescaleDB."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from pymodbus.client import ModbusTcpClient
from pymodbus.client.mixin import ModbusClientMixin

from telemetry_common import (
    HEARTBEAT_REGISTER,
    WORD_ORDER,
    DatabaseSettings,
    ParameterMapping,
    as_number,
    configure_logging,
    configure_pymodbus_logging,
    connect_db,
    load_parameter_mappings,
    required_env,
    telemetry_interval_seconds,
)

RECONNECT_DELAY_SECONDS = 10
REGISTERS_PER_VALUE = 2

DATATYPE = ModbusClientMixin.DATATYPE

INSERT_READING_SQL = """
INSERT INTO parameterReading (
    timestamp_utc, wellhead_id, parameter_type_id, mapping_id, raw_value
) VALUES (%s, %s, %s, %s, %s)
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

    def as_row(self) -> tuple[datetime, int, int, int, float]:
        """Return the tuple expected by INSERT_READING_SQL."""
        return (
            self.timestamp_utc,
            self.wellhead_id,
            self.parameter_type_id,
            self.mapping_id,
            self.raw_value,
        )


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
    """Decide whether the gateway has published anything new since the last poll.

    A telemetry source that has stopped updating still answers reads with its
    last values. Recording those as new readings would silently invent data, so
    an unadvanced heartbeat means the cycle is skipped.

    An unreadable heartbeat is not treated as stale: that indicates a transport
    problem, which the register reads will surface on their own.
    """
    return heartbeat is not None and heartbeat == last_heartbeat


def read_heartbeat(client: ModbusTcpClient, unit_id: int) -> int | None:
    """Read the gateway's last-batch timestamp, or None if it is unavailable."""
    result = client.read_holding_registers(
        HEARTBEAT_REGISTER, count=REGISTERS_PER_VALUE, device_id=unit_id
    )
    if result.isError():
        logger.warning("Modbus heartbeat read failed", extra={"unit_id": unit_id})
        return None

    heartbeat = int(
        as_number(
            ModbusClientMixin.convert_from_registers(
                result.registers, DATATYPE.UINT32, word_order=WORD_ORDER
            ),
            "heartbeat",
        )
    )

    # An untouched register block reads as zero, meaning the gateway has not
    # published a batch yet or is too old to publish one at all. Say so plainly
    # rather than letting it look like an indefinite stall.
    if heartbeat == 0:
        logger.error(
            "Gateway is not publishing a heartbeat; freshness cannot be verified",
            extra={"unit_id": unit_id, "register": HEARTBEAT_REGISTER},
        )
        return None

    return heartbeat


def poll_once(
    client: ModbusTcpClient,
    mappings: list[ParameterMapping],
    timestamp: datetime,
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
            logger.warning(
                "Modbus read failed",
                extra={
                    "mapping_id": mapping.mapping_id,
                    "register": mapping.modbus_register,
                },
            )
            continue

        value = decode_registers(result.registers, mapping.data_type)
        if value is None:
            continue

        readings.append(
            Reading(
                timestamp_utc=timestamp,
                wellhead_id=mapping.wellhead_id,
                parameter_type_id=mapping.parameter_type_id,
                mapping_id=mapping.mapping_id,
                raw_value=value,
            )
        )

    return readings


def ingest_forever(
    settings: Settings,
    mappings: list[ParameterMapping],
    client: ModbusTcpClient,
) -> None:
    """Poll and persist until the connection fails, then let the caller retry."""
    heartbeat_unit_id = mappings[0].modbus_unit_id
    last_heartbeat: int | None = None

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

            heartbeat = read_heartbeat(client, heartbeat_unit_id)
            if is_stale(heartbeat, last_heartbeat):
                logger.warning(
                    "Skipping poll: telemetry is stale",
                    extra={
                        "heartbeat": heartbeat,
                        "age_seconds": round(now.timestamp() - (heartbeat or 0)),
                    },
                )
            else:
                last_heartbeat = heartbeat
                readings = poll_once(client, mappings, now)
                if readings:
                    execute_batch(
                        cursor, INSERT_READING_SQL, [r.as_row() for r in readings]
                    )
                    conn.commit()
                    logger.info(
                        "Inserted parameter readings", extra={"count": len(readings)}
                    )

            remaining = settings.poll_interval_seconds - (time.time() - started_at)
            if remaining > 0:
                time.sleep(remaining)
    finally:
        if not conn.closed:
            conn.close()


def run_ingestion_loop(settings: Settings, mappings: list[ParameterMapping]) -> None:
    """Run the ingestion loop, reconnecting after transport or database failures."""
    client = ModbusTcpClient(settings.modbus_host, port=settings.modbus_port)

    while True:
        try:
            ingest_forever(settings, mappings, client)
        except Exception:
            logger.exception(
                "Ingestion loop failed; reconnecting",
                extra={"retry_in_seconds": RECONNECT_DELAY_SECONDS},
            )
            if client.is_socket_open():
                client.close()
            time.sleep(RECONNECT_DELAY_SECONDS)


def main() -> int:
    """Start the database ingestion service."""
    settings = Settings.from_env()
    logger.info("Starting database ingestion service")

    try:
        mappings = load_parameter_mappings(settings.database)
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
    run_ingestion_loop(settings, mappings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
