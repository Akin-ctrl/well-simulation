"""Poll Modbus registers and persist decoded readings to TimescaleDB."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import execute_batch
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

POLL_INTERVAL_SECONDS = 5
RECONNECT_DELAY_SECONDS = 10

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("database_ingestion")


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    modbus_host: str
    modbus_port: int
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def required_env(name: str, default: str | None = None) -> str:
    """Return an environment variable or raise a clear configuration error."""
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    """Load and validate service configuration."""
    return Settings(
        modbus_host=required_env("MODBUS_HOST", "modbus"),
        modbus_port=int(required_env("MODBUS_PORT", "5020")),
        db_host=required_env("POSTGRES_HOST", "db"),
        db_port=int(required_env("POSTGRES_PORT", "5432")),
        db_name=required_env("POSTGRES_DB"),
        db_user=required_env("POSTGRES_USER"),
        db_password=required_env("POSTGRES_PASSWORD"),
    )


def connect_db(settings: Settings):
    """Open a PostgreSQL connection using service settings."""
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def get_ingestion_metadata(settings: Settings) -> list[dict[str, Any]]:
    """Fetch Modbus polling metadata from the database."""
    query = """
    SELECT dpm.mapping_id, wh.wellhead_id, pt.parameter_type_id, dpm.modbus_register, pt.data_type
    FROM deviceParameterMapping dpm
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    JOIN device d ON dpm.device_id = d.device_id
    JOIN wellHead wh ON d.device_id = wh.device_id
    WHERE dpm.active = TRUE
    ORDER BY dpm.modbus_register;
    """
    with connect_db(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return [
        {
            "mapping_id": row[0],
            "wellhead_id": row[1],
            "param_type_id": row[2],
            "register": row[3],
            "type": row[4],
        }
        for row in rows
    ]


def decode_registers(registers: list[int], data_type: str) -> float | None:
    """Decode two Modbus registers according to the configured parameter type."""
    decoder = BinaryPayloadDecoder.fromRegisters(
        registers,
        byteorder=Endian.Big,
        wordorder=Endian.Little,
    )
    if data_type == "float":
        return decoder.decode_32bit_float()
    if data_type in {"integer", "boolean"}:
        return float(decoder.decode_32bit_int())
    logger.warning("Unsupported parameter data type: %s", data_type)
    return None


def run_ingestion_loop(settings: Settings, ingestion_map: list[dict[str, Any]]) -> None:
    """Continuously poll Modbus and batch-insert readings."""
    insert_sql = """
        INSERT INTO parameterReading (timestamp_utc, wellhead_id, parameter_type_id, mapping_id, raw_value)
        VALUES (%s, %s, %s, %s, %s)
    """
    client = ModbusTcpClient(settings.modbus_host, port=settings.modbus_port)

    while True:
        conn = None
        try:
            conn = connect_db(settings)
            cursor = conn.cursor()

            if not client.connect():
                raise ConnectionError(
                    f"Unable to connect to Modbus server at {settings.modbus_host}:{settings.modbus_port}"
                )

            while True:
                start_time = time.time()
                records_to_insert = []
                current_timestamp = datetime.now(timezone.utc)

                for item in ingestion_map:
                    result = client.read_holding_registers(
                        item["register"],
                        2,
                        slave=1,
                    )
                    if result.isError():
                        logger.warning(
                            "Modbus read failed for mapping_id=%s register=%s",
                            item["mapping_id"],
                            item["register"],
                        )
                        continue

                    value = decode_registers(result.registers, item["type"])
                    if value is None:
                        continue

                    records_to_insert.append(
                        (
                            current_timestamp,
                            item["wellhead_id"],
                            item["param_type_id"],
                            item["mapping_id"],
                            value,
                        )
                    )

                if records_to_insert:
                    execute_batch(cursor, insert_sql, records_to_insert)
                    conn.commit()
                    logger.info("Inserted %s parameter readings", len(records_to_insert))

                time_to_wait = POLL_INTERVAL_SECONDS - (time.time() - start_time)
                if time_to_wait > 0:
                    time.sleep(time_to_wait)
        except Exception:
            logger.exception(
                "Ingestion loop failed; reconnecting in %s seconds",
                RECONNECT_DELAY_SECONDS,
            )
            if client.is_socket_open():
                client.close()
            if conn is not None and not conn.closed:
                conn.close()
            time.sleep(RECONNECT_DELAY_SECONDS)


def main() -> None:
    """Start the database ingestion service."""
    settings = load_settings()
    logger.info("Starting database ingestion service")

    try:
        ingestion_map = get_ingestion_metadata(settings)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while loading ingestion metadata")
        return

    if not ingestion_map:
        logger.error("No ingestion metadata found in database")
        return

    logger.info("Loaded %s parameter mappings for ingestion", len(ingestion_map))
    run_ingestion_loop(settings, ingestion_map)


if __name__ == "__main__":
    main()
