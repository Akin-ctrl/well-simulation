"""Expose synthetic wellhead telemetry through a Modbus TCP server."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any

import psycopg2
from pymodbus.constants import Endian
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.server.sync import StartTcpServer

SIMULATOR_SCRIPT = "wellhead_simulator.py"
REGISTER_BLOCK_SIZE = 2000

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("modbus_gateway")

server_context: ModbusServerContext | None = None
register_map: dict[int, dict[str, dict[str, Any]]] = {}


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    modbus_bind_host: str
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
        modbus_bind_host=required_env("MODBUS_BIND_HOST", "0.0.0.0"),
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
        user=settings.db_user,
        password=settings.db_password,
        dbname=settings.db_name,
    )


def build_register_map(settings: Settings) -> dict[int, dict[str, dict[str, Any]]]:
    """Fetch Modbus register mappings from the database."""
    query = """
    SELECT wh.wellhead_id, pt.code, dpm.modbus_register, pt.data_type
    FROM deviceParameterMapping dpm
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    JOIN device d ON dpm.device_id = d.device_id
    JOIN wellHead wh ON d.device_id = wh.device_id
    WHERE dpm.active = TRUE;
    """
    with connect_db(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    mappings: dict[int, dict[str, dict[str, Any]]] = {}
    for wellhead_id, param_code, register, data_type in rows:
        mappings.setdefault(wellhead_id, {})[param_code] = {
            "register": register,
            "type": data_type,
        }

    logger.info("Loaded %s wellhead register maps", len(mappings))
    return mappings


def encode_value(value: float | int, data_type: str) -> list[int]:
    """Encode a Python value into two Modbus registers."""
    builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Little)
    if data_type == "float":
        builder.add_32bit_float(float(value))
    elif data_type in {"integer", "boolean"}:
        builder.add_32bit_int(int(value))
    else:
        raise ValueError(f"Unsupported Modbus data type: {data_type}")
    return builder.to_registers()


def update_registers(wellhead_data_list: list[dict[str, Any]]) -> None:
    """Update Modbus holding registers from a telemetry batch."""
    if server_context is None:
        raise RuntimeError("Modbus server context is not initialized")

    for data_point in wellhead_data_list:
        wellhead_id = data_point["wellhead_id"]
        wellhead_registers = register_map.get(wellhead_id)
        if not wellhead_registers:
            logger.warning("No register mapping for wellhead_id=%s", wellhead_id)
            continue

        for param_code, value in data_point["parameters"].items():
            mapping_info = wellhead_registers.get(param_code)
            if mapping_info is None:
                continue

            payload = encode_value(value, mapping_info["type"])
            server_context[0x00].setValues(
                3,
                mapping_info["register"],
                payload,
            )


def data_updater_thread() -> None:
    """Run the simulator subprocess and update Modbus registers from its JSON output."""
    logger.info("Starting simulator subprocess for Modbus register updates")
    process = subprocess.Popen(
        ["python", SIMULATOR_SCRIPT],
        stdout=subprocess.PIPE,
        text=True,
    )

    if process.stdout is None:
        raise RuntimeError("Simulator stdout pipe was not created")

    while True:
        output = process.stdout.readline()
        if output:
            try:
                update_registers(json.loads(output.strip()))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                logger.exception("Failed to process simulator telemetry")

        if process.poll() is not None:
            logger.error("Simulator process terminated with code %s", process.returncode)
            break
        time.sleep(0.1)


def run_modbus_server(settings: Settings) -> None:
    """Initialize and run the Modbus TCP server."""
    global server_context
    store = ModbusSlaveContext(
        hr=ModbusSequentialDataBlock(0, [0] * REGISTER_BLOCK_SIZE)
    )
    server_context = ModbusServerContext(slaves=store, single=True)

    logger.info(
        "Starting Modbus TCP server on %s:%s",
        settings.modbus_bind_host,
        settings.modbus_port,
    )
    StartTcpServer(
        context=server_context,
        address=(settings.modbus_bind_host, settings.modbus_port),
    )


def main() -> None:
    """Start the Modbus gateway service."""
    global register_map
    settings = load_settings()
    try:
        register_map = build_register_map(settings)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while building register map")
        return

    if not register_map:
        logger.error("No active Modbus register mappings found")
        return

    updater = threading.Thread(target=data_updater_thread, daemon=True)
    updater.start()
    run_modbus_server(settings)


if __name__ == "__main__":
    main()
