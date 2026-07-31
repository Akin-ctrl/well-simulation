"""Expose synthetic wellhead telemetry through a Modbus TCP server."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
from pymodbus.client.mixin import ModbusClientMixin
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import ServerStop, StartTcpServer

from telemetry_common import (
    HEARTBEAT_REGISTER,
    WORD_ORDER,
    DatabaseSettings,
    ParameterMapping,
    collect_unit_ids,
    configure_logging,
    configure_pymodbus_logging,
    load_parameter_mappings,
    required_env,
)

SIMULATOR_SCRIPT = "wellhead_simulator.py"
REGISTER_BLOCK_SIZE = 2000
SIMULATOR_POLL_SECONDS = 0.1

DATATYPE = ModbusClientMixin.DATATYPE

logger = configure_logging("modbus_gateway")
configure_pymodbus_logging()


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the Modbus gateway."""

    modbus_bind_host: str
    modbus_port: int
    database: DatabaseSettings

    @classmethod
    def from_env(cls) -> Settings:
        """Load and validate service configuration."""
        return cls(
            # Binding to all interfaces is intentional: the gateway is reached
            # by other containers on the compose network, and the port is only
            # published to the host for local inspection.
            modbus_bind_host=required_env("MODBUS_BIND_HOST", "0.0.0.0"),  # noqa: S104
            modbus_port=int(required_env("MODBUS_PORT", "5020")),
            database=DatabaseSettings.from_env(),
        )


def encode_value(value: float, data_type: str) -> list[int]:
    """Encode a Python value into two Modbus registers."""
    if data_type == "float":
        return ModbusClientMixin.convert_to_registers(
            float(value), DATATYPE.FLOAT32, word_order=WORD_ORDER
        )
    if data_type in {"integer", "boolean"}:
        return ModbusClientMixin.convert_to_registers(
            int(value), DATATYPE.INT32, word_order=WORD_ORDER
        )
    raise ValueError(f"Unsupported Modbus data type: {data_type}")


class RegisterStore:
    """Owns the Modbus register image and the mappings that address it.

    Replaces what was previously three module-level mutable globals, so the
    server context can never be observed before it exists.
    """

    def __init__(self, mappings: list[ParameterMapping]) -> None:
        """Build one holding-register context per distinct Modbus unit id."""
        self._by_wellhead: dict[int, dict[str, ParameterMapping]] = {}
        for mapping in mappings:
            self._by_wellhead.setdefault(mapping.wellhead_id, {})[
                mapping.parameter_code
            ] = mapping

        self.unit_ids = collect_unit_ids(mappings)
        self.context = ModbusServerContext(
            devices={
                unit_id: ModbusDeviceContext(
                    hr=ModbusSequentialDataBlock(0, [0] * REGISTER_BLOCK_SIZE)
                )
                for unit_id in self.unit_ids
            },
            single=False,
        )

    def apply_batch(self, telemetry: list[dict[str, object]]) -> None:
        """Write one telemetry batch into the register image."""
        for data_point in telemetry:
            wellhead_id = data_point["wellhead_id"]
            parameters = data_point["parameters"]
            if not isinstance(wellhead_id, int) or not isinstance(parameters, dict):
                raise TypeError("Malformed telemetry batch")

            wellhead_registers = self._by_wellhead.get(wellhead_id)
            if not wellhead_registers:
                logger.warning(
                    "No register mapping for wellhead",
                    extra={"wellhead_id": wellhead_id},
                )
                continue

            for param_code, value in parameters.items():
                mapping = wellhead_registers.get(param_code)
                if mapping is None:
                    continue

                self.context[mapping.modbus_unit_id].setValues(
                    3, mapping.modbus_register, encode_value(value, mapping.data_type)
                )

        self.write_heartbeat(datetime.now(timezone.utc))

    def write_heartbeat(self, timestamp: datetime) -> None:
        """Publish the time of the latest batch to every device context."""
        payload = ModbusClientMixin.convert_to_registers(
            int(timestamp.timestamp()), DATATYPE.UINT32, word_order=WORD_ORDER
        )
        for unit_id in self.unit_ids:
            self.context[unit_id].setValues(3, HEARTBEAT_REGISTER, payload)


def pump_simulator_output(store: RegisterStore, process: subprocess.Popen[str]) -> int:
    """Feed simulator output into the register store until the process exits.

    Returns the simulator's exit code. If the simulator stops, the gateway must
    stop too: continuing to serve the last registers would let ingestion record
    stale values as fresh readings, which is worse than an outage because
    nothing downstream can detect it.
    """
    if process.stdout is None:
        raise RuntimeError("Simulator stdout pipe was not created")

    while True:
        line = process.stdout.readline()
        if line:
            try:
                store.apply_batch(json.loads(line.strip()))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                logger.exception("Failed to process simulator telemetry")

        exit_code = process.poll()
        if exit_code is not None:
            return exit_code

        time.sleep(SIMULATOR_POLL_SECONDS)


def run_simulator_thread(store: RegisterStore) -> None:
    """Run the simulator subprocess and stop the server when it exits."""
    logger.info("Starting simulator subprocess for Modbus register updates")
    # sys.executable rather than a bare "python" so the interpreter is resolved
    # explicitly instead of via PATH lookup.
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, SIMULATOR_SCRIPT],
        stdout=subprocess.PIPE,
        text=True,
    )

    exit_code = pump_simulator_output(store, process)

    logger.error(
        "Simulator exited; stopping the gateway so the telemetry source "
        "cannot go stale unnoticed",
        extra={"simulator_exit_code": exit_code},
    )
    # Ask the Modbus server to shut down cleanly, closing its listening socket.
    # The main thread then returns from StartTcpServer and main() exits non-zero
    # so the container restart policy recovers the service.
    ServerStop()


def main() -> int:
    """Start the Modbus gateway service."""
    settings = Settings.from_env()

    try:
        mappings = load_parameter_mappings(settings.database)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while loading register mappings")
        return 1

    if not mappings:
        logger.error("No active Modbus register mappings found")
        return 1

    # Build the register image before the simulator starts so the first batch
    # always has somewhere to land.
    store = RegisterStore(mappings)
    logger.info(
        "Loaded register mappings",
        extra={"mappings": len(mappings), "unit_ids": store.unit_ids},
    )

    threading.Thread(target=run_simulator_thread, args=(store,), daemon=True).start()

    logger.info(
        "Starting Modbus TCP server",
        extra={
            "bind_host": settings.modbus_bind_host,
            "port": settings.modbus_port,
            "unit_ids": store.unit_ids,
        },
    )
    StartTcpServer(
        context=store.context,
        address=(settings.modbus_bind_host, settings.modbus_port),
    )

    # Reached only once the simulator thread has stopped the server.
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
