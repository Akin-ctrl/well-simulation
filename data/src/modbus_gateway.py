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
    MetadataReloader,
    ParameterMapping,
    ServiceHealth,
    collect_unit_ids,
    configure_logging,
    configure_pymodbus_logging,
    load_parameter_mappings,
    mappings_changed,
    required_env,
    service_port,
    start_service_endpoints,
    telemetry_interval_seconds,
)
from telemetry_metrics import BATCH_ERRORS, BATCHES_APPLIED, METADATA_RELOADS

SIMULATOR_SCRIPT = "wellhead_simulator.py"
REGISTER_BLOCK_SIZE = 2000
SIMULATOR_POLL_SECONDS = 0.1
DEFAULT_SERVICE_PORT = 9102

# pymodbus offsets a device context by one, so a block of N registers starting
# at address 0 accepts N-1 values from that address. Writing the full N returns
# an exception code and changes nothing.
USABLE_REGISTERS = REGISTER_BLOCK_SIZE - 1

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


class RegisterWriteError(RuntimeError):
    """A Modbus register write was rejected by the datastore."""


class RegisterStore:
    """Owns the Modbus register image and the mappings that address it.

    Replaces what was previously three module-level mutable globals, so the
    server context can never be observed before it exists.
    """

    def __init__(self, mappings: list[ParameterMapping]) -> None:
        """Build one holding-register context per distinct Modbus unit id."""
        self._by_wellhead: dict[int, dict[str, ParameterMapping]] = {}
        self._index_mappings(mappings)

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

    def _write(self, unit_id: int, address: int, values: list[int]) -> None:
        """Write holding registers, raising if the datastore rejects it.

        `setValues` reports a bad address by returning a Modbus exception code
        rather than raising, so an unchecked call fails silently. That is the
        same class of problem as serving stale telemetry: the system carries on
        looking correct while the data is wrong.
        """
        result = self.context[unit_id].setValues(3, address, values)
        if result is not None:
            raise RegisterWriteError(
                f"unit {unit_id} rejected a write of {len(values)} registers "
                f"at address {address} (modbus exception {result})"
            )

    def replace_mappings(self, mappings: list[ParameterMapping]) -> None:
        """Adopt a new mapping set and clear the register image.

        ADR 0035 clears the whole block rather than only the registers whose
        mapping moved. A value left at an address nothing maps any more is a
        stale reading on a live Modbus interface, and working out exactly which
        addresses to clear is the kind of bookkeeping that fails silently. The
        cost is that every register reads zero until the next telemetry batch.
        """
        self._index_mappings(mappings)
        previous_unit_ids = self.unit_ids
        self.unit_ids = collect_unit_ids(mappings)

        # A unit id that appears for the first time needs its own register
        # image. The running server holds a reference to this context, so
        # devices are added to it rather than the context being replaced.
        for unit_id in self.unit_ids:
            if unit_id not in previous_unit_ids:
                self.context[unit_id] = ModbusDeviceContext(
                    hr=ModbusSequentialDataBlock(0, [0] * REGISTER_BLOCK_SIZE)
                )

        for unit_id in self.unit_ids:
            self._write(unit_id, 0, [0] * USABLE_REGISTERS)

    def _index_mappings(self, mappings: list[ParameterMapping]) -> None:
        """Rebuild the wellhead and parameter lookup from a mapping set."""
        self._by_wellhead = {}
        for mapping in mappings:
            self._by_wellhead.setdefault(mapping.wellhead_id, {})[
                mapping.parameter_code
            ] = mapping

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

                self._write(
                    mapping.modbus_unit_id,
                    mapping.modbus_register,
                    encode_value(value, mapping.data_type),
                )

        self.write_heartbeat(datetime.now(timezone.utc))

    def write_heartbeat(self, timestamp: datetime) -> None:
        """Publish the time of the latest batch to every device context."""
        payload = ModbusClientMixin.convert_to_registers(
            int(timestamp.timestamp()), DATATYPE.UINT32, word_order=WORD_ORDER
        )
        for unit_id in self.unit_ids:
            self._write(unit_id, HEARTBEAT_REGISTER, payload)


def apply_metadata_reload(
    store: RegisterStore, reloader: MetadataReloader, current: list[ParameterMapping]
) -> list[ParameterMapping]:
    """Reload the mappings if due, and adopt them if they changed.

    Returns the mappings now in force, which are the previous ones when the
    reload is not due, the read failed, or nothing changed.
    """
    if not reloader.is_due():
        return current

    reloaded = reloader.reload()
    if reloaded is None:
        METADATA_RELOADS.labels(result="failed").inc()
        return current

    if not mappings_changed(current, reloaded):
        METADATA_RELOADS.labels(result="unchanged").inc()
        return current

    METADATA_RELOADS.labels(result="changed").inc()
    store.replace_mappings(reloaded)
    logger.info(
        "Register mappings reloaded; register image cleared",
        extra={"mappings": len(reloaded), "unit_ids": store.unit_ids},
    )
    return reloaded


def pump_simulator_output(
    store: RegisterStore,
    process: subprocess.Popen[str],
    reloader: MetadataReloader,
    mappings: list[ParameterMapping],
    health: ServiceHealth,
) -> int:
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
                BATCH_ERRORS.inc()
                logger.exception("Failed to process simulator telemetry")
            else:
                BATCHES_APPLIED.inc()
                # Serving real values is what makes the gateway useful, so
                # readiness starts at the first batch rather than at startup.
                health.ready = True

        mappings = apply_metadata_reload(store, reloader, mappings)

        exit_code = process.poll()
        if exit_code is not None:
            return exit_code

        time.sleep(SIMULATOR_POLL_SECONDS)


def run_simulator_thread(
    store: RegisterStore,
    reloader: MetadataReloader,
    mappings: list[ParameterMapping],
    health: ServiceHealth,
) -> None:
    """Run the simulator subprocess and stop the server when it exits."""
    logger.info("Starting simulator subprocess for Modbus register updates")
    # sys.executable rather than a bare "python" so the interpreter is resolved
    # explicitly instead of via PATH lookup.
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, SIMULATOR_SCRIPT],
        stdout=subprocess.PIPE,
        text=True,
    )

    exit_code = pump_simulator_output(store, process, reloader, mappings, health)

    health.alive = False
    health.ready = False
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

    health = ServiceHealth(detail={"unit_ids": store.unit_ids})
    start_service_endpoints(
        "modbus_gateway", health, service_port(DEFAULT_SERVICE_PORT)
    )

    reloader = MetadataReloader(settings.database, telemetry_interval_seconds())
    threading.Thread(
        target=run_simulator_thread,
        args=(store, reloader, mappings, health),
        daemon=True,
    ).start()

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
