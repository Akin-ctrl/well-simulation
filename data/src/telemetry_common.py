"""Shared configuration, logging, and database access for the telemetry services.

The simulator, Modbus gateway, and ingestion service previously each carried
their own copy of environment loading, database connection, and parameter
mapping queries. Standard 5 asks for shared helpers over copy-paste, and
Standard 6 asks for one authoritative definition of a shared contract, so those
concerns live here.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import IO, Any, Literal

import psycopg2
import psycopg2.extensions

# Both the gateway and ingestion encode values as two 16-bit words, big-endian
# bytes with the low word first. This reproduces the layout written before the
# pymodbus 3.x upgrade, so existing register maps and stored readings stay valid.
WORD_ORDER: Literal["little"] = "little"

# Wellhead N occupies registers (N-1)*100 through (N-1)*100+34, so the highest
# mapped register for the seeded 12 wellheads is 1134. The heartbeat sits above
# that range and carries the epoch seconds of the last telemetry batch, letting
# ingestion tell a live pipeline from a stalled one serving stale registers.
HEARTBEAT_REGISTER = 1900

_RESERVED_LOG_FIELDS = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
)


class JsonLogFormatter(logging.Formatter):
    """Render records as one JSON object per line.

    The TypeScript API already emits JSON logs. Matching that here means a
    single parser can read the whole system, which Standard 2 requires.
    """

    def __init__(self, service: str) -> None:
        """Bind the formatter to the emitting service name."""
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        """Serialise one log record as a JSON line."""
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Anything passed via logger.info(..., extra={...}) becomes a top-level
        # field, so operational values stay queryable instead of being buried in
        # a formatted string.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_FIELDS and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(service: str, *, stream: IO[str] | None = None) -> logging.Logger:
    """Install structured JSON logging and return the service logger.

    The simulator writes telemetry to stdout, so its logs must go to stderr to
    avoid corrupting that stream.
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonLogFormatter(service))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    return logging.getLogger(service)


def configure_pymodbus_logging() -> None:
    """Bind pymodbus verbosity to the service log level.

    pymodbus 3.x enables frame-level debug logging by default, emitting a hex
    dump per request. At 216 register reads per interval that buries every
    operational signal.
    """
    from pymodbus import pymodbus_apply_logging_config

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    pymodbus_apply_logging_config(level)

    # That call attaches a plain-text StreamHandler to the "pymodbus.logging"
    # logger (not "pymodbus") and leaves propagation on, so each record would be
    # emitted twice: once unstructured by that handler and once as JSON by the
    # root handler. Drop its handler and keep propagation, leaving exactly one
    # structured line per record.
    pymodbus_logger = logging.getLogger("pymodbus.logging")
    pymodbus_logger.handlers.clear()
    pymodbus_logger.propagate = True


def required_env(name: str, default: str | None = None) -> str:
    """Return an environment variable or raise a clear configuration error."""
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def telemetry_interval_seconds() -> int:
    """Return the shared simulate/poll interval.

    One value drives both the simulator and ingestion. When they diverged, every
    simulated value was recorded to the historian several times over.
    """
    return int(required_env("TELEMETRY_INTERVAL_SECONDS", "5"))


@dataclass(frozen=True)
class DatabaseSettings:
    """PostgreSQL connection settings."""

    host: str
    port: int
    name: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> DatabaseSettings:
        """Build settings from the standard POSTGRES_* environment variables."""
        return cls(
            host=required_env("POSTGRES_HOST", "db"),
            port=int(required_env("POSTGRES_PORT", "5432")),
            name=required_env("POSTGRES_DB"),
            user=required_env("POSTGRES_USER"),
            password=required_env("POSTGRES_PASSWORD"),
        )


def connect_db(settings: DatabaseSettings) -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection using the supplied settings."""
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.name,
        user=settings.user,
        password=settings.password,
    )


@dataclass(frozen=True)
class ParameterMapping:
    """One active parameter on one wellhead, and where to read or write it.

    This is the shared contract between the gateway, which writes registers, and
    ingestion, which reads them. Both previously ran near-identical queries and
    kept the result in untyped dictionaries.
    """

    mapping_id: int
    wellhead_id: int
    parameter_type_id: int
    parameter_code: str
    modbus_register: int
    modbus_unit_id: int
    data_type: str


_PARAMETER_MAPPING_QUERY = """
SELECT dpm.mapping_id,
       wh.wellhead_id,
       pt.parameter_type_id,
       pt.code,
       dpm.modbus_register,
       d.modbus_unit_id,
       pt.data_type
FROM deviceParameterMapping dpm
JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
JOIN device d ON dpm.device_id = d.device_id
JOIN wellHead wh ON d.device_id = wh.device_id
WHERE dpm.active = TRUE
ORDER BY d.modbus_unit_id, dpm.modbus_register;
"""


def load_parameter_mappings(settings: DatabaseSettings) -> list[ParameterMapping]:
    """Fetch every active parameter mapping, ordered by unit and register."""
    with connect_db(settings) as conn, conn.cursor() as cursor:
        cursor.execute(_PARAMETER_MAPPING_QUERY)
        rows = cursor.fetchall()

    return [ParameterMapping(*row) for row in rows]


def collect_unit_ids(mappings: list[ParameterMapping]) -> list[int]:
    """Return every distinct Modbus unit id referenced by the mappings."""
    return sorted({mapping.modbus_unit_id for mapping in mappings})


def as_number(value: object, context: str) -> float:
    """Narrow a decoded Modbus value to a single number.

    pymodbus returns a union covering strings and lists because one decoder
    serves every data type. Every call here decodes a scalar, so anything else
    means the register layout and the configured type have diverged.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"Expected a numeric Modbus value for {context}: {value!r}")
    return float(value)
