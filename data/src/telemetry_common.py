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
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import IO, Any, ClassVar, Literal

import psycopg2
import psycopg2.extensions
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

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


class MetadataReloader:
    """Re-read metadata on a timer so config changes reach a running service.

    ADR 0035. The services configure themselves from database tables, but each
    read its metadata once at startup and cached it for the life of the process,
    so an edited row did nothing until the container restarted. The
    documentation claimed otherwise.

    A timer was chosen over a Postgres notification or a reload endpoint because
    it adds no new surface: no listener thread, no triggers, and no HTTP server
    in services that have none. The cost is that a change takes up to one
    interval to apply, and one extra query per interval per service.
    """

    def __init__(
        self,
        settings: DatabaseSettings,
        interval_seconds: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Start the reload clock, with the current mappings treated as fresh."""
        self._settings = settings
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._last_loaded_at = clock()

    def is_due(self) -> bool:
        """Report whether the reload interval has elapsed."""
        return self._clock() - self._last_loaded_at >= self._interval_seconds

    def reload(self) -> list[ParameterMapping] | None:
        """Re-read the mappings, or return None if the read failed.

        A failed read must not stop the service. The mappings it already holds
        are still valid, so the caller keeps them and the next tick tries again.
        """
        try:
            mappings = load_parameter_mappings(self._settings)
        except psycopg2.Error:
            logging.getLogger(__name__).exception(
                "Metadata reload failed; keeping the mappings already loaded"
            )
            return None
        finally:
            # Count the attempt either way, so a database that is down cannot
            # turn the reload into a hot loop against it.
            self._last_loaded_at = self._clock()

        return mappings


def mappings_changed(
    before: list[ParameterMapping], after: list[ParameterMapping]
) -> bool:
    """Report whether a reload produced a different set of mappings.

    Compared as sets because `load_parameter_mappings` orders its result, and a
    reordering with the same contents is not a change worth acting on.
    """
    return set(before) != set(after)


@dataclass
class ServiceHealth:
    """What a service reports about itself.

    `ready` is deliberately separate from `alive`. A service can be running and
    still unable to do its job, and a container orchestrator needs to tell those
    apart: restart the first, route around the second.
    """

    alive: bool = True
    ready: bool = False
    detail: dict[str, Any] = field(default_factory=dict)


class ServiceEndpointHandler(BaseHTTPRequestHandler):
    """Serve health, readiness, and metrics for one service."""

    health: ClassVar[ServiceHealth]
    service_name: ClassVar[str]

    def do_GET(self) -> None:
        """Answer the three operational endpoints, and 404 anything else."""
        if self.path == "/health":
            self._json(200 if self.health.alive else 503, {"status": "ok"})
        elif self.path == "/ready":
            ready = self.health.ready
            self._json(
                200 if ready else 503,
                {"status": "ready" if ready else "not_ready", **self.health.detail},
            )
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._json(404, {"error": "not found"})

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        """Write one JSON response."""
        body = json.dumps({"service": self.service_name, **payload}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence the stdlib's stderr access log.

        It writes plain text, which would break the structured log stream that
        Standard 2 requires. Health checks poll constantly, so this would be the
        majority of the output.
        """


def start_service_endpoints(
    service_name: str, health: ServiceHealth, port: int
) -> ThreadingHTTPServer:
    """Serve /health, /ready, and /metrics on a background thread.

    ADR 0017 asks for health and readiness on every service. These three had
    none, so a stuck process was indistinguishable from a working one.
    """
    handler = type(
        "BoundServiceEndpointHandler",
        (ServiceEndpointHandler,),
        {"health": health, "service_name": service_name},
    )

    server = ThreadingHTTPServer(("0.0.0.0", port), handler)  # noqa: S104
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    logging.getLogger(service_name).info(
        "Serving operational endpoints", extra={"port": port}
    )
    return server


def service_port(default: int) -> int:
    """Read the port for this service's operational endpoints."""
    return int(required_env("SERVICE_PORT", str(default)))
