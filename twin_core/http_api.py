"""Serve bounded internal state reads and authenticated model projections."""

from __future__ import annotations

import json
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar, Literal
from urllib.parse import urlsplit

from prometheus_client import CONTENT_TYPE_LATEST

from twin_core.database import DatabaseUnavailableError
from twin_core.model import ControlInputs
from twin_core.projection import (
    MAX_CONTROL_STEPS,
    AlarmRuleConfigurationError,
    ControlChange,
    ScheduledControl,
    validate_projection_request,
    validate_window,
)
from twin_core.runtime import (
    ProjectionBusyError,
    ProjectionUnavailableError,
    ReadCapacityError,
    TwinRuntime,
)

WELLHEAD_PATH = re.compile(
    r"/wellheads/([1-9][0-9]{0,9})/(state|latest-telemetry|controls)"
)
FORECAST_PATH = re.compile(r"/wellheads/([1-9][0-9]{0,9})/forecast")
COMMAND_NOTIFY_PATH = re.compile(
    r"/control-commands/[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}/notify"
)
MAX_REQUEST_BYTES = 4096
DEFAULT_HORIZON_SECONDS = 1800
DEFAULT_STEP_SECONDS = 60
CONTROL_FIELDS = {
    "chokePositionPercent": "choke_position_percent",
    "masterValveOpen": "master_valve_open",
    "wingValveOpen": "wing_valve_open",
    "pumpOn": "pump_on",
    "emergencyShutdown": "emergency_shutdown",
}


@dataclass(frozen=True)
class ProjectionInput:
    """A validated forecast or scenario request ready for the model runtime."""

    kind: Literal["forecast", "scenario"]
    wellhead_id: int
    horizon_seconds: int
    step_seconds: int
    name: str | None = None
    controls: tuple[ScheduledControl, ...] = ()


def _object(value: object, name: str) -> dict[str, object]:
    """Require a JSON object with string keys."""
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be an object")
    return value


def _window(body: dict[str, object]) -> tuple[int, int]:
    """Apply ADR 0025 defaults and the model's bounded request limits."""
    return validate_window(
        body.get("horizonSeconds", DEFAULT_HORIZON_SECONDS),
        body.get("stepSeconds", DEFAULT_STEP_SECONDS),
    )


def _control_change(value: object) -> ControlChange:
    """Accept only scenario controls, never pressure or flow outcomes."""
    raw = _object(value, "control change")
    if not raw or set(raw) - set(CONTROL_FIELDS):
        raise ValueError("Scenario control fields are missing or unknown")
    choke = raw.get("chokePositionPercent")
    if choke is not None and (
        isinstance(choke, bool) or not isinstance(choke, (int, float))
    ):
        raise ValueError("chokePositionPercent must be a number")
    change = ControlChange(
        choke_position_percent=float(choke) if choke is not None else None,
        master_valve_open=_optional_bool(raw, "masterValveOpen"),
        wing_valve_open=_optional_bool(raw, "wingValveOpen"),
        pump_on=_optional_bool(raw, "pumpOn"),
        emergency_shutdown=_optional_bool(raw, "emergencyShutdown"),
    )
    change.apply(ControlInputs())
    return change


def _optional_bool(raw: dict[str, object], key: str) -> bool | None:
    """Keep false distinct from an omitted control field."""
    value = raw.get(key)
    if value is not None and type(value) is not bool:
        raise ValueError(f"{key} must be a boolean")
    return value


def _scenario_steps(value: object) -> tuple[ScheduledControl, ...]:
    """Read at most sixteen ordered changes at whole-minute offsets."""
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_CONTROL_STEPS:
        raise ValueError("controls must contain 1 to 16 steps")
    steps: list[ScheduledControl] = []
    for item in value:
        raw = _object(item, "control step")
        if set(raw) != {"offsetMinutes", "input"}:
            raise ValueError("Control step fields must be offsetMinutes and input")
        offset = raw["offsetMinutes"]
        if type(offset) is not int:
            raise ValueError("offsetMinutes must be an integer")
        steps.append(ScheduledControl(offset * 60, _control_change(raw["input"])))
    return tuple(steps)


def _projection_input(path: str, body: dict[str, object]) -> ProjectionInput:
    """Validate the route's exact fields, name, controls, and time window."""
    horizon, step = _window(body)
    match = FORECAST_PATH.fullmatch(path)
    if match is not None:
        if set(body) - {"horizonSeconds", "stepSeconds"}:
            raise ValueError("Unknown forecast field")
        return ProjectionInput("forecast", int(match.group(1)), horizon, step)
    if set(body) != {"wellheadId", "name", "horizonSeconds", "stepSeconds", "controls"}:
        raise ValueError("Scenario fields are incomplete or unknown")
    wellhead_id = body["wellheadId"]
    if type(wellhead_id) is not int or not 1 <= wellhead_id <= 2_147_483_647:
        raise ValueError("Invalid wellheadId")
    name = body["name"]
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 120:
        raise ValueError("Scenario name must contain 1 to 120 characters")
    return ProjectionInput(
        "scenario",
        wellhead_id,
        horizon,
        step,
        name.strip(),
        _scenario_steps(body["controls"]),
    )


class TwinHttpServer(ThreadingHTTPServer):
    """Allow a slow database read without blocking health checks."""

    daemon_threads = True
    request_queue_size = 16


class TwinRequestHandler(BaseHTTPRequestHandler):
    """Serve state reads and private, bounded forecast or scenario requests."""

    runtime: ClassVar[TwinRuntime]

    def setup(self) -> None:
        """Bound how long a client may hold a request thread idle."""
        super().setup()
        self.connection.settimeout(5)

    def do_GET(self) -> None:
        """Route a bounded read or return a structured error."""
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            self._json(400, {"error": "query_not_supported"})
            return

        if parsed.path == "/health":
            self._json(
                200,
                {
                    "status": "ok",
                    "service": "twin-core",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            return
        if parsed.path == "/ready":
            ready, payload = self.runtime.readiness()
            self._json(200 if ready else 503, payload)
            return
        if parsed.path == "/metrics":
            self._body(200, self.runtime.metrics.render(), CONTENT_TYPE_LATEST)
            return

        if parsed.path == "/model-snapshot":
            self._model_snapshot()
            return

        match = WELLHEAD_PATH.fullmatch(parsed.path)
        if match is None:
            self._json(404, {"error": "not_found"})
            return

        self._wellhead(int(match.group(1)), match.group(2))

    def _wellhead(self, wellhead_id: int, view: str) -> None:
        """Serve one active wellhead view."""
        state = self.runtime.state(wellhead_id)
        if state is None:
            self._json(404, {"error": "wellhead_not_found"})
            return

        if view == "state":
            self._json(200, state.to_payload())
            return

        if view == "controls":
            if not self._authorized():
                return
            controls = self.runtime.control_payload(wellhead_id)
            self._json(
                200 if controls else 503, controls or {"error": "controls_unavailable"}
            )
            return

        try:
            snapshot = self.runtime.latest_telemetry(wellhead_id)
        except DatabaseUnavailableError:
            self._json(503, {"error": "historian_unavailable"})
            return
        except ReadCapacityError:
            self._json(503, {"error": "service_busy"})
            return
        self._json(200, snapshot.to_payload())

    def _model_snapshot(self) -> None:
        """Send a complete model fleet or an explicit unready response."""
        snapshot = self.runtime.model_snapshot()
        if snapshot is None:
            self._json(503, {"error": "model_not_ready"})
        else:
            self._json(200, snapshot)

    def do_POST(self) -> None:
        """Run analysis or accept authenticated validation and queue hints."""
        parsed = urlsplit(self.path)
        if (
            not parsed.query
            and not parsed.fragment
            and (
                COMMAND_NOTIFY_PATH.fullmatch(parsed.path) is not None
                or parsed.path in ("/fleet/refresh", "/parameter-changes/validate")
            )
        ):
            self._control_post(parsed.path)
            return
        match = FORECAST_PATH.fullmatch(parsed.path)
        if (
            (match is None and parsed.path != "/scenarios")
            or parsed.query
            or parsed.fragment
        ):
            self._json(405, {"error": "method_not_allowed"}, allow="GET")
            return
        if not self._authorized():
            return
        try:
            actor = int(self.headers.get("X-Twin-Actor-Id", ""))
            if not 1 <= actor <= 2_147_483_647:
                raise ValueError("Invalid actor")
            request = _projection_input(parsed.path, self._request_object())
            validate_projection_request(
                request.kind,
                request.name,
                request.controls,
                request.horizon_seconds,
                request.step_seconds,
            )
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "invalid_projection_request"})
            return
        if self.runtime.state(request.wellhead_id) is None:
            self._json(404, {"error": "wellhead_not_found"})
            return
        self._run_projection(request, actor)

    def _authorized(self) -> bool:
        """Check the server-to-server secret without accepting a human token."""
        expected = self.runtime.settings.internal_token
        received = self.headers.get("X-Twin-Internal-Token", "")
        if not expected or not secrets.compare_digest(received, expected):
            self._json(403, {"error": "forbidden"})
            return False
        return True

    def _control_post(self, path: str) -> None:
        """Validate proposed parameters or wake workers; never apply an HTTP body."""
        if not self._authorized():
            return
        try:
            body = self._request_object()
            if path == "/parameter-changes/validate":
                self._json(200, self.runtime.validate_parameter(body))
                return
            if body:
                raise ValueError("Hints must have an empty body")
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "invalid_control_request"})
            return
        except (DatabaseUnavailableError, ProjectionUnavailableError):
            self._json(503, {"error": "parameter_validation_unavailable"})
            return
        if path == "/fleet/refresh":
            self.runtime.request_fleet_refresh()
        elif self.runtime.control_inbox is not None:
            self.runtime.control_inbox.notify()
        else:
            self._json(503, {"error": "control_inbox_unavailable"})
            return
        self._json(202, {"status": "notification_accepted"})

    def _run_projection(self, request: ProjectionInput, actor: int) -> None:
        """Map service and data failures to stable, safe HTTP responses."""
        try:
            result = self.runtime.create_projection(
                request.kind,
                request.wellhead_id,
                actor,
                request.horizon_seconds,
                request.step_seconds,
                name=request.name,
                controls=request.controls,
            )
        except ProjectionBusyError:
            self._json(503, {"error": "projection_busy"})
            return
        except ProjectionUnavailableError:
            self._json(503, {"error": "model_not_ready"})
            return
        except (DatabaseUnavailableError, AlarmRuleConfigurationError):
            self._json(503, {"error": "projection_dependency_unavailable"})
            return
        except (ArithmeticError, ValueError):
            self._json(503, {"error": "projection_failed"})
            return
        self._json(201, result.to_payload())

    def _request_object(self) -> dict[str, object]:
        """Read one small JSON request with a declared size and media type."""
        if (
            self.headers.get("Content-Type", "").split(";", 1)[0].strip()
            != "application/json"
        ):
            raise ValueError("Content-Type must be application/json")
        length = int(self.headers.get("Content-Length", ""))
        if not 0 < length <= MAX_REQUEST_BYTES:
            raise ValueError("Projection request body is too large or empty")
        return _object(json.loads(self.rfile.read(length).decode("utf-8")), "request")

    def do_PUT(self) -> None:
        """Reject writes until the controlled command phase is implemented."""
        self._json(405, {"error": "method_not_allowed"}, allow="GET")

    def do_DELETE(self) -> None:
        """Reject writes until the controlled command phase is implemented."""
        self._json(405, {"error": "method_not_allowed"}, allow="GET")

    def _json(
        self, status: int, payload: dict[str, object], *, allow: str | None = None
    ) -> None:
        """Send one JSON response without caching operational data."""
        self._body(
            status,
            json.dumps(payload, allow_nan=False).encode("utf-8"),
            "application/json",
            allow=allow,
        )

    def _body(
        self,
        status: int,
        body: bytes,
        content_type: str,
        *,
        allow: str | None = None,
    ) -> None:
        """Write a response with explicit length and security headers."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if allow:
            self.send_header("Allow", allow)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Suppress the standard library's unstructured access log."""


def create_server(bind_host: str, port: int, runtime: TwinRuntime) -> TwinHttpServer:
    """Bind the handler to one runtime instance."""
    handler = type(
        "BoundTwinRequestHandler",
        (TwinRequestHandler,),
        {"runtime": runtime},
    )
    return TwinHttpServer((bind_host, port), handler)
