"""Serve the small, read-only internal twin-core HTTP API."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar
from urllib.parse import urlsplit

from prometheus_client import CONTENT_TYPE_LATEST

from twin_core.database import DatabaseUnavailableError
from twin_core.runtime import ReadCapacityError, TwinRuntime

WELLHEAD_PATH = re.compile(r"/wellheads/([1-9][0-9]{0,9})/(state|latest-telemetry)")


class TwinHttpServer(ThreadingHTTPServer):
    """Allow a slow database read without blocking health checks."""

    daemon_threads = True
    request_queue_size = 16


class TwinRequestHandler(BaseHTTPRequestHandler):
    """Serve health, readiness, metrics, state, and current telemetry."""

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

        match = WELLHEAD_PATH.fullmatch(parsed.path)
        if match is None:
            self._json(404, {"error": "not_found"})
            return

        wellhead_id = int(match.group(1))
        state = self.runtime.state(wellhead_id)
        if state is None:
            self._json(404, {"error": "wellhead_not_found"})
            return

        if match.group(2) == "state":
            self._json(200, state.to_payload())
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

    def do_POST(self) -> None:
        """Reject writes until the controlled command phase is implemented."""
        self._json(405, {"error": "method_not_allowed"}, allow="GET")

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
