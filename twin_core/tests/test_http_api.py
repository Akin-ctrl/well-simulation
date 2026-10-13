"""HTTP checks for the internal read-only twin-core boundary."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Generator
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
from twin_core.config import NoDatabaseOverrides, ServiceSettings, load_defaults
from twin_core.database import (
    DatabaseUnavailableError,
    HistorianReader,
    TelemetrySnapshot,
    make_snapshot,
)
from twin_core.http_api import create_server
from twin_core.metrics import TwinMetrics
from twin_core.persistence import TwinPersistence
from twin_core.projection import AlarmRule, ProjectionResult
from twin_core.runtime import TwinRuntime
from twin_core.state import ModelRegistry


class FakeHistorian(HistorianReader):
    """Return one known well and one real-looking simulator reading."""

    def __init__(self) -> None:
        """Avoid opening a database in transport tests."""

    def active_wellhead_ids(self) -> list[int]:
        """Return a small active fleet."""
        return [1]

    def latest_telemetry(
        self, wellhead_id: int, freshness_seconds: int
    ) -> TelemetrySnapshot:
        """Return a reading with explicit source and insertion times."""
        now = datetime.now(timezone.utc)
        return make_snapshot(
            wellhead_id,
            [
                (
                    "tubing_pressure",
                    "psi",
                    1800.0,
                    now,
                    now,
                    "synthetic_random_simulator",
                )
            ],
            now,
            freshness_seconds,
        )


@pytest.fixture
def server() -> Generator[ThreadingHTTPServer]:
    """Run the actual handler against a fake data source."""
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password=os.getenv("TEST_DB_PASSWORD", "unused"),
        environment_overrides={},
    )
    defaults = load_defaults()
    registry = ModelRegistry(defaults, {}, NoDatabaseOverrides())
    runtime = TwinRuntime(settings, defaults, registry, FakeHistorian(), TwinMetrics())
    assert runtime.refresh_fleet()
    assert runtime.registry.tick(datetime.now(timezone.utc)) == (1, 0)
    http_server = create_server("127.0.0.1", 0, runtime)
    thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    thread.start()
    yield http_server
    http_server.shutdown()
    http_server.server_close()
    thread.join(timeout=5)


def _get(server: ThreadingHTTPServer, path: str) -> tuple[int, dict[str, object]]:
    """Get and decode one JSON response, including expected HTTP errors."""
    url = f"http://127.0.0.1:{server.server_port}{path}"
    try:
        with urlopen(url, timeout=2) as response:  # noqa: S310
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def test_state_exposes_running_model_values(server: ThreadingHTTPServer) -> None:
    status, body = _get(server, "/wellheads/1/state")
    assert status == 200
    assert body["status"] == "running"
    assert body["reason"] == "cold_start_no_snapshot"
    assert isinstance(body["process"], dict)
    assert body["process"]["flowRateBpd"] > 0
    assert body["process"]["source"] == "synthetic_reduced_order_model"
    assert body["process"]["fieldCalibrated"] is False


def test_latest_telemetry_is_labelled_as_existing_simulator_output(
    server: ThreadingHTTPServer,
) -> None:
    status, body = _get(server, "/wellheads/1/latest-telemetry")
    assert status == 200
    assert body["source"] == "synthetic_random_simulator"
    assert body["modelGenerated"] is False
    assert body["quality"] == "fresh"


def test_unknown_well_and_unready_service_are_explicit(
    server: ThreadingHTTPServer,
) -> None:
    assert _get(server, "/wellheads/2/state")[0] == 404
    status, body = _get(server, "/ready")
    assert status == 503
    assert body["reason"] == "tick_loop_not_current"
    assert body["modelStatus"] == "lagging"


def test_write_method_is_rejected(server: ThreadingHTTPServer) -> None:
    url = f"http://127.0.0.1:{server.server_port}/wellheads/1/state"
    request = Request(url, data=b"{}", method="POST")  # noqa: S310
    with pytest.raises(HTTPError) as error:
        urlopen(request, timeout=2)  # noqa: S310
    assert error.value.code == 405
    assert error.value.headers["Allow"] == "GET"


def test_model_snapshot_rejects_unready_fleet(server: ThreadingHTTPServer) -> None:
    status, body = _get(server, "/model-snapshot")
    assert status == 503
    assert body["error"] == "model_not_ready"


class RecordingProjectionStore(TwinPersistence):
    """Keep transport tests independent of PostgreSQL, including failures."""

    def __init__(self) -> None:
        """Record committed run requests without a database connection."""
        self.saved: list[tuple[ProjectionResult, int]] = []
        self.fail = False

    def load_alarm_rules(self) -> tuple[AlarmRule, ...]:
        return (AlarmRule(1, "flow_rate", "WARNING", ">", 0.0),)

    def write_projection(self, result: ProjectionResult, requested_by: int) -> None:
        if self.fail:
            raise DatabaseUnavailableError("offline")
        self.saved.append((result, requested_by))


ProjectionServer = tuple[ThreadingHTTPServer, RecordingProjectionStore, TwinRuntime]


@pytest.fixture
def projection_server() -> Generator[ProjectionServer]:
    """Serve authenticated run requests against the real model and a fake sink."""
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password=os.getenv("TEST_DB_PASSWORD", "unused"),
        environment_overrides={},
        internal_token="a" * 32,
    )
    defaults = load_defaults()
    registry = ModelRegistry(defaults, {}, NoDatabaseOverrides())
    store = RecordingProjectionStore()
    runtime = TwinRuntime(
        settings, defaults, registry, FakeHistorian(), TwinMetrics(), persistence=store
    )
    # Startup recovery is not under test here. Populate one current running well.
    registry.replace_wellheads([1])
    assert registry.tick(datetime.now(timezone.utc)) == (1, 0)
    http_server = create_server("127.0.0.1", 0, runtime)
    thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    thread.start()
    yield http_server, store, runtime
    http_server.shutdown()
    http_server.server_close()
    thread.join(timeout=5)


def _post(
    server: ThreadingHTTPServer,
    path: str,
    body: dict[str, object],
    *,
    token: str | None = "a" * 32,
) -> tuple[int, dict[str, object]]:
    headers = {"Content-Type": "application/json", "X-Twin-Actor-Id": "42"}
    if token is not None:
        headers["X-Twin-Internal-Token"] = token
    request = Request(
        f"http://127.0.0.1:{server.server_port}{path}",
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=3) as response:  # noqa: S310
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def test_projection_route_authenticates_and_saves_forecast(
    projection_server: ProjectionServer,
) -> None:
    server, store, runtime = projection_server
    before = runtime.state(1)
    assert _post(server, "/wellheads/1/forecast", {}, token=None)[0] == 403
    assert not store.saved

    status, body = _post(
        server, "/wellheads/1/forecast", {"horizonSeconds": 300, "stepSeconds": 60}
    )

    assert status == 201
    assert body["kind"] == "forecast"
    assert isinstance(body["points"], list)
    assert len(body["points"]) == 5
    assert body["fieldCalibrated"] is False
    assert store.saved[0][1] == 42
    assert runtime.state(1) == before


def test_scenario_route_rejects_outcomes_and_saves_baseline(
    projection_server: ProjectionServer,
) -> None:
    server, store, _runtime = projection_server
    body: dict[str, object] = {
        "wellheadId": 1,
        "name": "Close choke",
        "horizonSeconds": 300,
        "stepSeconds": 60,
        "controls": [{"offsetMinutes": 0, "input": {"chokePositionPercent": 0}}],
    }
    assert (
        _post(
            server,
            "/scenarios",
            {**body, "controls": [{"offsetMinutes": 0, "input": {"flowRateBpd": 0}}]},
        )[0]
        == 400
    )

    status, response = _post(server, "/scenarios", body)
    assert status == 201
    assert response["kind"] == "scenario"
    assert len(store.saved) == 1
    points = response["points"]
    assert isinstance(points, list)
    last = points[-1]
    assert last["values"]["flow_rate"] == 0.0
    assert last["baselineValues"]["flow_rate"] > 0.0


def test_projection_failure_returns_503_without_partial_fake_save(
    projection_server: ProjectionServer,
) -> None:
    server, store, _runtime = projection_server
    store.fail = True
    status, body = _post(server, "/wellheads/1/forecast", {"horizonSeconds": 300})
    assert status == 503
    assert body["error"] == "projection_dependency_unavailable"
    assert not store.saved
