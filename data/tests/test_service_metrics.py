"""Check that service metrics report current health and confirmed commits."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import Mock
from urllib.request import urlopen

import pytest

import database_ingestion as ingestion
from telemetry_common import (
    DatabaseSettings,
    ModelVersion,
    ServiceHealth,
    start_service_endpoints,
)


def test_telemetry_metric_tracks_health_without_a_readiness_request() -> None:
    health = ServiceHealth()
    server = start_service_endpoints("test", health, 0)
    try:
        for alive, ready, expected in [
            (True, False, 0),
            (True, True, 1),
            (False, True, 0),
        ]:
            health.alive, health.ready = alive, ready
            with urlopen(
                f"http://127.0.0.1:{server.server_port}/metrics", timeout=2
            ) as response:
                assert (
                    f"well_sim_telemetry_ready {expected}.0" in response.read().decode()
                )
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("commit_succeeds", [True, False])
def test_timestamp_changes_only_after_a_commit(
    monkeypatch: pytest.MonkeyPatch, commit_succeeds: bool
) -> None:
    connection = Mock(closed=False)
    if not commit_succeeds:
        connection.commit.side_effect = RuntimeError("commit failed")
    metric = Mock()
    monkeypatch.setattr(ingestion, "connect_db", Mock(return_value=connection))
    monkeypatch.setattr(ingestion, "LAST_COMMIT_TIMESTAMP", metric)
    monkeypatch.setattr(
        ingestion, "apply_metadata_reload", lambda _loader, mappings: mappings
    )
    monkeypatch.setattr(ingestion, "execute_batch", Mock())
    now = datetime.now(timezone.utc)
    status = ingestion.GatewayStatus(int(time.time()), ModelVersion(0, 3, 0))
    monkeypatch.setattr(
        ingestion, "read_fleet_status", Mock(side_effect=[status, RuntimeError("stop")])
    )
    reading = ingestion.Reading(now, 1, 1, 1, 1.0, status.model_version)
    monkeypatch.setattr(
        ingestion, "poll_consistent_batch", Mock(return_value=(status, [reading]))
    )
    monkeypatch.setattr(time, "sleep", Mock())
    settings = ingestion.Settings(
        "unused",
        5020,
        5,
        DatabaseSettings("unused", 5432, "unused", "unused", "unused"),
    )
    health = ServiceHealth()
    with pytest.raises(RuntimeError, match=r"stop|commit failed"):
        ingestion.ingest_forever(settings, [Mock()], Mock(), health)
    if commit_succeeds:
        metric.set.assert_called_once()
        assert metric.set.call_args.args[0] >= now.timestamp()
        assert health.ready
    else:
        metric.set.assert_not_called()
        assert not health.ready
    connection.close.assert_called_once()
