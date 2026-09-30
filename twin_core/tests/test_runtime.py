"""Tests for bounded database-read capacity."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest
import twin_core.runtime as runtime_module
from twin_core.config import NoDatabaseOverrides, ServiceSettings, load_defaults
from twin_core.database import HistorianReader, TelemetrySnapshot, make_snapshot
from twin_core.metrics import TwinMetrics
from twin_core.runtime import ReadCapacityError, TwinRuntime
from twin_core.state import ModelRegistry


class BlockingHistorian(HistorianReader):
    """Hold one read until the test permits it to finish."""

    def __init__(self, started: threading.Event, release: threading.Event) -> None:
        """Store synchronization events without opening a database."""
        self.started = started
        self.release = release

    def latest_telemetry(
        self, wellhead_id: int, freshness_seconds: int
    ) -> TelemetrySnapshot:
        """Block one request long enough to test the capacity limit."""
        self.started.set()
        if not self.release.wait(timeout=3):
            raise RuntimeError("Test read was not released")
        now = datetime.now(timezone.utc)
        return make_snapshot(wellhead_id, [], now, freshness_seconds)


def test_read_capacity_rejects_excess_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_module, "MAX_CONCURRENT_TELEMETRY_READS", 1)
    started = threading.Event()
    release = threading.Event()
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=8000,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password="unused",  # noqa: S106
        environment_overrides={},
    )
    defaults = load_defaults()
    runtime = TwinRuntime(
        settings,
        defaults,
        ModelRegistry(defaults, {}, NoDatabaseOverrides()),
        BlockingHistorian(started, release),
        TwinMetrics(),
    )
    first = threading.Thread(target=runtime.latest_telemetry, args=(1,))
    first.start()
    assert started.wait(timeout=2)
    try:
        with pytest.raises(ReadCapacityError):
            runtime.latest_telemetry(1)
        assert runtime.metrics.rejected_reads._value.get() == 1
    finally:
        release.set()
        first.join(timeout=3)
    assert not first.is_alive()
    assert runtime.latest_telemetry(1).quality == "missing"
