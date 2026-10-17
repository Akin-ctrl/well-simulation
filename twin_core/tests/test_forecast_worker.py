"""Verify bounded due slots and worker outcomes without waiting for wall time."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock

import pytest
from twin_core.database import DatabaseUnavailableError
from twin_core.forecast_job import ScheduledForecastJob, due_slot
from twin_core.forecast_store import ForecastClaimError, ForecastStore
from twin_core.forecast_worker import ForecastWorker
from twin_core.metrics import TwinMetrics
from twin_core.runtime import (
    ProjectionBusyError,
    ProjectionUnavailableError,
    TwinRuntime,
)

NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
JOB = ScheduledForecastJob(
    "job",
    "schedule",
    1,
    1,
    1,
    "ADMIN",
    900,
    300,
    60,
    NOW,
    NOW + timedelta(seconds=60),
    2,
)


@pytest.mark.parametrize("interval", [300, 900, 86400])
@pytest.mark.parametrize("offset", [0, 1, 299, 300, 901, 2592000])
def test_one_due_slot_without_backlog(interval: int, offset: int) -> None:
    now = NOW + timedelta(seconds=offset)
    due, following, missed = due_slot(now, NOW, interval)
    assert due <= now < following
    assert following - due == timedelta(seconds=interval)
    assert missed == offset // interval


@pytest.mark.parametrize("interval", [0, 299, 301, 86401])
def test_invalid_interval_refused(interval: int) -> None:
    with pytest.raises(ValueError):
        due_slot(NOW, NOW, interval)


def test_backwards_clock_does_not_claim_future_work() -> None:
    with pytest.raises(ValueError, match="not due"):
        due_slot(NOW, NOW + timedelta(microseconds=1), 900)


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (None, None),
        (ProjectionBusyError(), "projection_busy"),
        (ProjectionUnavailableError(), "model_not_current"),
        (DatabaseUnavailableError(), "database_unavailable"),
        (ForecastClaimError("authority_revoked"), "authority_revoked"),
        (ValueError(), "projection_failed"),
    ],
)
def test_worker_records_terminal_outcome_once(
    error: Exception | None, reason: str | None
) -> None:
    runtime = MagicMock(spec=TwinRuntime)
    runtime.metrics = TwinMetrics()
    runtime.create_projection.side_effect = error
    store = MagicMock(spec=ForecastStore)
    store.expire.return_value = 0
    store.claim.return_value = JOB
    store.fail.return_value = "failed"
    ForecastWorker(cast(TwinRuntime, runtime), cast(ForecastStore, store)).run_once()
    runtime.create_projection.assert_called_once_with(
        "forecast", 1, 1, 300, 60, scheduled_job=JOB
    )
    if reason:
        store.fail.assert_called_once_with(JOB, reason)
    else:
        store.fail.assert_not_called()
    assert runtime.metrics.forecast_intervals_missed._value.get() == 2


def test_revoked_claim_does_not_calculate() -> None:
    runtime = MagicMock(spec=TwinRuntime)
    runtime.metrics = TwinMetrics()
    store = MagicMock(spec=ForecastStore)
    store.expire.return_value = 1
    store.claim.return_value = replace(JOB, status="skipped")
    ForecastWorker(cast(TwinRuntime, runtime), cast(ForecastStore, store)).run_once()
    runtime.create_projection.assert_not_called()
    store.fail.assert_not_called()
    assert (
        runtime.metrics.scheduled_forecasts.labels(outcome="expired")._value.get() == 1
    )


def test_worker_shutdown_interrupts_the_poll_wait() -> None:
    import threading

    scanned = threading.Event()
    runtime = MagicMock(spec=TwinRuntime)
    runtime.metrics = TwinMetrics()
    store = MagicMock(spec=ForecastStore)

    def scanned_once() -> int:
        scanned.set()
        return 0

    store.expire.side_effect = scanned_once
    store.claim.return_value = None
    worker = ForecastWorker(cast(TwinRuntime, runtime), cast(ForecastStore, store))
    worker.start()
    assert scanned.wait(timeout=2)
    worker.stop()
    assert not worker._thread.is_alive()
    assert runtime.metrics.forecast_scheduler_ready._value.get() == 0
