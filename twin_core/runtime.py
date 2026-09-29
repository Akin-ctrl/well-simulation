"""Run fleet refreshes and fixed-step service ticks."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from twin_core.config import ModelDefaults, ServiceSettings
from twin_core.database import (
    DatabaseUnavailableError,
    HistorianReader,
    TelemetrySnapshot,
)
from twin_core.metrics import TwinMetrics
from twin_core.scheduler import plan_ticks
from twin_core.state import ModelRegistry, WellheadState

logger = logging.getLogger(__name__)
TICK_INTERVAL_SECONDS = 1.0
MAX_READY_TICK_AGE_SECONDS = 3.0
RETRY_FLEET_SECONDS = 5.0
MAX_CONCURRENT_TELEMETRY_READS = 8


class ReadCapacityError(RuntimeError):
    """The bounded telemetry read pool has no free slot."""


class TwinRuntime:
    """Own the operational lifecycle of the twin-core skeleton."""

    def __init__(
        self,
        settings: ServiceSettings,
        defaults: ModelDefaults,
        registry: ModelRegistry,
        historian: HistorianReader,
        metrics: TwinMetrics,
    ) -> None:
        """Keep transport, state, data access, and metrics separate."""
        self.settings = settings
        self.defaults = defaults
        self.registry = registry
        self.historian = historian
        self.metrics = metrics
        self._stop = threading.Event()
        self._read_slots = threading.BoundedSemaphore(MAX_CONCURRENT_TELEMETRY_READS)
        self._lock = threading.Lock()
        self._fleet_ok = False
        self._last_tick_monotonic: float | None = None

    def stop(self) -> None:
        """Request a clean exit after the current bounded operation."""
        self._stop.set()

    def _set_fleet_ok(self, value: bool) -> None:
        """Update database readiness without exposing mutable state."""
        with self._lock:
            self._fleet_ok = value

    def refresh_fleet(self) -> bool:
        """Reload active wellheads and reject bad model configuration."""
        try:
            ids = self.historian.active_wellhead_ids()
            self.registry.replace_wellheads(ids)
        except DatabaseUnavailableError:
            logger.error("Active wellhead refresh failed")
            self.metrics.database_errors.labels(operation="fleet_refresh").inc()
            self._set_fleet_ok(False)
            self.metrics.ready.set(0)
            return False
        except ValueError:
            logger.error("Active wellhead or override configuration is invalid")
            self._set_fleet_ok(False)
            self.metrics.ready.set(0)
            return False

        self.metrics.active_wellheads.set(self.registry.count())
        self.metrics.invalid_parameters.set(self.registry.invalid_count())
        self._set_fleet_ok(True)
        if not ids:
            logger.warning("No active wellheads are available")
        return True

    def state(self, wellhead_id: int) -> WellheadState | None:
        """Read current lifecycle state for one active wellhead."""
        return self.registry.get(wellhead_id)

    def latest_telemetry(self, wellhead_id: int) -> TelemetrySnapshot:
        """Limit concurrent database reads and report overload explicitly."""
        if not self._read_slots.acquire(blocking=False):
            self.metrics.rejected_reads.inc()
            raise ReadCapacityError("Telemetry read capacity is full")
        try:
            try:
                return self.historian.latest_telemetry(
                    wellhead_id, self.settings.telemetry_freshness_seconds
                )
            except DatabaseUnavailableError:
                self.metrics.database_errors.labels(operation="latest_telemetry").inc()
                self._set_fleet_ok(False)
                self.metrics.ready.set(0)
                logger.error(
                    "Latest telemetry read failed", extra={"wellhead_id": wellhead_id}
                )
                raise
        finally:
            self._read_slots.release()

    def readiness(self) -> tuple[bool, dict[str, object]]:
        """Report whether fleet data and the service tick loop are current."""
        now = time.monotonic()
        with self._lock:
            fleet_ok = self._fleet_ok
            last_tick = self._last_tick_monotonic

        count = self.registry.count()
        invalid = self.registry.invalid_count()
        tick_current = (
            last_tick is not None and now - last_tick <= MAX_READY_TICK_AGE_SECONDS
        )
        ready = fleet_ok and count > 0 and invalid == 0 and tick_current
        if not fleet_ok:
            reason = "historian_unavailable"
        elif count == 0:
            reason = "no_active_wellheads"
        elif invalid:
            reason = "invalid_model_parameters"
        elif not tick_current:
            reason = "tick_loop_not_current"
        else:
            reason = "service_ready_model_unavailable"
        self.metrics.ready.set(1 if ready else 0)
        return ready, {
            "status": "ready" if ready else "not_ready",
            "service": "twin-core",
            "modelVersion": self.defaults.version,
            "modelStatus": "unavailable",
            "activeWellheads": count,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def run(self) -> None:
        """Refresh the fleet and execute bounded one-second service ticks."""
        next_due = time.monotonic() + TICK_INTERVAL_SECONDS
        next_refresh = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= next_refresh:
                success = self.refresh_fleet()
                interval = (
                    self.settings.fleet_refresh_seconds
                    if success
                    else RETRY_FLEET_SECONDS
                )
                next_refresh = time.monotonic() + interval

            now = time.monotonic()
            batch = plan_ticks(
                now, next_due, TICK_INTERVAL_SECONDS, self.settings.max_catchup_steps
            )
            next_due = batch.next_due
            if batch.steps:
                self.metrics.lag_seconds.set(batch.lag_seconds)
                for _ in range(batch.steps):
                    self.registry.tick(datetime.now(timezone.utc))
                    self.metrics.ticks.inc()
                with self._lock:
                    self._last_tick_monotonic = time.monotonic()

            if batch.skipped:
                self.metrics.skipped_ticks.inc(batch.skipped)
                logger.warning(
                    "Skipped delayed service ticks",
                    extra={
                        "skipped_ticks": batch.skipped,
                        "lag_seconds": batch.lag_seconds,
                    },
                )

            wait_seconds = max(0.0, min(next_due, next_refresh) - time.monotonic())
            self._stop.wait(wait_seconds)
