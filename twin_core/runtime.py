"""Run fleet refreshes and fixed-step service ticks."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from twin_core.config import ModelDefaults, ServiceSettings
from twin_core.database import (
    DatabaseUnavailableError,
    HistorianReader,
    TelemetrySnapshot,
)
from twin_core.metrics import TwinMetrics
from twin_core.model import MODEL_STEP_SECONDS
from twin_core.persistence import (
    SNAPSHOT_INTERVAL_SECONDS,
    PersistenceWriter,
    RecoveryEvent,
    StoredSnapshot,
    TwinPersistence,
)
from twin_core.recovery import (
    decode_process,
    parameter_fingerprint,
    restore_state,
    saved_controls,
)
from twin_core.scheduler import plan_ticks, reconcile_clock
from twin_core.state import ModelRegistry, WellheadState

logger = logging.getLogger(__name__)
TICK_INTERVAL_SECONDS = MODEL_STEP_SECONDS
MAX_READY_TICK_AGE_SECONDS = 3.0
RETRY_FLEET_SECONDS = 5.0
MAX_CONCURRENT_TELEMETRY_READS = 8
# Disagreement between tick time and the wall clock that counts as a jump
# rather than ordinary jitter.
CLOCK_DRIFT_TOLERANCE_SECONDS = 2.0


def _utc_now() -> datetime:
    """Read the wall clock in UTC."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class FleetHealth:
    """The facts readiness is judged on, read once under the lock."""

    fleet_ok: bool
    count: int
    invalid: int
    running: int
    tick_current: bool
    clock_ok: bool
    holding: bool


def readiness_reason(health: FleetHealth) -> str:
    """Name the first thing stopping readiness, most fundamental first."""
    if not health.fleet_ok:
        return "historian_unavailable"
    if health.count == 0:
        return "no_active_wellheads"
    if health.invalid:
        return "invalid_model_parameters"
    if health.holding:
        return "wall_clock_moved_back"
    if not health.clock_ok:
        return "tick_clock_drift"
    if not health.tick_current:
        return "tick_loop_not_current"
    if health.running != health.count:
        return "model_not_running"
    return "service_ready_model_running"


def fleet_model_status(health: FleetHealth) -> str:
    """Summarise the model across the fleet for the readiness payload."""
    if not health.fleet_ok or health.count == 0:
        return "unavailable"
    if health.running > 0 and (not health.tick_current or not health.clock_ok):
        return "lagging"
    if health.running == health.count:
        return "running"
    if health.running > 0:
        return "degraded"
    return "unavailable"


class ReadCapacityError(RuntimeError):
    """The bounded telemetry read pool has no free slot."""


class TwinRuntime:
    """Own the operational lifecycle of the reduced-order twin core."""

    def __init__(
        self,
        settings: ServiceSettings,
        defaults: ModelDefaults,
        registry: ModelRegistry,
        historian: HistorianReader,
        metrics: TwinMetrics,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = _utc_now,
        persistence: TwinPersistence | None = None,
        writer: PersistenceWriter | None = None,
    ) -> None:
        """Keep transport, state, data access, and metrics separate.

        The two clocks are injectable so tests can simulate a machine that
        sleeps: the monotonic clock stands still while the wall clock moves on.
        """
        self._monotonic = monotonic
        self._wall = wall
        self.settings = settings
        self.defaults = defaults
        self.registry = registry
        self.historian = historian
        self.metrics = metrics
        self.persistence = persistence
        self.writer = writer
        self._recovered_wells: set[int] = set()
        self._last_snapshot_submission_at: datetime | None = None
        self._stop = threading.Event()
        self._read_slots = threading.BoundedSemaphore(MAX_CONCURRENT_TELEMETRY_READS)
        self._lock = threading.Lock()
        self._fleet_ok = False
        self._last_tick_monotonic: float | None = None
        self._last_tick_utc: datetime | None = None
        self._holding = False
        # Set by start_clock(). Tick time is anchor_utc plus monotonic time
        # elapsed since anchor_monotonic.
        self._anchor_monotonic = 0.0
        self._anchor_utc = _utc_now()
        self._next_due = 0.0
        self._next_refresh = 0.0

    def stop(self) -> None:
        """Request a clean exit after the current bounded operation."""
        self._stop.set()

    def _event(self, event: RecoveryEvent) -> None:
        """Expose and queue an operational event without blocking a tick."""
        self.metrics.recoveries.labels(outcome=event.event_type).inc()
        if event.wellhead_id is not None:
            self.registry.record_recovery_event(
                event.wellhead_id, event.event_type, event.occurred_at
            )
        if self.writer is not None:
            self.writer.submit_event(event)

    def _restore_one(
        self, wellhead_id: int, snapshot: StoredSnapshot | None, now: datetime
    ) -> None:
        """Choose a cold start or validated recovery for one new active well."""
        if snapshot is None:
            self._event(
                RecoveryEvent(
                    now, wellhead_id, "cold_start_no_snapshot", self.defaults.version
                )
            )
            return
        parameters = self.registry.parameters_for(wellhead_id)
        if parameters is None:
            return
        reason = "invalid_state"
        try:
            if snapshot.model_version != self.defaults.version:
                reason = "model_version"
                raise ValueError("Snapshot model version differs")
            if snapshot.parameter_fingerprint != parameter_fingerprint(parameters):
                reason = "parameter_fingerprint"
                raise ValueError("Snapshot parameters differ")
            saved = decode_process(snapshot.state)
            if saved.simulated_at != snapshot.simulated_at:
                reason = "timestamp"
                raise ValueError("Snapshot times differ")
            controls = saved_controls(saved)
            gap = (now - snapshot.simulated_at).total_seconds()
            recovered = restore_state(
                saved,
                parameters,
                controls,
                gap,
                now,
                self.settings.max_catchup_steps,
            )
        except (ValueError, ArithmeticError, TypeError):
            self._event(
                RecoveryEvent(
                    now,
                    wellhead_id,
                    "snapshot_incompatible",
                    self.defaults.version,
                    snapshot.simulated_at,
                    details={"check": reason},
                )
            )
            logger.warning(
                "Twin snapshot rejected",
                extra={"wellhead_id": wellhead_id, "check": reason},
            )
            return
        skipped = max(0, int(gap)) if gap > self.settings.max_catchup_steps else 0
        self.registry.restore(
            wellhead_id,
            recovered,
            snapshot.tick_index,
            snapshot.skipped_ticks + skipped,
            snapshot.simulated_at,
            now,
        )
        if skipped:
            self.metrics.skipped_ticks.inc(skipped)
        self._event(
            RecoveryEvent(
                now,
                wellhead_id,
                "snapshot_restored",
                self.defaults.version,
                snapshot.simulated_at,
                max(0.0, gap),
                details={"skipped_ticks": skipped},
            )
        )

    def _set_fleet_ok(self, value: bool) -> None:
        """Update database readiness without exposing mutable state."""
        with self._lock:
            self._fleet_ok = value

    def refresh_fleet(self) -> bool:
        """Reload active wellheads and reject bad model configuration."""
        try:
            ids = self.historian.active_wellhead_ids()
            # A well removed from the fleet can reappear later with a saved
            # state. It needs a fresh recovery decision at that point.
            self._recovered_wells.intersection_update(ids)
            new_ids = [
                wellhead_id
                for wellhead_id in ids
                if wellhead_id not in self._recovered_wells
            ]
            snapshots = (
                self.persistence.load_snapshots(new_ids)
                if self.persistence is not None
                else {}
            )
            self.registry.replace_wellheads(ids)
            if self.persistence is not None:
                now = self._wall()
                for wellhead_id in new_ids:
                    state = self.registry.get(wellhead_id)
                    if state is None or state.status != "initializing":
                        continue
                    self._restore_one(wellhead_id, snapshots.get(wellhead_id), now)
                    self._recovered_wells.add(wellhead_id)
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
        self.metrics.running_wellheads.set(self.registry.running_count())
        self._set_fleet_ok(True)
        if not ids:
            logger.warning("No active wellheads are available")
        return True

    def state(self, wellhead_id: int) -> WellheadState | None:
        """Read current lifecycle state for one active wellhead."""
        return self.registry.get(wellhead_id)

    def model_snapshot(self) -> dict[str, object] | None:
        """Return a complete running fleet from one registry read."""
        ready, _ = self.readiness()
        if not ready:
            return None
        states = self.registry.snapshot()
        if not states or any(state.process is None for state in states):
            return None
        return {
            "source": "synthetic_reduced_order_model",
            "fieldCalibrated": False,
            "modelVersion": self.defaults.version,
            "wellheads": [state.to_payload() for state in states],
        }

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
        now = self._monotonic()
        with self._lock:
            fleet_ok = self._fleet_ok
            last_tick = self._last_tick_monotonic
            last_tick_utc = self._last_tick_utc
            holding = self._holding

        count = self.registry.count()
        invalid = self.registry.invalid_count()
        running = self.registry.running_count()
        tick_current = (
            last_tick is not None and now - last_tick <= MAX_READY_TICK_AGE_SECONDS
        )
        # The monotonic check above cannot see a machine that slept, because the
        # monotonic clock stood still too. Compare the last tick with the wall
        # clock as well, so stale tick times are never reported as ready.
        clock_ok = not holding and (
            last_tick_utc is None
            or abs((self._wall() - last_tick_utc).total_seconds())
            <= MAX_READY_TICK_AGE_SECONDS
        )
        ready = (
            fleet_ok and count > 0 and running == count and tick_current and clock_ok
        )
        health = FleetHealth(
            fleet_ok=fleet_ok,
            count=count,
            invalid=invalid,
            running=running,
            tick_current=tick_current,
            clock_ok=clock_ok,
            holding=holding,
        )
        reason = readiness_reason(health)
        model_status = fleet_model_status(health)
        self.metrics.ready.set(1 if ready else 0)
        return ready, {
            "status": "ready" if ready else "not_ready",
            "service": "twin-core",
            "modelVersion": self.defaults.version,
            "modelStatus": model_status,
            "activeWellheads": count,
            "runningWellheads": running,
            "reason": reason,
            "timestamp": self._wall().isoformat(),
        }

    def start_clock(self) -> None:
        """Anchor tick time to the wall clock and schedule the first tick."""
        now = self._monotonic()
        self._anchor_monotonic = now
        self._anchor_utc = self._wall()
        self._next_due = now + TICK_INTERVAL_SECONDS
        self._next_refresh = now

    def run(self) -> None:
        """Refresh the fleet and execute bounded one-second service ticks."""
        self.start_clock()
        while not self._stop.is_set():
            self._stop.wait(self.cycle())

    def _tick_time(self, monotonic_seconds: float) -> datetime:
        """Convert a monotonic instant into the UTC time a tick carries."""
        return self._anchor_utc + timedelta(
            seconds=monotonic_seconds - self._anchor_monotonic
        )

    def _reconcile_clock(self, monotonic_now: float) -> None:
        """Realign tick time when the wall clock has jumped.

        Scheduling stays on the monotonic clock, so a wall-clock jump never
        moves deadlines or triggers a burst of catch-up steps. But tick times
        are UTC instants that the gateway and ingestion compare with their own
        wall clocks. After the machine slept they were hours behind, every
        snapshot was rejected as stale, and readiness still said ready.
        """
        check = reconcile_clock(
            self._tick_time(monotonic_now),
            self._wall(),
            CLOCK_DRIFT_TOLERANCE_SECONDS,
            TICK_INTERVAL_SECONDS,
        )
        if check.action == "none":
            return
        self._anchor_utc += timedelta(seconds=check.drift_seconds)
        if check.action == "reanchor":
            # The model did not run for this time. Count it as a gap and never
            # replay it as one large step (ADR 0020, ADR 0022).
            self.registry.record_gap(check.skipped_ticks)
            self.metrics.skipped_ticks.inc(check.skipped_ticks)
            self.metrics.clock_adjustments.labels(direction="forward").inc()
            logger.warning(
                "Wall clock moved ahead of tick time; recording a gap",
                extra={
                    "drift_seconds": round(check.drift_seconds, 3),
                    "skipped_ticks": check.skipped_ticks,
                },
            )
            self._event(
                RecoveryEvent(
                    self._wall(),
                    None,
                    "clock_jump_forward",
                    self.defaults.version,
                    gap_seconds=check.drift_seconds,
                    details={"skipped_ticks": check.skipped_ticks},
                )
            )
        else:
            self.metrics.clock_adjustments.labels(direction="backward").inc()
            logger.warning(
                "Wall clock moved back; holding ticks until it passes the last tick",
                extra={"drift_seconds": round(check.drift_seconds, 3)},
            )
            self._event(
                RecoveryEvent(
                    self._wall(),
                    None,
                    "clock_jump_backward",
                    self.defaults.version,
                    details={"drift_seconds": check.drift_seconds},
                )
            )

    def cycle(self) -> float:
        """Run one scheduler pass. Returns the seconds to wait before the next."""
        now = self._monotonic()
        if now >= self._next_refresh:
            success = self.refresh_fleet()
            interval = (
                self.settings.fleet_refresh_seconds if success else RETRY_FLEET_SECONDS
            )
            self._next_refresh = self._monotonic() + interval

        now = self._monotonic()
        self._reconcile_clock(now)
        batch = plan_ticks(
            now, self._next_due, TICK_INTERVAL_SECONDS, self.settings.max_catchup_steps
        )
        self._next_due = batch.next_due
        if batch.skipped:
            self.registry.record_gap(batch.skipped)
            self.metrics.skipped_ticks.inc(batch.skipped)
            logger.warning(
                "Skipped delayed model ticks",
                extra={
                    "skipped_ticks": batch.skipped,
                    "lag_seconds": batch.lag_seconds,
                },
            )
        if batch.steps:
            self.metrics.lag_seconds.set(batch.lag_seconds)
            holding = False
            for index in range(batch.steps):
                scheduled = (
                    batch.next_due - (batch.steps - index) * TICK_INTERVAL_SECONDS
                )
                at = self._tick_time(scheduled)
                with self._lock:
                    last_tick_utc = self._last_tick_utc
                if last_tick_utc is not None and at <= last_tick_utc:
                    # The wall clock went back. The model has already covered
                    # this time, so it waits rather than stepping backwards.
                    holding = True
                    continue
                advanced, failed = self.registry.tick(at)
                self.metrics.ticks.inc()
                self.metrics.model_steps.inc(advanced)
                self.metrics.model_step_errors.inc(failed)
                with self._lock:
                    self._last_tick_utc = at
                if self.writer is not None and (
                    self._last_snapshot_submission_at is None
                    or (at - self._last_snapshot_submission_at).total_seconds()
                    >= SNAPSHOT_INTERVAL_SECONDS
                ):
                    self.writer.submit_snapshot(self.registry.persistence_copy())
                    self._last_snapshot_submission_at = at
            self.metrics.running_wellheads.set(self.registry.running_count())
            with self._lock:
                self._holding = holding
                if not holding:
                    self._last_tick_monotonic = self._monotonic()

        if self.writer is not None:
            self.writer.refresh_age(self._wall())
        return max(0.0, min(self._next_due, self._next_refresh) - self._monotonic())
