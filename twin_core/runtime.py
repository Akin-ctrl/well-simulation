"""Run fleet refreshes and fixed-step service ticks."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Literal

from twin_core.config import (
    ModelDefaults,
    ParameterOverride,
    ServiceSettings,
    resolve_parameters,
)
from twin_core.control_store import ControlStore
from twin_core.controls import ControlInbox
from twin_core.database import (
    DatabaseUnavailableError,
    HistorianReader,
    TelemetrySnapshot,
)
from twin_core.forecast_job import ScheduledForecastJob
from twin_core.historical import (
    MAX_SNAPSHOT_AGE,
    SNAPSHOT_PAGE_SIZE,
    SNAPSHOT_SEARCH_WINDOW,
    SnapshotMissingError,
    SnapshotSelection,
    historical_base,
)
from twin_core.metrics import TwinMetrics
from twin_core.model import MODEL_STEP_SECONDS, ModelParameters, ProcessState
from twin_core.persistence import (
    SNAPSHOT_INTERVAL_SECONDS,
    PersistenceWriter,
    RecoveryEvent,
    StoredSnapshot,
    TwinPersistence,
)
from twin_core.projection import (
    ProjectionBase,
    ProjectionResult,
    ScheduledControl,
    project,
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
MAX_CONCURRENT_PROJECTIONS = 2
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


class ProjectionUnavailableError(RuntimeError):
    """A current, running model or persistence boundary is unavailable."""


class ProjectionBusyError(RuntimeError):
    """Both bounded projection slots are in use."""


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
        control_store: ControlStore | None = None,
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
        self.control_store = control_store
        self.control_inbox = (
            ControlInbox(control_store, registry, metrics, self._controls_reconciled)
            if control_store is not None
            else None
        )
        if control_store is not None:
            registry.require_durable_controls()
        self._refresh_requested = threading.Event()
        self._recovered_wells: set[int] = set()
        self._last_snapshot_submission_at: datetime | None = None
        self._stop = threading.Event()
        self._read_slots = threading.BoundedSemaphore(MAX_CONCURRENT_TELEMETRY_READS)
        self._projection_slots = threading.BoundedSemaphore(MAX_CONCURRENT_PROJECTIONS)
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
            gap = (now - snapshot.simulated_at).total_seconds()
            recovered, controls_changed = self._recover_process(
                wellhead_id, saved, parameters, gap, now
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
        skipped = (
            max(0, int(gap))
            if controls_changed or gap > self.settings.max_catchup_steps
            else 0
        )
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

    def _recover_process(
        self,
        wellhead_id: int,
        saved: ProcessState,
        parameters: ModelParameters,
        gap: float,
        now: datetime,
    ) -> tuple[ProcessState, bool]:
        """Avoid inventing recovery steps with controls older than the active row."""
        controls = saved_controls(saved)
        # Validate saved bounds before using them to seed a missing active row.
        restore_state(saved, parameters, controls, 0, saved.simulated_at)
        context = self.registry.control_context(wellhead_id)
        if self.control_store is not None and context is not None:
            active = self.control_store.restore(replace(context, controls=controls))
            if active.controls != controls:
                return replace(saved, simulated_at=max(now, saved.simulated_at)), True
        return restore_state(
            saved, parameters, controls, gap, now, self.settings.max_catchup_steps
        ), False

    def _controls_reconciled(self, wellhead_id: int) -> None:
        """Record that the active row replaced an older or uncertain memory value."""
        self._event(
            RecoveryEvent(
                self._wall(),
                wellhead_id,
                "active_controls_restored",
                self.defaults.version,
            )
        )

    def control_payload(self, wellhead_id: int) -> dict[str, object] | None:
        """Expose installed setpoints separately from the previous process step."""
        context = self.registry.control_context(wellhead_id)
        state = self.registry.get(wellhead_id)
        if context is None or state is None:
            return None
        control = context.controls
        return {
            "wellheadId": wellhead_id,
            "revision": context.revision,
            "modelVersion": context.model_version,
            "parameterFingerprint": context.parameter_fingerprint,
            "status": context.status,
            "lastTickAt": state.last_tick_at.isoformat()
            if state.last_tick_at
            else None,
            "controls": {
                "chokePositionPercent": control.choke_position_percent,
                "masterValveOpen": control.master_valve_open,
                "wingValveOpen": control.wing_valve_open,
                "swabValveOpen": control.swab_valve_open,
                "pumpOn": control.pump_on,
                "emergencyShutdown": control.emergency_shutdown,
            },
        }

    def request_fleet_refresh(self) -> None:
        """Request refresh on the scheduler thread without mutating it from HTTP."""
        self._refresh_requested.set()

    def validate_parameter(self, body: dict[str, object]) -> dict[str, object]:
        """Validate a proposed override against the same resolver used at startup."""
        if set(body) != {"wellheadId", "modelVersion", "parameterKey", "value", "unit"}:
            raise ValueError("Unknown or missing parameter fields")
        well = body["wellheadId"]
        key = body["parameterKey"]
        if (
            type(well) is not int
            or not isinstance(key, str)
            or key not in self.defaults.parameters
        ):
            raise ValueError("Invalid well or parameter")
        parameters = self.registry.parameters_for(well)
        if self.registry.get(well) is None or self.persistence is None:
            raise ProjectionUnavailableError("Model parameter context unavailable")
        if body["modelVersion"] != self.defaults.version:
            raise ValueError("Model version differs")
        overrides = dict(
            self.persistence.load_overrides([well], self.defaults.version).get(well, {})
        )
        value, unit = body["value"], body["unit"]
        if value is None:
            if unit is not None:
                raise ValueError("Removing an override requires a null unit")
            overrides.pop(key, None)
        else:
            spec = self.defaults.parameters[key]
            if unit != spec.unit:
                raise ValueError("Parameter unit differs")
            overrides[key] = ParameterOverride(
                spec.validate(value), spec.unit, "authenticated_api"
            )
        proposed = ModelParameters.from_values(
            resolve_parameters(
                self.defaults, self.settings.environment_overrides, overrides, well
            )
        )
        return {
            "modelVersion": self.defaults.version,
            "runningParameterFingerprint": parameter_fingerprint(parameters)
            if parameters
            else None,
            "proposedParameterFingerprint": parameter_fingerprint(proposed),
            "restartRequired": True,
        }

    def parameter_catalogue(self, wellhead_id: int) -> dict[str, object] | None:
        """Expose canonical definitions even when a well cannot start its model."""
        if self.registry.get(wellhead_id) is None:
            return None
        running = self.registry.parameters_for(wellhead_id)
        return {
            "wellheadId": wellhead_id,
            "modelVersion": self.defaults.version,
            "parameters": [
                {
                    "key": spec.key,
                    "description": spec.description,
                    "unit": spec.unit,
                    "defaultValue": spec.default,
                    "minimum": spec.minimum,
                    "maximum": spec.maximum,
                    "requiresRestart": spec.requires_restart,
                    "runningValue": getattr(running, spec.key) if running else None,
                }
                for spec in self.defaults.parameters.values()
            ],
        }

    def _set_fleet_ok(self, value: bool) -> None:
        """Update database readiness without exposing mutable state."""
        with self._lock:
            self._fleet_ok = value

    def _initialize_well(
        self, wellhead_id: int, snapshot: StoredSnapshot | None, now: datetime
    ) -> None:
        """Initialize process recovery and durable controls before releasing ticks."""
        state = self.registry.get(wellhead_id)
        if state is None or state.status not in (
            "initializing",
            "recovering",
        ):
            return
        if state.status == "initializing":
            self._restore_one(wellhead_id, snapshot, now)
        if self.control_store is not None:
            context = self.registry.control_context(wellhead_id)
            if context is None:
                return
            active = self.control_store.restore(context)
            if self.registry.install_controls(wellhead_id, active):
                self._controls_reconciled(wellhead_id)
        self._recovered_wells.add(wellhead_id)

    def _expire_before_recovery(self) -> None:
        """Drain expired queue pages before releasing newly recovered wells."""
        if self.control_store is None:
            return
        while not self._stop.is_set():
            expired = self.control_store.expire_queued()
            self.metrics.control_outcomes.labels(outcome="expired").inc(expired)
            if expired < 100:
                return

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
            if new_ids:
                self._expire_before_recovery()
            snapshots = (
                self.persistence.load_snapshots(new_ids)
                if self.persistence is not None
                else {}
            )
            self.registry.replace_wellheads(ids)
            if self.persistence is not None:
                now = self._wall()
                for wellhead_id in new_ids:
                    self._initialize_well(wellhead_id, snapshots.get(wellhead_id), now)
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

    def snapshot_candidates(self, wellhead_id: int, at: datetime) -> dict[str, object]:
        """Find a small saved-state window using installed parameter identity."""
        now = self._wall()
        if not now - MAX_SNAPSHOT_AGE <= at <= now:
            raise ValueError("Search time must be within the preceding 89 days")
        parameters = self.registry.parameters_for(wellhead_id)
        if parameters is None or self.persistence is None:
            raise ProjectionUnavailableError("Snapshot parameters are unavailable")
        if not self._read_slots.acquire(blocking=False):
            raise ReadCapacityError("Snapshot read capacity is full")
        try:
            candidates = self.persistence.find_snapshots(
                wellhead_id,
                max(at - SNAPSHOT_SEARCH_WINDOW, now - MAX_SNAPSHOT_AGE),
                at,
            )
        finally:
            self._read_slots.release()
        fingerprint = parameter_fingerprint(parameters)
        return {
            "wellheadId": wellhead_id,
            "checkedAt": now.isoformat(),
            "hasMore": len(candidates) > SNAPSHOT_PAGE_SIZE,
            "items": [
                {
                    **item.selection.to_payload(),
                    "modelVersion": item.model_version,
                    "compatible": item.reason(self.defaults.version, fingerprint)
                    == "compatible",
                    "reason": item.reason(self.defaults.version, fingerprint),
                }
                for item in candidates[:SNAPSHOT_PAGE_SIZE]
            ],
        }

    def _historical_base(
        self, wellhead_id: int, selection: SnapshotSelection
    ) -> ProjectionBase:
        """Read an exact snapshot within the shared database concurrency bound."""
        parameters = self.registry.parameters_for(wellhead_id)
        if parameters is None or self.persistence is None:
            raise ProjectionUnavailableError("Snapshot parameters are unavailable")
        if not self._read_slots.acquire(blocking=False):
            raise ProjectionBusyError("Snapshot read capacity is full")
        try:
            saved = self.persistence.selected_snapshot(wellhead_id, selection)
        finally:
            self._read_slots.release()
        if saved is None:
            raise SnapshotMissingError("Selected snapshot no longer exists")
        candidate, payload = saved
        return historical_base(
            wellhead_id,
            candidate,
            payload,
            parameters,
            self.defaults.version,
            self._wall(),
        )

    def create_projection(
        self,
        kind: Literal["forecast", "scenario"],
        wellhead_id: int,
        requested_by: int,
        horizon_seconds: int,
        step_seconds: int,
        *,
        name: str | None = None,
        controls: tuple[ScheduledControl, ...] = (),
        base_snapshot: SnapshotSelection | None = None,
        scheduled_job: ScheduledForecastJob | None = None,
    ) -> ProjectionResult:
        """Run one bounded private model copy and persist it atomically."""
        if self.persistence is None:
            raise ProjectionUnavailableError("Projection persistence is unavailable")
        if not self._projection_slots.acquire(blocking=False):
            self.metrics.projection_runs.labels(kind=kind, outcome="busy").inc()
            raise ProjectionBusyError("Projection capacity is full")
        started = self._monotonic()
        try:
            if scheduled_job is not None and (
                kind != "forecast"
                or scheduled_job.wellhead_id != wellhead_id
                or scheduled_job.owner_user_id != requested_by
                or scheduled_job.horizon_seconds != horizon_seconds
                or scheduled_job.step_seconds != step_seconds
            ):
                raise ValueError("Scheduled job inputs do not match the claim")
            if base_snapshot is not None and kind != "scenario":
                raise ValueError("Only scenarios accept a saved starting state")
            base = (
                self._historical_base(wellhead_id, base_snapshot)
                if base_snapshot is not None
                else self.registry.projection_base(wellhead_id)
            )
            now = self._wall()
            with self._lock:
                holding = self._holding
            if base is None or (
                base_snapshot is None
                and (
                    holding
                    or abs((now - base.state.simulated_at).total_seconds())
                    > MAX_READY_TICK_AGE_SECONDS
                )
            ):
                raise ProjectionUnavailableError("Running model state is not current")
            rules = self.persistence.load_alarm_rules()
            result = project(
                kind,
                str(uuid.uuid4()),
                base,
                now,
                horizon_seconds,
                step_seconds,
                rules,
                name=name,
                controls=controls,
            )
            result = replace(result, scheduled_job=scheduled_job)
            self.persistence.write_projection(result, requested_by)
            self.metrics.projection_runs.labels(kind=kind, outcome="complete").inc()
            return result
        except (
            DatabaseUnavailableError,
            ProjectionUnavailableError,
            ValueError,
            ArithmeticError,
        ):
            self.metrics.projection_runs.labels(kind=kind, outcome="failed").inc()
            raise
        finally:
            self.metrics.projection_duration.labels(kind=kind).observe(
                max(0.0, self._monotonic() - started)
            )
            self._projection_slots.release()

    def status_snapshot(self) -> dict[str, object]:
        """Expose lifecycle even when some wells have no valid process state."""
        now = self._wall()
        with self._lock:
            holding = self._holding
        items = []
        for state in self.registry.snapshot():
            item = state.to_payload()
            item.pop("process")
            age = (
                (now - state.process.simulated_at).total_seconds()
                if state.process
                else None
            )
            item.update(
                {
                    "simulatedAt": state.process.simulated_at.isoformat()
                    if state.process
                    else None,
                    "processAgeSeconds": age,
                    "current": state.status == "running"
                    and not holding
                    and age is not None
                    and abs(age) <= MAX_READY_TICK_AGE_SECONDS,
                }
            )
            items.append(item)
        return {"checkedAt": now.isoformat(), "available": True, "wellheads": items}

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
        if now >= self._next_refresh or self._refresh_requested.is_set():
            self._refresh_requested.clear()
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
                before_step = (
                    self.control_inbox.apply_one
                    if self.control_inbox is not None and index == batch.steps - 1
                    else None
                )
                advanced, failed = self.registry.tick(at, before_step)
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
