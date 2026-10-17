"""Bounded, append-only persistence for synthetic twin states and recovery."""

from __future__ import annotations

import logging
import math
import threading
from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Literal, cast

import psycopg2
import psycopg2.extensions
from psycopg2.extras import Json, execute_batch

from twin_core.config import ParameterOverride, ServiceSettings
from twin_core.database import DatabaseUnavailableError
from twin_core.historical import (
    SNAPSHOT_PAGE_SIZE,
    SnapshotCandidate,
    SnapshotSelection,
)
from twin_core.metrics import TwinMetrics
from twin_core.projection import (
    MODEL_SIGNALS,
    AlarmRule,
    AlarmRuleConfigurationError,
    ProjectionResult,
)
from twin_core.state import WellheadState

logger = logging.getLogger(__name__)
SNAPSHOT_INTERVAL_SECONDS = 30
EVENT_CAPACITY = 100
SHUTDOWN_WAIT_SECONDS = 3


@dataclass(frozen=True)
class StoredSnapshot:
    """The latest persisted model state for one well."""

    wellhead_id: int
    simulated_at: datetime
    model_version: str
    parameter_fingerprint: str
    tick_index: int
    skipped_ticks: int
    state: object


@dataclass(frozen=True)
class RecoveryEvent:
    """One durable recovery or clock adjustment fact."""

    occurred_at: datetime
    wellhead_id: int | None
    event_type: str
    model_version: str
    snapshot_simulated_at: datetime | None = None
    gap_seconds: float | None = None
    details: Mapping[str, object] | None = None


SnapshotCopy = tuple[tuple[WellheadState, str], ...]


class TwinPersistence:
    """Use short read-only queries and separate append-only write connections."""

    def __init__(self, settings: ServiceSettings) -> None:
        """Keep credentials in memory without logging them."""
        self._settings = settings

    def _connect(self, *, read_only: bool) -> psycopg2.extensions.connection:
        options = "-c statement_timeout=3000"
        if read_only:
            options += " -c default_transaction_read_only=on"
        return psycopg2.connect(
            host=self._settings.database_host,
            port=self._settings.database_port,
            dbname=self._settings.database_name,
            user=self._settings.database_user,
            password=self._settings.database_password,
            connect_timeout=3,
            options=options,
        )

    def load_snapshots(self, wellhead_ids: Sequence[int]) -> dict[int, StoredSnapshot]:
        """Read one newest snapshot per active well in a bounded transaction."""
        if not wellhead_ids:
            return {}
        try:
            with closing(self._connect(read_only=True)) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT ON (wellhead_id) wellhead_id, simulated_at, "
                    "model_version, parameter_fingerprint, tick_index, "
                    "skipped_ticks, state FROM twin_state_snapshot "
                    "WHERE wellhead_id = ANY(%s) "
                    "ORDER BY wellhead_id, simulated_at DESC, snapshot_id DESC",
                    (list(wellhead_ids),),
                )
                rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not load twin snapshots") from exc
        return {
            int(row[0]): StoredSnapshot(
                int(row[0]),
                row[1],
                str(row[2]),
                str(row[3]),
                int(row[4]),
                int(row[5]),
                row[6],
            )
            for row in rows
        }

    def find_snapshots(
        self, wellhead_id: int, start: datetime, end: datetime
    ) -> tuple[SnapshotCandidate, ...]:
        """Read one bounded time window without transferring full process JSON."""
        try:
            with closing(self._connect(read_only=True)) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT snapshot_id, simulated_at, model_version, "
                    "parameter_fingerprint FROM twin_state_snapshot "
                    "WHERE wellhead_id = %s AND simulated_at >= %s "
                    "AND simulated_at <= %s "
                    "ORDER BY simulated_at DESC, snapshot_id DESC LIMIT %s",
                    (wellhead_id, start, end, SNAPSHOT_PAGE_SIZE + 1),
                )
                return tuple(
                    SnapshotCandidate(
                        SnapshotSelection(str(row[0]), row[1]), row[2], row[3]
                    )
                    for row in cur.fetchall()
                )
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not find saved snapshots") from exc

    def selected_snapshot(
        self, wellhead_id: int, selection: SnapshotSelection
    ) -> tuple[SnapshotCandidate, object] | None:
        """Use well ownership and both primary-key fields for an exact read."""
        try:
            with closing(self._connect(read_only=True)) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT model_version, parameter_fingerprint, state "
                    "FROM twin_state_snapshot WHERE wellhead_id = %s "
                    "AND snapshot_id = %s AND simulated_at = %s",
                    (wellhead_id, int(selection.snapshot_id), selection.simulated_at),
                )
                row = cur.fetchone()
                return (
                    (SnapshotCandidate(selection, row[0], row[1]), row[2])
                    if row is not None
                    else None
                )
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not read selected snapshot") from exc

    def load_overrides(
        self, wellhead_ids: Sequence[int], model_version: str
    ) -> dict[int, dict[str, ParameterOverride]]:
        """Read the latest effective row for each well and parameter key."""
        if not wellhead_ids:
            return {}
        try:
            with closing(self._connect(read_only=True)) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT ON (wellhead_id, parameter_key) "
                    "wellhead_id, parameter_key, value, unit, source "
                    "FROM wellhead_model_parameter "
                    "WHERE wellhead_id = ANY(%s) AND model_version = %s "
                    "AND valid_from <= now() "
                    "ORDER BY wellhead_id, parameter_key, valid_from DESC, "
                    "parameter_override_id DESC",
                    (list(wellhead_ids), model_version),
                )
                rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not load model overrides") from exc
        overrides: dict[int, dict[str, ParameterOverride]] = {}
        for wellhead_id, key, value, unit, source in rows:
            if value is not None:
                overrides.setdefault(int(wellhead_id), {})[str(key)] = (
                    ParameterOverride(float(value), str(unit), str(source))
                )
        return overrides

    def load_alarm_rules(self) -> tuple[AlarmRule, ...]:
        """Read active thresholds once for one whole projection run."""
        try:
            with closing(self._connect(read_only=True)) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT ar.alarm_rule_id, pt.code, ar.severity_level, "
                    "ar.operator, ar.threshold_value FROM alarmRule ar "
                    "JOIN parameterType pt ON "
                    "pt.parameter_type_id = ar.parameter_type_id "
                    "WHERE ar.active = TRUE ORDER BY ar.alarm_rule_id"
                )
                rows = cur.fetchall()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not load active alarm rules") from exc
        rules: list[AlarmRule] = []
        for rule_id, code, severity, operator, threshold in rows:
            if (
                code not in MODEL_SIGNALS
                or severity not in ("WARNING", "CRITICAL")
                or operator not in (">", ">=", "<", "<=", "==")
                or not isinstance(threshold, (float, int))
                or not math.isfinite(threshold)
            ):
                raise AlarmRuleConfigurationError(
                    "Active alarm rule is not projectable"
                )
            rules.append(
                AlarmRule(
                    int(rule_id),
                    str(code),
                    cast("Literal['WARNING', 'CRITICAL']", severity),
                    cast("Literal['>', '>=', '<', '<=', '==']", operator),
                    float(threshold),
                )
            )
        return tuple(rules)

    def write_projection(self, result: ProjectionResult, requested_by: int) -> None:
        """Insert a completed run and every point in one transaction."""
        base = result.base
        common = (
            result.run_id,
            result.generated_at,
            base.wellhead_id,
            requested_by,
            base.model_version,
            base.parameter_fingerprint,
            base.state.simulated_at,
            result.horizon_seconds,
            result.step_seconds,
            Json({**base.state.to_payload(), "branchOrigin": base.branch_origin()}),
            Json(asdict(base.parameters)),
            Json(asdict(base.controls)),
        )
        rules = Json([asdict(rule) for rule in result.alarm_rules])
        reasons = Json(list(result.confidence_reasons))
        try:
            with closing(self._connect(read_only=False)) as conn, conn.cursor() as cur:
                if result.kind == "forecast":
                    cur.execute(
                        "INSERT INTO prediction_run (run_id, created_at, wellhead_id, "
                        "requested_by_user_id, model_version, parameter_fingerprint, "
                        "base_simulated_at, horizon_seconds, step_seconds, base_state, "
                        "parameter_values, control_values, alarm_rules, "
                        "confidence_score, confidence_reasons) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        "%s, %s, %s)",
                        (*common, rules, result.confidence_score, reasons),
                    )
                    execute_batch(
                        cur,
                        "INSERT INTO prediction_point "
                        "(run_id, forecast_at, elapsed_seconds, values_json, "
                        "tubing_pressure_psi, flow_rate_bpd, "
                        "wellhead_temperature_f, water_cut_percent, "
                        "alarm_risk, triggered_rule_ids) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        [
                            (
                                result.run_id,
                                point.at,
                                point.elapsed_seconds,
                                Json(point.values),
                                point.values["tubing_pressure"],
                                point.values["flow_rate"],
                                point.values["wellhead_temperature"],
                                point.values["water_cut"],
                                point.alarm_risk,
                                Json(list(point.triggered_rule_ids)),
                            )
                            for point in result.points
                        ],
                    )
                else:
                    cur.execute(
                        "INSERT INTO scenario_run (run_id, created_at, wellhead_id, "
                        "requested_by_user_id, name, model_version, "
                        "parameter_fingerprint, "
                        "base_simulated_at, horizon_seconds, step_seconds, base_state, "
                        "parameter_values, control_values, scheduled_controls, "
                        "alarm_rules, "
                        "confidence_score, confidence_reasons) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        "%s, %s, %s, %s, %s)",
                        (
                            *common[:4],
                            result.name,
                            *common[4:],
                            Json([asdict(step) for step in result.controls]),
                            rules,
                            result.confidence_score,
                            reasons,
                        ),
                    )
                    execute_batch(
                        cur,
                        "INSERT INTO scenario_point "
                        "(run_id, forecast_at, elapsed_seconds, values_json, "
                        "tubing_pressure_psi, flow_rate_bpd, "
                        "wellhead_temperature_f, water_cut_percent, "
                        "baseline_values_json, alarm_risk, baseline_alarm_risk, "
                        "triggered_rule_ids) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        [
                            (
                                result.run_id,
                                point.at,
                                point.elapsed_seconds,
                                Json(point.values),
                                point.values["tubing_pressure"],
                                point.values["flow_rate"],
                                point.values["wellhead_temperature"],
                                point.values["water_cut"],
                                Json(point.baseline_values),
                                point.alarm_risk,
                                point.baseline_alarm_risk,
                                Json(list(point.triggered_rule_ids)),
                            )
                            for point in result.points
                        ],
                    )
                conn.commit()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not save projection run") from exc

    def write_snapshots(self, copy: SnapshotCopy) -> int:
        """Insert one fleet state in one transaction, or none on failure."""
        rows: list[tuple[object, ...]] = []
        for well, fingerprint in copy:
            process = well.process
            if well.status != "running" or process is None:
                continue
            rows.append(
                (
                    process.simulated_at,
                    well.wellhead_id,
                    well.model_version,
                    fingerprint,
                    well.status,
                    well.tick_index,
                    well.skipped_ticks,
                    process.tubing_pressure_psi,
                    process.casing_pressure_psi,
                    process.flow_rate_bpd,
                    process.wellhead_temperature_f,
                    process.water_cut_percent,
                    process.choke_position_percent,
                    process.blockage_factor,
                    process.corrosion_factor,
                    Json(process.to_payload()),
                )
            )
        if not rows:
            return 0
        try:
            with closing(self._connect(read_only=False)) as conn, conn.cursor() as cur:
                execute_batch(
                    cur,
                    "INSERT INTO twin_state_snapshot (simulated_at, wellhead_id, "
                    "model_version, parameter_fingerprint, status, tick_index, "
                    "skipped_ticks, tubing_pressure_psi, casing_pressure_psi, "
                    "flow_rate_bpd, wellhead_temperature_f, water_cut_percent, "
                    "choke_position_percent, blockage_factor, corrosion_factor, "
                    "state) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s)",
                    rows,
                )
                conn.commit()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not save twin snapshots") from exc
        return len(rows)

    def write_event(self, event: RecoveryEvent) -> None:
        """Append an audit event without updating any prior row."""
        try:
            with closing(self._connect(read_only=False)) as conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO twin_recovery_event "
                    "(occurred_at, wellhead_id, event_type, model_version, "
                    "snapshot_simulated_at, gap_seconds, details) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        event.occurred_at,
                        event.wellhead_id,
                        event.event_type,
                        event.model_version,
                        event.snapshot_simulated_at,
                        event.gap_seconds,
                        Json(dict(event.details or {})),
                    ),
                )
                conn.commit()
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Could not save recovery event") from exc


class DatabaseOverrideSource:
    """Supply per-well overrides from the authoritative append-only table."""

    def __init__(self, persistence: TwinPersistence) -> None:
        """Share the same bounded database boundary as snapshot loading."""
        self._persistence = persistence

    def load(
        self, wellhead_ids: Sequence[int], model_version: str
    ) -> Mapping[int, Mapping[str, ParameterOverride]]:
        """Return effective overrides to the model's existing validator."""
        return self._persistence.load_overrides(wellhead_ids, model_version)


class PersistenceWriter:
    """Move database inserts off the tick thread with bounded mailboxes."""

    def __init__(self, persistence: TwinPersistence, metrics: TwinMetrics) -> None:
        """Create one snapshot slot and a bounded event queue."""
        self._persistence = persistence
        self._metrics = metrics
        self._condition = threading.Condition()
        self._pending: SnapshotCopy | None = None
        self._events: deque[RecoveryEvent] = deque()
        self._stopping = False
        self._last_saved_at: datetime | None = None
        self._thread = threading.Thread(
            target=self._run, name="twin-persistence", daemon=True
        )

    def start(self) -> None:
        """Start the sole database writer thread."""
        self._thread.start()

    def submit_snapshot(self, copy: SnapshotCopy) -> None:
        """Keep the newest fleet copy without waiting for the database."""
        with self._condition:
            if self._pending is not None:
                self._metrics.snapshots_replaced.inc()
            self._pending = copy
            self._condition.notify()

    def submit_event(self, event: RecoveryEvent) -> None:
        """Queue an event, or report an explicit capacity overflow."""
        with self._condition:
            if len(self._events) == EVENT_CAPACITY:
                self._metrics.recovery_events_dropped.inc()
                logger.error(
                    "Recovery event queue is full",
                    extra={"event_type": event.event_type},
                )
                return
            self._events.append(event)
            self._condition.notify()

    def stop(self, final_copy: SnapshotCopy) -> bool:
        """Save the final state directly, then give queued events time to drain.

        The tick loop has stopped, so this database call cannot delay a model
        step. It also cannot wait behind an event or an older snapshot in the
        background writer. False means the final write failed and was counted.
        """
        with self._condition:
            if self._pending is not None:
                self._metrics.snapshots_replaced.inc()
            self._pending = None
            self._stopping = True
            self._condition.notify()
        saved = self._save_snapshot(final_copy, phase="shutdown")
        self._thread.join(timeout=SHUTDOWN_WAIT_SECONDS)
        if self._thread.is_alive():
            with self._condition:
                queued_events = len(self._events)
            logger.error(
                "Persistence writer did not finish before shutdown",
                extra={"queued_events": queued_events},
            )
        return saved

    def refresh_age(self, now: datetime) -> None:
        """Report elapsed wall time since a fleet snapshot was saved."""
        with self._condition:
            last = self._last_saved_at
        if last is not None:
            self._metrics.last_snapshot_age_seconds.set(
                max(0.0, (now - last).total_seconds())
            )

    def _save_snapshot(self, copy: SnapshotCopy, *, phase: str) -> bool:
        """Write one immutable copy and record success or failure once."""
        try:
            count = self._persistence.write_snapshots(copy)
        except DatabaseUnavailableError:
            self._metrics.snapshot_write_failures.inc()
            logger.error("Twin snapshot write failed", extra={"phase": phase})
            return False
        self._metrics.snapshot_writes.inc(count)
        if count:
            saved_at = max(
                state.process.simulated_at
                for state, _fingerprint in copy
                if state.status == "running" and state.process is not None
            )
            with self._condition:
                if self._last_saved_at is None or saved_at > self._last_saved_at:
                    self._last_saved_at = saved_at
            self.refresh_age(datetime.now(timezone.utc))
        return True

    def _run(self) -> None:
        """Drain events and newest snapshots, keeping failures observable."""
        while True:
            with self._condition:
                self._condition.wait_for(
                    lambda: self._stopping or self._events or self._pending is not None
                )
                event = self._events.popleft() if self._events else None
                copy = self._pending
                self._pending = None
                stopping = self._stopping
            if event is not None:
                try:
                    self._persistence.write_event(event)
                except DatabaseUnavailableError:
                    self._metrics.database_errors.labels(
                        operation="recovery_event_write"
                    ).inc()
                    logger.error(
                        "Recovery event write failed",
                        extra={"event_type": event.event_type},
                    )
            if copy is not None:
                self._save_snapshot(copy, phase="periodic")
            with self._condition:
                if stopping and not self._events and self._pending is None:
                    return
