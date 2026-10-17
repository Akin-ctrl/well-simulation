"""Persist synthetic control revisions with atomic command outcomes."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import cast

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from twin_core.config import ServiceSettings
from twin_core.database import DatabaseUnavailableError
from twin_core.model import ControlInputs
from twin_core.projection import ControlChange


class ControlCommitUnknownError(DatabaseUnavailableError):
    """The server may have committed before the connection lost its reply."""


@dataclass(frozen=True)
class ActiveControls:
    """One durable setpoint revision and its model identity."""

    revision: int
    model_version: str
    parameter_fingerprint: str
    controls: ControlInputs
    command_id: str | None = None


@dataclass(frozen=True)
class ControlContext:
    """Current model identity copied while holding the registry lock."""

    wellhead_id: int
    model_version: str
    parameter_fingerprint: str
    status: str
    controls: ControlInputs
    revision: int


@dataclass(frozen=True)
class CommandRef:
    """A queue hint whose mutable fields must be read again in the transaction."""

    command_id: str
    wellhead_id: int
    actor_id: int


@dataclass(frozen=True)
class QueuedCommand:
    """The locked command fields relevant to application."""

    status: str
    expires_at: datetime
    expected_revision: int
    model_version: str
    parameter_fingerprint: str
    activated_role: str
    activated_session_version: int
    targets: ControlChange


@dataclass(frozen=True)
class CommandOutcome:
    """Confirmed result; active is present only after an applied commit."""

    status: str
    reason: str
    active: ActiveControls | None = None


def merge_live_controls(current: ControlInputs, change: ControlChange) -> ControlInputs:
    """Use existing model validation and require a separate shutdown/reset."""
    supplied = [value for value in vars(change).values() if value is not None]
    if not supplied:
        raise ValueError("A live command needs at least one target")
    if change.emergency_shutdown is not None and len(supplied) != 1:
        raise ValueError("Shutdown or reset must be a separate command")
    if current.emergency_shutdown and change.emergency_shutdown is None:
        raise ValueError("Reset shutdown before changing other controls")
    return change.apply(current)


def command_rejection(
    command: QueuedCommand,
    context: ControlContext | None,
    active: ActiveControls,
    authority: tuple[str, str, bool | None, int] | None,
    now: datetime,
) -> str | None:
    """Choose a bounded rejection reason without changing model state."""
    if command.expires_at <= now:
        return "expired"
    if authority is None or (
        authority[0] != command.activated_role
        or authority[1] != "ACTIVE"
        or authority[2] is True
        or authority[3] != command.activated_session_version
    ):
        return "authority_revoked"
    if context is None or context.status != "running":
        return "model_unavailable"
    if (
        command.expected_revision != active.revision
        or context.revision != active.revision
    ):
        return "revision_changed"
    if (
        command.model_version != context.model_version
        or active.model_version != context.model_version
    ):
        return "model_changed"
    if (
        command.parameter_fingerprint != context.parameter_fingerprint
        or active.parameter_fingerprint != context.parameter_fingerprint
    ):
        return "parameters_changed"
    return None


def _active(row: tuple[object, ...] | None) -> ActiveControls:
    """Decode typed SQL columns, validating every full control set."""
    if row is None:
        raise DatabaseUnavailableError("Active controls missing")
    controls = ControlInputs(
        choke_position_percent=cast(float, row[3]),
        master_valve_open=cast(bool, row[4]),
        wing_valve_open=cast(bool, row[5]),
        swab_valve_open=cast(bool, row[6]),
        pump_on=cast(bool, row[7]),
        emergency_shutdown=cast(bool, row[8]),
    )
    controls.validate()
    return ActiveControls(
        cast(int, row[0]),
        str(row[1]),
        str(row[2]),
        controls,
        str(row[9]) if row[9] else None,
    )


ACTIVE_COLUMNS = (
    "revision, model_version, parameter_fingerprint, choke_position_percent, "
    "master_valve_open, wing_valve_open, swab_valve_open, pump_on, "
    "emergency_shutdown, applied_command_id"
)


class ControlStore:
    """Keep the command, active row, and lifecycle event in one transaction."""

    def __init__(self, settings: ServiceSettings) -> None:
        """Retain connection settings without exposing credentials."""
        self.settings = settings

    def _connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.settings.database_host,
            port=self.settings.database_port,
            dbname=self.settings.database_name,
            user=self.settings.database_user,
            password=self.settings.database_password,
            connect_timeout=2,
            options="-c statement_timeout=250 -c lock_timeout=75",
        )

    def scan(self) -> tuple[tuple[CommandRef, ...], int]:
        """Read the oldest hundred commands and report the complete queue depth."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT command_id, wellhead_id, activated_by, count(*) OVER () "
                    "FROM control_command WHERE status = 'queued' "
                    "ORDER BY queued_at, command_id LIMIT 100"
                )
                rows = cur.fetchall()
            return tuple(CommandRef(str(r[0]), r[1], r[2]) for r in rows), rows[0][
                3
            ] if rows else 0
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Control inbox unavailable") from exc

    def expire_queued(self) -> int:
        """Expire a bounded batch with matching events before replay is possible."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    "WITH candidates AS (SELECT command_id FROM control_command "
                    "WHERE status='queued' AND expires_at <= clock_timestamp() "
                    "ORDER BY queued_at LIMIT 100 FOR UPDATE SKIP LOCKED), "
                    "expired AS (UPDATE control_command AS command "
                    "SET status='expired', result_at=clock_timestamp(), "
                    "result_reason='expired' FROM candidates "
                    "WHERE command.command_id=candidates.command_id "
                    "RETURNING command.command_id, command.request_id) "
                    "INSERT INTO control_command_event "
                    "(command_id,event_type,reason_code,request_id) "
                    "SELECT command_id,'expired','expired',request_id FROM expired"
                )
                count = cur.rowcount
                conn.commit()
                return count
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Command expiry unavailable") from exc

    def restore(self, context: ControlContext) -> ActiveControls:
        """Bootstrap a missing row, or adopt saved controls and the running identity."""
        values = context.controls
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO wellhead_control_state "
                    "(wellhead_id, model_version, parameter_fingerprint, "
                    "choke_position_percent, master_valve_open, wing_valve_open, "
                    "swab_valve_open, pump_on, emergency_shutdown) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        context.wellhead_id,
                        context.model_version,
                        context.parameter_fingerprint,
                        values.choke_position_percent,
                        values.master_valve_open,
                        values.wing_valve_open,
                        values.swab_valve_open,
                        values.pump_on,
                        values.emergency_shutdown,
                    ),
                )
                cur.execute(
                    sql.SQL(
                        "UPDATE wellhead_control_state SET model_version=%s, "
                        "parameter_fingerprint=%s WHERE wellhead_id=%s RETURNING {}"
                    ).format(sql.SQL(ACTIVE_COLUMNS)),
                    (
                        context.model_version,
                        context.parameter_fingerprint,
                        context.wellhead_id,
                    ),
                )
                active = _active(cur.fetchone())
                conn.commit()
                return active
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Active controls unavailable") from exc

    def read(self, wellhead_id: int) -> ActiveControls:
        """Resolve an uncertain commit from the authoritative active row."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT {} FROM wellhead_control_state WHERE wellhead_id=%s"
                    ).format(sql.SQL(ACTIVE_COLUMNS)),
                    (wellhead_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise DatabaseUnavailableError("Active controls missing")
                active = _active(row)
                if active.command_id is not None:
                    cur.execute(
                        "SELECT status, result_revision FROM control_command "
                        "WHERE command_id=%s",
                        (active.command_id,),
                    )
                    if cur.fetchone() != ("applied", active.revision):
                        raise DatabaseUnavailableError("Active control result differs")
                return active
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError("Active controls unavailable") from exc

    def apply(self, ref: CommandRef, context: ControlContext | None) -> CommandOutcome:
        """Recheck authority, revision, identity, expiry, and targets before commit."""
        committing = False
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                # Every mutation takes locks in this order, including the API.
                cur.execute(
                    "SELECT * FROM control_actor_authority(%s)", (ref.actor_id,)
                )
                authority = cur.fetchone()
                cur.execute(
                    "SELECT status, expires_at, expected_revision, model_version, "
                    "parameter_fingerprint, activated_role, activated_session_version, "
                    "choke_position_percent, master_valve_open, wing_valve_open, "
                    "pump_on, emergency_shutdown FROM control_command "
                    "WHERE command_id=%s AND wellhead_id=%s "
                    "AND activated_by=%s FOR UPDATE",
                    (ref.command_id, ref.wellhead_id, ref.actor_id),
                )
                row = cur.fetchone()
                if row is None or row[0] != "queued":
                    return CommandOutcome("unchanged", "already_terminal")
                command = QueuedCommand(
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    ControlChange(row[7], row[8], row[9], row[10], row[11]),
                )
                cur.execute(
                    sql.SQL(
                        "SELECT {} FROM wellhead_control_state "
                        "WHERE wellhead_id=%s FOR UPDATE"
                    ).format(sql.SQL(ACTIVE_COLUMNS)),
                    (ref.wellhead_id,),
                )
                active_row = cur.fetchone()
                if active_row is None:
                    raise DatabaseUnavailableError("Active controls missing")
                active = _active(active_row)
                cur.execute("SELECT clock_timestamp()")
                clock = cur.fetchone()
                if clock is None:
                    raise DatabaseUnavailableError("Database clock unavailable")
                reason = command_rejection(
                    command, context, active, authority, clock[0]
                )
                target = active.controls
                if reason is None:
                    try:
                        target = merge_live_controls(active.controls, command.targets)
                    except ValueError:
                        reason = "invalid_targets"
                outcome = self._write_outcome(cur, ref, active, target, reason)
                committing = True
                conn.commit()
                return outcome
        except psycopg2.Error as exc:
            if committing:
                raise ControlCommitUnknownError(
                    "Control commit outcome unknown"
                ) from exc
            raise DatabaseUnavailableError("Control transaction failed") from exc

    @staticmethod
    def _write_outcome(
        cur: psycopg2.extensions.cursor,
        ref: CommandRef,
        active: ActiveControls,
        target: ControlInputs,
        reason: str | None,
    ) -> CommandOutcome:
        status = (
            "expired" if reason == "expired" else "rejected" if reason else "applied"
        )
        updated: ActiveControls | None = None
        if reason is None:
            cur.execute(
                "UPDATE wellhead_control_state SET revision=revision+1, "
                "choke_position_percent=%s, master_valve_open=%s, wing_valve_open=%s, "
                "swab_valve_open=%s, pump_on=%s, emergency_shutdown=%s, "
                "applied_command_id=%s, applied_at=clock_timestamp() "
                "WHERE wellhead_id=%s",
                (
                    target.choke_position_percent,
                    target.master_valve_open,
                    target.wing_valve_open,
                    target.swab_valve_open,
                    target.pump_on,
                    target.emergency_shutdown,
                    ref.command_id,
                    ref.wellhead_id,
                ),
            )
            updated = ActiveControls(
                active.revision + 1,
                active.model_version,
                active.parameter_fingerprint,
                target,
                ref.command_id,
            )
        cur.execute(
            "UPDATE control_command SET status=%s, result_at=clock_timestamp(), "
            "result_revision=%s, result_reason=%s WHERE command_id=%s",
            (
                status,
                updated.revision if updated else None,
                reason or "applied",
                ref.command_id,
            ),
        )
        cur.execute(
            "INSERT INTO control_command_event "
            "(command_id,event_type,reason_code,request_id) "
            "SELECT command_id,%s,%s,request_id FROM control_command "
            "WHERE command_id=%s",
            (status, reason or "applied", ref.command_id),
        )
        return CommandOutcome(status, reason or "applied", updated)
