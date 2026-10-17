"""Bounded claims and atomic completion checks for scheduled forecasts."""

from __future__ import annotations

from contextlib import closing
from datetime import timedelta
from uuid import uuid4

import psycopg2
import psycopg2.extensions

from twin_core.config import ServiceSettings
from twin_core.database import DatabaseUnavailableError
from twin_core.forecast_job import ScheduledForecastJob, due_slot

LEASE_SECONDS = 60


class ForecastClaimError(ValueError):
    """A claim expired or its owner no longer authorizes scheduled work."""


def _authority(cursor: psycopg2.extensions.cursor, owner: int, role: str) -> bool:
    """Hold the same account-first lock order as administrator writes."""
    cursor.execute("SELECT * FROM control_actor_authority(%s)", (owner,))
    row = cursor.fetchone()
    return bool(
        row and row[0] == role == "ADMIN" and row[1] == "ACTIVE" and row[2] is not True
    )


def authorize_completion(
    cursor: psycopg2.extensions.cursor, job: ScheduledForecastJob
) -> None:
    """Fence late workers and authority changes before any result insertion."""
    if not _authority(cursor, job.owner_user_id, job.owner_role):
        raise ForecastClaimError("authority_revoked")
    cursor.execute(
        "SELECT status, lease_expires_at > clock_timestamp(), "
        "owner_user_id, schedule_id, schedule_revision, wellhead_id, "
        "horizon_seconds, step_seconds, scheduled_for "
        "FROM forecast_job WHERE job_id=%s FOR UPDATE",
        (job.job_id,),
    )
    row = cursor.fetchone()
    if (
        not row
        or row[:2] != ("running", True)
        or (
            row[2] != job.owner_user_id
            or str(row[3]) != job.schedule_id
            or row[4] != job.schedule_revision
            or row[5] != job.wellhead_id
            or row[6] != job.horizon_seconds
            or row[7] != job.step_seconds
            or row[8] != job.scheduled_for
        )
    ):
        raise ForecastClaimError("claim_expired")


def complete_job(
    cursor: psycopg2.extensions.cursor, job: ScheduledForecastJob, run_id: str
) -> None:
    """Finish under the job lock held by authorize_completion in this transaction."""
    cursor.execute(
        "UPDATE forecast_job SET status='complete', completed_at=clock_timestamp(), "
        "reason_code=NULL, run_id=%s WHERE job_id=%s AND status='running' "
        "AND lease_expires_at > clock_timestamp()",
        (run_id, job.job_id),
    )
    if cursor.rowcount != 1:
        raise ForecastClaimError("claim_expired")


class ForecastStore:
    """Give each scheduler operation a short connection and bounded transaction."""

    def __init__(self, settings: ServiceSettings) -> None:
        """Keep credentials private, following the existing model stores."""
        self.settings = settings

    def _connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.settings.database_host,
            port=self.settings.database_port,
            dbname=self.settings.database_name,
            user=self.settings.database_user,
            password=self.settings.database_password,
            connect_timeout=3,
            options="-c statement_timeout=3000 -c lock_timeout=500 -c timezone=UTC",
        )

    def expire(self) -> int:
        """Close a bounded batch of abandoned claims without replaying them."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    "WITH expired AS (SELECT job_id FROM forecast_job "
                    "WHERE status='running' AND lease_expires_at <= clock_timestamp() "
                    "ORDER BY lease_expires_at, job_id LIMIT 100 FOR UPDATE "
                    "SKIP LOCKED) "
                    "UPDATE forecast_job j SET status='failed', "
                    "completed_at=clock_timestamp(), reason_code='worker_interrupted' "
                    "FROM expired e WHERE j.job_id=e.job_id"
                )
                count = cur.rowcount
                conn.commit()
                return count
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError(
                "Scheduled claim cleanup unavailable"
            ) from exc

    def claim(self) -> ScheduledForecastJob | None:
        """Peek, lock the owner, then claim a still-matching due schedule."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT schedule_id, owner_user_id, owner_role FROM "
                    "forecast_schedule s "
                    "WHERE enabled AND next_due_at <= clock_timestamp() "
                    "AND NOT EXISTS (SELECT 1 FROM forecast_job j WHERE "
                    "j.schedule_id=s.schedule_id AND j.status='running') "
                    "ORDER BY next_due_at, schedule_id LIMIT 1"
                )
                candidate = cur.fetchone()
                if candidate is None:
                    return None
                allowed = _authority(cur, candidate[1], candidate[2])
                cur.execute(
                    "SELECT schedule_id, revision, wellhead_id, "
                    "owner_user_id, owner_role, "
                    "interval_seconds, horizon_seconds, step_seconds, next_due_at, "
                    "clock_timestamp() FROM forecast_schedule WHERE schedule_id=%s "
                    "AND owner_user_id=%s AND owner_role=%s AND enabled "
                    "AND next_due_at <= clock_timestamp() "
                    "AND NOT EXISTS (SELECT 1 FROM forecast_job j WHERE "
                    "j.schedule_id=forecast_schedule.schedule_id "
                    "AND j.status='running') FOR UPDATE SKIP LOCKED",
                    candidate,
                )
                row = cur.fetchone()
                if row is None:
                    return None
                scheduled, following, missed = due_slot(row[9], row[8], row[5])
                job = ScheduledForecastJob(
                    str(uuid4()),
                    str(row[0]),
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    scheduled,
                    row[9] + timedelta(seconds=LEASE_SECONDS),
                    missed,
                    "running" if allowed else "skipped",
                    None if allowed else "authority_revoked",
                )
                cur.execute(
                    "INSERT INTO forecast_job (job_id, schedule_id, schedule_revision, "
                    "wellhead_id, owner_user_id, owner_role, "
                    "interval_seconds, horizon_seconds, "
                    "step_seconds, scheduled_for, started_at, "
                    "lease_expires_at, missed_intervals, "
                    "status, completed_at, reason_code) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        job.job_id,
                        job.schedule_id,
                        job.schedule_revision,
                        job.wellhead_id,
                        job.owner_user_id,
                        job.owner_role,
                        job.interval_seconds,
                        job.horizon_seconds,
                        job.step_seconds,
                        scheduled,
                        row[9],
                        job.lease_expires_at,
                        missed,
                        job.status,
                        None if allowed else row[9],
                        job.reason_code,
                    ),
                )
                if allowed:
                    cur.execute(
                        "UPDATE forecast_schedule SET next_due_at=%s WHERE "
                        "schedule_id=%s",
                        (following, job.schedule_id),
                    )
                else:
                    cur.execute(
                        "UPDATE forecast_schedule SET enabled=FALSE, next_due_at=NULL, "
                        "revision=revision+1, updated_at=clock_timestamp() "
                        "WHERE schedule_id=%s",
                        (job.schedule_id,),
                    )
                conn.commit()
                return job
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError(
                "Scheduled forecast claim unavailable"
            ) from exc

    def fail(self, job: ScheduledForecastJob, reason: str) -> str:
        """Record a failure without rewriting an already committed outcome."""
        try:
            with closing(self._connect()) as conn, conn.cursor() as cur:
                # Hold owner before schedule/job when revocation also disables work.
                allowed = _authority(cur, job.owner_user_id, job.owner_role)
                if not allowed:
                    cur.execute(
                        "UPDATE forecast_schedule SET enabled=FALSE, next_due_at=NULL, "
                        "revision=revision+1, updated_at=clock_timestamp() "
                        "WHERE schedule_id=%s AND owner_user_id=%s AND enabled",
                        (job.schedule_id, job.owner_user_id),
                    )
                cur.execute(
                    "UPDATE forecast_job SET status='failed', "
                    "completed_at=clock_timestamp(), "
                    "reason_code=%s WHERE job_id=%s AND status='running' "
                    "RETURNING status",
                    (reason, job.job_id),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT status FROM forecast_job WHERE job_id=%s", (job.job_id,)
                    )
                    row = cur.fetchone()
                conn.commit()
                return str(row[0]) if row else "unknown"
        except psycopg2.Error as exc:
            raise DatabaseUnavailableError(
                "Scheduled forecast outcome unavailable"
            ) from exc
