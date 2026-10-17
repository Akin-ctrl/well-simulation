"""Immutable scheduled forecast context and due-time arithmetic."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class ScheduledForecastJob:
    """One claimed occurrence; a failed or expired occurrence is never reassigned."""

    job_id: str
    schedule_id: str
    schedule_revision: int
    wellhead_id: int
    owner_user_id: int
    owner_role: str
    interval_seconds: int
    horizon_seconds: int
    step_seconds: int
    scheduled_for: datetime
    lease_expires_at: datetime
    missed_intervals: int
    status: str = "running"
    reason_code: str | None = None

    def execution_origin(self) -> dict[str, str]:
        """Explain automation without representing it as a manual request."""
        return {
            "kind": "scheduled",
            "scheduleId": self.schedule_id,
            "jobId": self.job_id,
            "scheduledFor": self.scheduled_for.isoformat(),
        }


def due_slot(
    now: datetime, due: datetime, interval_seconds: int
) -> tuple[datetime, datetime, int]:
    """Return one current due slot and skip missed intervals without a backlog."""
    if interval_seconds < 300 or interval_seconds > 86400 or interval_seconds % 60:
        raise ValueError("Invalid forecast interval")
    if now < due:
        raise ValueError("Schedule is not due")
    interval = timedelta(seconds=interval_seconds)
    missed = (now - due) // interval
    scheduled = due + missed * interval
    return scheduled, scheduled + interval, missed
