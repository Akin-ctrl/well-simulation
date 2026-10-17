"""Plan bounded fixed-step ticks, and reconcile tick time with the wall clock."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True)
class TickBatch:
    """Work due at one scheduler pass."""

    steps: int
    skipped: int
    lag_seconds: float
    next_due: float


def plan_ticks(
    now: float, next_due: float, interval_seconds: float, max_catchup_steps: int
) -> TickBatch:
    """Run only a bounded number of one-step updates after a delay.

    Any excess steps are counted as a gap. They are never collapsed into one
    large model step or silently replayed.
    """
    if not all(math.isfinite(value) for value in (now, next_due, interval_seconds)):
        raise ValueError("Scheduler times must be finite")
    if interval_seconds <= 0 or max_catchup_steps <= 0:
        raise ValueError("Scheduler interval and catch-up budget must be positive")
    if now < next_due:
        return TickBatch(0, 0, 0.0, next_due)

    due = math.floor((now - next_due) / interval_seconds) + 1
    steps = min(due, max_catchup_steps)
    skipped = due - steps
    return TickBatch(
        steps=steps,
        skipped=skipped,
        lag_seconds=max(0.0, now - next_due),
        next_due=next_due + due * interval_seconds,
    )


ClockAction = Literal["none", "reanchor", "hold"]


@dataclass(frozen=True)
class ClockCheck:
    """How far the tick clock has moved from the wall clock, and what to do.

    `drift_seconds` is wall time minus tick time. It is positive when the wall
    clock is ahead, which happens when the machine sleeps: the monotonic clock
    the scheduler counts with stands still, but the wall clock does not.
    """

    drift_seconds: float
    skipped_ticks: int
    action: ClockAction


def reconcile_clock(
    tick_clock_utc: datetime,
    wall_utc: datetime,
    tolerance_seconds: float,
    interval_seconds: float,
) -> ClockCheck:
    """Decide how ticks should follow a wall clock that has jumped.

    Forward by more than the tolerance: the model did not run for that time.
    Each whole interval is a skipped tick, recorded as a gap and never replayed
    (ADR 0022), and later ticks carry the real time again.

    Backward by more than the tolerance: tick times must never go back, or the
    model would refuse the step. Ticks hold until the wall clock passes the
    last tick.

    Within the tolerance: ordinary jitter between the two clocks. Nothing
    changes.
    """
    if tolerance_seconds <= 0 or interval_seconds <= 0:
        raise ValueError("Clock tolerance and interval must be positive")
    drift = (wall_utc - tick_clock_utc).total_seconds()
    if drift > tolerance_seconds:
        return ClockCheck(drift, math.floor(drift / interval_seconds), "reanchor")
    if drift < -tolerance_seconds:
        return ClockCheck(drift, 0, "hold")
    return ClockCheck(drift, 0, "none")
