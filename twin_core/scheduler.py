"""Plan bounded fixed-step ticks without relying on wall-clock jumps."""

from __future__ import annotations

import math
from dataclasses import dataclass


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
