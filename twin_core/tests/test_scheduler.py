"""Tests for bounded fixed-step scheduling and visible gaps."""

from __future__ import annotations

import pytest
from twin_core.scheduler import plan_ticks


def test_no_tick_before_deadline() -> None:
    batch = plan_ticks(9.9, 10.0, 1.0, 5)
    assert batch.steps == 0
    assert batch.next_due == 10.0


def test_one_tick_at_deadline() -> None:
    batch = plan_ticks(10.0, 10.0, 1.0, 5)
    assert (batch.steps, batch.skipped, batch.next_due) == (1, 0, 11.0)


def test_catchup_is_bounded_and_excess_is_recorded() -> None:
    batch = plan_ticks(20.0, 10.0, 1.0, 5)
    assert batch.steps == 5
    assert batch.skipped == 6
    assert batch.lag_seconds == 10.0
    assert batch.next_due == 21.0


@pytest.mark.parametrize("interval,budget", [(0.0, 5), (1.0, 0)])
def test_invalid_schedule_is_rejected(interval: float, budget: int) -> None:
    with pytest.raises(ValueError):
        plan_ticks(10.0, 10.0, interval, budget)
