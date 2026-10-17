"""Tick time must follow the wall clock when it jumps (ADR 0020 amendment).

On 2026-10-03 the host slept for about 9 hours 15 minutes. The monotonic clock
that schedules ticks stood still, so every tick after waking carried a time
33,291 seconds in the past. The gateway rejected every snapshot as stale while
twin-core reported ready. These tests replay that with fake clocks.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import pytest
from twin_core.config import NoDatabaseOverrides, ServiceSettings, load_defaults
from twin_core.database import HistorianReader
from twin_core.metrics import TwinMetrics
from twin_core.runtime import (
    FleetHealth,
    TwinRuntime,
    fleet_model_status,
    readiness_reason,
)
from twin_core.scheduler import reconcile_clock
from twin_core.state import ModelRegistry

START = datetime(2026, 10, 3, 3, 22, tzinfo=timezone.utc)
SLEEP_SECONDS = 33_291


class FakeClock:
    """A monotonic clock and a wall clock that a test moves by hand."""

    def __init__(self) -> None:
        """Start both clocks at fixed values."""
        self.mono = 1_000.0
        self.wall = START

    def advance(self, seconds: float) -> None:
        """Let time pass normally: both clocks move together."""
        self.mono += seconds
        self.wall += timedelta(seconds=seconds)

    def monotonic(self) -> float:
        """Read the monotonic clock."""
        return self.mono

    def now(self) -> datetime:
        """Read the wall clock."""
        return self.wall


class FleetHistorian(HistorianReader):
    """Report a fixed fleet without opening a database."""

    def __init__(self, wellhead_ids: Sequence[int]) -> None:
        """Store the fleet the runtime will load."""
        self._ids = list(wellhead_ids)

    def active_wellhead_ids(self) -> list[int]:
        """Return the fixed fleet."""
        return list(self._ids)


def make_runtime(clock: FakeClock) -> TwinRuntime:
    """Build a runtime on the real model, driven by the fake clocks."""
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=8000,
        max_catchup_steps=5,
        fleet_refresh_seconds=3_600,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password="unused",  # noqa: S106
        environment_overrides={},
    )
    defaults = load_defaults()
    runtime = TwinRuntime(
        settings,
        defaults,
        ModelRegistry(defaults, {}, NoDatabaseOverrides()),
        FleetHistorian([1, 2]),
        TwinMetrics(),
        monotonic=clock.monotonic,
        wall=clock.now,
    )
    runtime.start_clock()
    return runtime


def run_seconds(runtime: TwinRuntime, clock: FakeClock, seconds: int) -> None:
    """Let `seconds` of normal time pass, one scheduler pass per second."""
    for _ in range(seconds):
        clock.advance(1)
        runtime.cycle()


class TestReconcileClock:
    def test_jitter_within_tolerance_changes_nothing(self) -> None:
        check = reconcile_clock(START, START + timedelta(seconds=1.5), 2.0, 1.0)
        assert check.action == "none"
        assert check.skipped_ticks == 0

    def test_forward_jump_becomes_skipped_ticks(self) -> None:
        wall = START + timedelta(seconds=SLEEP_SECONDS)
        check = reconcile_clock(START, wall, 2.0, 1.0)
        assert check.action == "reanchor"
        assert check.skipped_ticks == SLEEP_SECONDS

    def test_backward_jump_holds(self) -> None:
        check = reconcile_clock(START, START - timedelta(seconds=100), 2.0, 1.0)
        assert check.action == "hold"
        assert check.skipped_ticks == 0

    def test_rejects_a_non_positive_tolerance(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            reconcile_clock(START, START, 0.0, 1.0)


class TestMachineSleeps:
    def test_ticks_carry_real_time_after_waking_and_the_gap_is_recorded(
        self,
    ) -> None:
        clock = FakeClock()
        runtime = make_runtime(clock)
        run_seconds(runtime, clock, 3)
        assert runtime.readiness()[0]
        before = runtime.state(1)
        assert before is not None

        # The machine sleeps: the monotonic clock stands still, the wall clock
        # does not. Then one second of normal running after waking.
        clock.wall += timedelta(seconds=SLEEP_SECONDS)
        run_seconds(runtime, clock, 1)

        after = runtime.state(1)
        assert after is not None
        assert after.process is not None
        lag = (clock.wall - after.process.simulated_at).total_seconds()
        assert 0 <= lag <= 1
        assert after.skipped_ticks == before.skipped_ticks + SLEEP_SECONDS
        # One ordinary step, never one huge step through the missing time.
        assert after.tick_index == before.tick_index + 1
        adjustments = runtime.metrics.clock_adjustments.labels(direction="forward")
        assert adjustments._value.get() == 1
        ready, payload = runtime.readiness()
        assert ready, payload

    def test_readiness_refuses_stale_tick_times(self) -> None:
        clock = FakeClock()
        runtime = make_runtime(clock)
        run_seconds(runtime, clock, 3)

        # Just after waking, before the next pass has realigned anything.
        clock.wall += timedelta(seconds=SLEEP_SECONDS)

        ready, payload = runtime.readiness()
        assert not ready
        assert payload["reason"] == "tick_clock_drift"
        assert payload["modelStatus"] == "lagging"


class TestWallClockMovesBack:
    def test_ticks_hold_until_the_wall_clock_passes_the_last_tick(self) -> None:
        clock = FakeClock()
        runtime = make_runtime(clock)
        run_seconds(runtime, clock, 3)
        before = runtime.state(1)
        assert before is not None
        assert before.process is not None

        clock.wall -= timedelta(seconds=100)
        run_seconds(runtime, clock, 1)

        held = runtime.state(1)
        assert held is not None
        assert held.tick_index == before.tick_index
        ready, payload = runtime.readiness()
        assert not ready
        assert payload["reason"] == "wall_clock_moved_back"
        backward = runtime.metrics.clock_adjustments.labels(direction="backward")
        assert backward._value.get() == 1

        # Real time passes until the wall clock is beyond the last tick again.
        run_seconds(runtime, clock, 101)

        resumed = runtime.state(1)
        assert resumed is not None
        assert resumed.process is not None
        assert resumed.tick_index > before.tick_index
        assert resumed.process.simulated_at > before.process.simulated_at
        # Holding is waiting, not missed time: no gap is recorded for it.
        assert resumed.skipped_ticks == before.skipped_ticks
        assert runtime.readiness()[0]


class TestReadinessReason:
    HEALTHY = FleetHealth(
        fleet_ok=True,
        count=2,
        invalid=0,
        running=2,
        tick_current=True,
        clock_ok=True,
        holding=False,
    )

    def test_healthy_fleet(self) -> None:
        assert readiness_reason(self.HEALTHY) == "service_ready_model_running"
        assert fleet_model_status(self.HEALTHY) == "running"

    def test_a_held_clock_outranks_drift(self) -> None:
        health = FleetHealth(
            **{**vars(self.HEALTHY), "holding": True, "clock_ok": False}
        )
        assert readiness_reason(health) == "wall_clock_moved_back"
