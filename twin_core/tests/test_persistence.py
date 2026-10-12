"""Recovery and bounded persistence behavior at the model boundary."""

from __future__ import annotations

import math
import threading
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock

import pytest
from twin_core.config import (
    NoDatabaseOverrides,
    ParameterOverride,
    ServiceSettings,
    load_defaults,
    resolve_parameters,
)
from twin_core.database import DatabaseUnavailableError, HistorianReader
from twin_core.metrics import TwinMetrics
from twin_core.model import (
    ControlInputs,
    ModelParameters,
    balance_pressure,
    initial_state,
    step_model,
)
from twin_core.persistence import (
    EVENT_CAPACITY,
    PersistenceWriter,
    RecoveryEvent,
    SnapshotCopy,
    StoredSnapshot,
    TwinPersistence,
)
from twin_core.recovery import (
    decode_process,
    parameter_fingerprint,
    restore_state,
)
from twin_core.runtime import TwinRuntime
from twin_core.state import ModelRegistry


def _settings() -> ServiceSettings:
    return ServiceSettings(
        bind_host="127.0.0.1",
        port=8000,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password="unused",  # noqa: S106
        environment_overrides={},
    )


def _parameters() -> ModelParameters:
    defaults = load_defaults()
    return ModelParameters.from_values(resolve_parameters(defaults, {}, {}, 1))


class OneWellHistorian(HistorianReader):
    def active_wellhead_ids(self) -> list[int]:
        return [1]


class MemoryPersistence(TwinPersistence):
    def __init__(self, snapshot: StoredSnapshot | None) -> None:
        """Keep one test snapshot without opening a database."""
        self.snapshot = snapshot

    def load_snapshots(self, wellhead_ids: Sequence[int]) -> dict[int, StoredSnapshot]:
        if not wellhead_ids:
            return {}
        assert wellhead_ids == [1]
        return {1: self.snapshot} if self.snapshot else {}


class MemoryWriter(PersistenceWriter):
    def __init__(self) -> None:
        """Collect submitted copies and events without a worker thread."""
        self.events: list[RecoveryEvent] = []
        self.snapshots: list[SnapshotCopy] = []

    def submit_event(self, event: RecoveryEvent) -> None:
        self.events.append(event)

    def submit_snapshot(self, copy: SnapshotCopy) -> None:
        self.snapshots.append(copy)

    def refresh_age(self, now: datetime) -> None:
        assert now.utcoffset() == timedelta(0)


def _runtime(
    snapshot: StoredSnapshot | None, now: datetime
) -> tuple[TwinRuntime, MemoryWriter]:
    defaults = load_defaults()
    writer = MemoryWriter()
    runtime = TwinRuntime(
        _settings(),
        defaults,
        ModelRegistry(defaults, {}, NoDatabaseOverrides()),
        OneWellHistorian(_settings()),
        TwinMetrics(),
        monotonic=lambda: 0.0,
        wall=lambda: now,
        persistence=MemoryPersistence(snapshot),
        writer=writer,
    )
    return runtime, writer


def _snapshot(at: datetime, *, fingerprint: str | None = None) -> StoredSnapshot:
    state = initial_state(1, _parameters(), ControlInputs(), at)
    return StoredSnapshot(
        1,
        at,
        load_defaults().version,
        fingerprint or parameter_fingerprint(_parameters()),
        50,
        2,
        state.to_payload(),
    )


def test_process_json_round_trip_and_rejects_invalid_values() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    state = initial_state(1, _parameters(), ControlInputs(), at)
    assert decode_process(state.to_payload()) == state
    payload = state.to_payload()
    payload["tubingPressurePsi"] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        decode_process(payload)


def test_short_gap_replays_one_second_steps() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    saved = initial_state(1, _parameters(), ControlInputs(), at)
    recovered = restore_state(
        saved, _parameters(), ControlInputs(), 3.0, at + timedelta(seconds=3)
    )
    expected = saved
    for second in (1, 2, 3):
        expected = step_model(
            expected, ControlInputs(), _parameters(), at + timedelta(seconds=second)
        )
    assert recovered == expected


def test_long_gap_settles_fast_state_and_preserves_slow_damage() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    saved = initial_state(1, _parameters(), ControlInputs(), at)
    saved = step_model(saved, ControlInputs(), _parameters(), at + timedelta(seconds=1))
    recovered = restore_state(
        saved, _parameters(), ControlInputs(), 3600.0, at + timedelta(seconds=3601)
    )
    assert recovered.simulated_at == at + timedelta(seconds=3601)
    assert recovered.water_cut_percent == saved.water_cut_percent
    assert recovered.corrosion_factor == saved.corrosion_factor
    assert recovered.blockage_factor == saved.blockage_factor
    assert recovered.flow_rate_bpd > 0
    assert recovered.annulus_pressure_psi > 0


def test_nine_hour_gap_has_settled_pressure_without_advancing_damage() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    parameters = _parameters()
    saved = initial_state(1, parameters, ControlInputs(), at)
    recovered = restore_state(
        saved,
        parameters,
        ControlInputs(),
        9 * 3600.0,
        at + timedelta(hours=9),
    )
    balance = balance_pressure(ControlInputs(), parameters, 1.0, saved.blockage_factor)
    assert abs(recovered.tubing_pressure_psi - balance) / balance < 0.01
    assert recovered.water_cut_percent == saved.water_cut_percent
    assert recovered.corrosion_factor == saved.corrosion_factor


def test_ten_minute_pressure_matches_settling_formula() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    parameters = _parameters()
    controls = ControlInputs(choke_position_percent=50.0)
    initial = initial_state(1, parameters, ControlInputs(), at)
    saved = step_model(initial, controls, parameters, at + timedelta(seconds=1))
    balance = balance_pressure(controls, parameters, 1.0, saved.blockage_factor)
    pressure_drop = balance - parameters.downstream_pressure
    slope = (
        parameters.choke_coefficient
        * (controls.choke_position_percent / 100.0)
        * saved.blockage_factor
        / (2.0 * math.sqrt(pressure_drop))
    )
    rate = parameters.pressure_gain_coefficient * (
        parameters.productivity_index + slope
    )
    expected = balance + (saved.tubing_pressure_psi - balance) * math.exp(-600 * rate)
    recovered = restore_state(
        saved, parameters, controls, 600, at + timedelta(seconds=601)
    )
    assert recovered.tubing_pressure_psi == pytest.approx(expected, abs=1e-9)


def test_runtime_restores_and_records_gap_before_first_tick() -> None:
    saved_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    now = saved_at + timedelta(hours=1)
    runtime, writer = _runtime(_snapshot(saved_at), now)
    assert runtime.refresh_fleet()
    state = runtime.state(1)
    assert state is not None
    assert state.status == "recovering"
    assert state.skipped_ticks == 3602
    assert state.restored_from == saved_at
    assert state.to_payload()["lastRecoveryEvent"] == {
        "type": "snapshot_restored",
        "time": now.isoformat(),
    }
    assert [event.event_type for event in writer.events] == ["snapshot_restored"]
    runtime.registry.tick(now + timedelta(seconds=1))
    assert runtime.state(1) is not None
    assert runtime.state(1).reason == "restored_from_snapshot"  # type: ignore[union-attr]


def test_runtime_rejects_changed_parameters_and_starts_cold() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runtime, writer = _runtime(
        _snapshot(now - timedelta(seconds=10), fingerprint="wrong"), now
    )
    assert runtime.refresh_fleet()
    assert runtime.state(1).status == "initializing"  # type: ignore[union-attr]
    assert writer.events[0].event_type == "snapshot_incompatible"
    assert writer.events[0].details == {"check": "parameter_fingerprint"}


def test_runtime_rejects_snapshot_from_another_model_version() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = replace(_snapshot(now - timedelta(seconds=2)), model_version="9.9.9")
    runtime, writer = _runtime(snapshot, now)
    assert runtime.refresh_fleet()
    assert writer.events[0].event_type == "snapshot_incompatible"
    assert writer.events[0].details == {"check": "model_version"}


def test_runtime_rejects_invalid_saved_state() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = _snapshot(now - timedelta(seconds=2))
    state = dict(cast(dict[str, object], snapshot.state))
    state["waterCutPercent"] = -5.0
    runtime, writer = _runtime(replace(snapshot, state=state), now)
    assert runtime.refresh_fleet()
    assert runtime.state(1).status == "initializing"  # type: ignore[union-attr]
    assert writer.events[0].event_type == "snapshot_incompatible"
    assert writer.events[0].details == {"check": "invalid_state"}


def test_snapshot_read_failure_prevents_false_cold_start() -> None:
    class UnavailablePersistence(MemoryPersistence):
        def load_snapshots(
            self, _wellhead_ids: Sequence[int]
        ) -> dict[int, StoredSnapshot]:
            raise DatabaseUnavailableError("offline")

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runtime, writer = _runtime(None, now)
    runtime.persistence = UnavailablePersistence(None)
    assert not runtime.refresh_fleet()
    assert runtime.state(1) is None
    assert writer.events == []


def test_removed_well_gets_new_recovery_decision_when_readded() -> None:
    class ChangingHistorian(OneWellHistorian):
        def __init__(self) -> None:
            """Let the active fleet change between refreshes."""
            self.ids = [1]

        def active_wellhead_ids(self) -> list[int]:
            return self.ids

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runtime, writer = _runtime(_snapshot(now - timedelta(seconds=2)), now)
    historian = ChangingHistorian()
    runtime.historian = historian
    assert runtime.refresh_fleet()
    historian.ids = []
    assert runtime.refresh_fleet()
    assert runtime.state(1) is None
    historian.ids = [1]
    assert runtime.refresh_fleet()
    assert runtime.state(1).status == "recovering"  # type: ignore[union-attr]
    assert [event.event_type for event in writer.events] == [
        "snapshot_restored",
        "snapshot_restored",
    ]


def test_future_snapshot_waits_for_wall_time_to_pass() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runtime, _ = _runtime(_snapshot(now + timedelta(seconds=10)), now)
    assert runtime.refresh_fleet()
    runtime.registry.tick(now + timedelta(seconds=1))
    assert runtime.state(1).status == "recovering"  # type: ignore[union-attr]
    runtime.registry.tick(now + timedelta(seconds=11))
    assert runtime.state(1).status == "running"  # type: ignore[union-attr]


def test_override_loader_ignores_latest_null_tombstone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistence = TwinPersistence(_settings())
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        (1, "downstream_pressure", None, None, "manual"),
        (1, "productivity_index", 1.5, "bbl/day/psi", "manual"),
    ]

    def connect(*, read_only: bool) -> MagicMock:
        assert read_only
        return connection

    monkeypatch.setattr(persistence, "_connect", connect)
    overrides = persistence.load_overrides([1], "0.3.0")
    assert set(overrides[1]) == {"productivity_index"}
    assert overrides[1]["productivity_index"].value == 1.5
    assert "valid_from <= now()" in cursor.execute.call_args.args[0]


def test_invalid_override_affects_only_its_well_and_change_needs_restart() -> None:
    class OneWellOverrideSource(NoDatabaseOverrides):
        def __init__(self) -> None:
            """Expose a changed value for only the first test well."""
            self.value: float | None = None

        def load(
            self, _wellhead_ids: Sequence[int], _model_version: str
        ) -> Mapping[int, Mapping[str, ParameterOverride]]:
            if self.value is None:
                return {}
            return {
                1: {
                    "downstream_pressure": ParameterOverride(
                        self.value, "psi", "manual"
                    )
                }
            }

    source = OneWellOverrideSource()
    registry = ModelRegistry(load_defaults(), {}, source)
    registry.replace_wellheads([1, 2])
    registry.tick(datetime(2026, 1, 1, tzinfo=timezone.utc))
    source.value = 50000.0
    registry.replace_wellheads([1, 2])
    assert registry.get(1).reason == "invalid_parameters"  # type: ignore[union-attr]
    assert registry.get(2).status == "running"  # type: ignore[union-attr]
    source.value = 260.0
    registry.replace_wellheads([1, 2])
    assert registry.get(1).reason == "restart_required"  # type: ignore[union-attr]
    assert registry.get(2).status == "running"  # type: ignore[union-attr]


def test_writer_replaces_pending_snapshot_without_waiting() -> None:
    class BlockedDatabase(TwinPersistence):
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.written: list[SnapshotCopy] = []

        def write_snapshots(self, copy: SnapshotCopy) -> int:
            self.started.set()
            assert self.release.wait(timeout=3)
            self.written.append(copy)
            return 0

        def write_event(self, event: RecoveryEvent) -> None:
            assert event.event_type == "snapshot_restored"

    database = BlockedDatabase()
    metrics = TwinMetrics()
    writer = PersistenceWriter(database, metrics)
    first = cast(SnapshotCopy, (("first", "a"),))
    second = cast(SnapshotCopy, (("second", "b"),))
    latest = cast(SnapshotCopy, (("latest", "c"),))
    writer.start()
    try:
        writer.submit_snapshot(first)
        assert database.started.wait(timeout=2)
        writer.submit_snapshot(second)
        writer.submit_snapshot(latest)
        assert metrics.snapshots_replaced._value.get() == 1
    finally:
        database.release.set()
        writer.stop(latest)
    assert first in database.written
    assert latest in database.written
    assert second not in database.written


def test_writer_counts_event_overflow_and_write_failure() -> None:
    class FailedDatabase(TwinPersistence):
        def __init__(self) -> None:
            """Avoid connecting to a real database in this failure test."""

        def write_snapshots(self, _copy: SnapshotCopy) -> int:
            raise DatabaseUnavailableError("offline")

        def write_event(self, _event: RecoveryEvent) -> None:
            return

    metrics = TwinMetrics()
    writer = PersistenceWriter(FailedDatabase(), metrics)
    at = datetime.now(timezone.utc)
    for _ in range(EVENT_CAPACITY + 1):
        writer.submit_event(RecoveryEvent(at, None, "clock_jump_forward", "0.3.0"))
    assert metrics.recovery_events_dropped._value.get() == 1
    writer.start()
    writer.submit_snapshot(cast(SnapshotCopy, (("failed", "a"),)))
    deadline = threading.Event()
    try:
        for _ in range(100):
            if metrics.snapshot_write_failures._value.get() == 1:
                break
            deadline.wait(0.01)
        assert metrics.snapshot_write_failures._value.get() == 1
    finally:
        writer.stop(())
