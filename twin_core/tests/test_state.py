"""Tests for honest lifecycle state and rejected per-well overrides."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

import pytest
from twin_core.config import ParameterOverride, load_defaults
from twin_core.state import ModelRegistry


class FixedOverrides:
    """Return one valid and one invalid database-style override."""

    def load(
        self, _wellhead_ids: Sequence[int], _model_version: str
    ) -> Mapping[int, Mapping[str, ParameterOverride]]:
        return {
            1: {"downstream_pressure": ParameterOverride(300, "psi", "database")},
            2: {"downstream_pressure": ParameterOverride(50000, "psi", "database")},
        }


def test_state_initializes_only_after_first_model_tick() -> None:
    registry = ModelRegistry(load_defaults(), {}, FixedOverrides())
    registry.replace_wellheads([1, 2])
    state = registry.get(1)
    assert state is not None
    assert state.to_payload()["process"] is None
    assert state.reason == "model_not_started"

    tick_time = datetime(2026, 9, 29, tzinfo=timezone.utc)
    assert registry.tick(tick_time) == (1, 0)
    updated = registry.get(1)
    assert updated is not None
    assert updated.tick_index == 1
    assert updated.last_tick_at == tick_time
    assert updated.status == "running"
    assert updated.reason == "cold_start_no_snapshot"
    assert updated.process is not None
    assert updated.process.flow_rate_bpd > 0


def test_invalid_well_override_does_not_fall_back_to_default() -> None:
    registry = ModelRegistry(load_defaults(), {}, FixedOverrides())
    registry.replace_wellheads([1, 2])
    state = registry.get(2)
    assert state is not None
    assert state.status == "unavailable"
    assert state.reason == "invalid_parameters"
    assert registry.invalid_count() == 1


def test_fleet_refresh_preserves_state_and_gap_count() -> None:
    """Metadata refresh must not reset a running synthetic process."""
    from twin_core.config import NoDatabaseOverrides

    registry = ModelRegistry(load_defaults(), {}, NoDatabaseOverrides())
    registry.replace_wellheads([1])
    start = datetime(2026, 9, 29, tzinfo=timezone.utc)
    assert registry.tick(start) == (1, 0)
    first = registry.get(1)
    assert first is not None
    registry.record_gap(6)
    registry.replace_wellheads([1])
    retained = registry.get(1)
    assert retained is not None
    assert retained.process == first.process
    assert retained.skipped_ticks == 6
    assert retained.status == "running"


def test_changed_parameters_require_restart() -> None:
    """A future database override must not change running physics live."""
    from twin_core.config import NoDatabaseOverrides

    class MutableOverrides(NoDatabaseOverrides):
        """Change the future database source between fleet refreshes."""

        def __init__(self) -> None:
            self.value = 300.0

        def load(
            self, _wellhead_ids: Sequence[int], _model_version: str
        ) -> Mapping[int, Mapping[str, ParameterOverride]]:
            return {
                1: {
                    "downstream_pressure": ParameterOverride(
                        self.value, "psi", "database"
                    )
                }
            }

    source = MutableOverrides()
    registry = ModelRegistry(load_defaults(), {}, source)
    registry.replace_wellheads([1])
    start = datetime(2026, 9, 29, tzinfo=timezone.utc)
    assert registry.tick(start) == (1, 0)
    original = registry.get(1)
    assert original is not None
    source.value = 350.0
    registry.replace_wellheads([1])
    stopped = registry.get(1)
    assert stopped is not None
    assert stopped.status == "unavailable"
    assert stopped.reason == "restart_required"
    assert stopped.process == original.process
    assert registry.tick(start.replace(second=1)) == (0, 0)
    registry.replace_wellheads([1])
    still_stopped = registry.get(1)
    assert still_stopped is not None
    assert still_stopped.reason == "restart_required"


def test_failed_step_preserves_last_valid_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A calculation error must surface degraded state and keep prior output."""
    import twin_core.state as state_module
    from twin_core.config import NoDatabaseOverrides

    registry = ModelRegistry(load_defaults(), {}, NoDatabaseOverrides())
    registry.replace_wellheads([1])
    start = datetime(2026, 9, 29, tzinfo=timezone.utc)
    assert registry.tick(start) == (1, 0)
    prior = registry.get(1)
    assert prior is not None

    def fail_step(*_args: object, **_kwargs: object) -> None:
        raise ValueError("deliberate calculation fault")

    monkeypatch.setattr(state_module, "step_model", fail_step)
    assert registry.tick(start.replace(second=1)) == (0, 1)
    degraded = registry.get(1)
    assert degraded is not None
    assert degraded.status == "degraded"
    assert degraded.reason == "model_step_failed"
    assert degraded.process == prior.process


def test_correcting_initial_parameters_starts_well_but_later_change_needs_restart() -> (
    None
):
    """Recover before first run, then protect established process state."""

    class CorrectableOverrides:
        """Present invalid, valid, then invalid values on fleet refresh."""

        def __init__(self) -> None:
            self.value = 50000.0

        def load(
            self, _wellhead_ids: Sequence[int], _model_version: str
        ) -> Mapping[int, Mapping[str, ParameterOverride]]:
            return {
                1: {
                    "downstream_pressure": ParameterOverride(
                        self.value, "psi", "database"
                    )
                }
            }

    source = CorrectableOverrides()
    registry = ModelRegistry(load_defaults(), {}, source)
    registry.replace_wellheads([1])
    invalid = registry.get(1)
    assert invalid is not None and invalid.reason == "invalid_parameters"

    source.value = 300.0
    registry.replace_wellheads([1])
    corrected = registry.get(1)
    assert corrected is not None and corrected.status == "initializing"
    start = datetime(2026, 9, 29, tzinfo=timezone.utc)
    assert registry.tick(start) == (1, 0)
    running = registry.get(1)
    assert running is not None and running.status == "running"

    source.value = 50000.0
    registry.replace_wellheads([1])
    source.value = 300.0
    registry.replace_wellheads([1])
    stopped = registry.get(1)
    assert stopped is not None and stopped.reason == "restart_required"
    assert stopped.process == running.process
