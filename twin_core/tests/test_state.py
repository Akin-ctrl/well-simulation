"""Tests for honest lifecycle state and rejected per-well overrides."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

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


def test_state_has_no_fake_process_value() -> None:
    registry = ModelRegistry(load_defaults(), {}, FixedOverrides())
    registry.replace_wellheads([1, 2])
    state = registry.get(1)
    assert state is not None
    assert state.to_payload()["process"] is None
    assert state.reason == "model_not_implemented"

    tick_time = datetime(2026, 9, 29, tzinfo=timezone.utc)
    registry.tick(tick_time)
    updated = registry.get(1)
    assert updated is not None
    assert updated.tick_index == 1
    assert updated.last_tick_at == tick_time
    assert updated.process is None


def test_invalid_well_override_does_not_fall_back_to_default() -> None:
    registry = ModelRegistry(load_defaults(), {}, FixedOverrides())
    registry.replace_wellheads([1, 2])
    state = registry.get(2)
    assert state is not None
    assert state.status == "unavailable"
    assert state.reason == "invalid_parameters"
    assert registry.invalid_count() == 1
