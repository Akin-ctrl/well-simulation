from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from twin_core.config import NoDatabaseOverrides, load_defaults
from twin_core.control_store import (
    ActiveControls,
    CommandOutcome,
    ControlCommitUnknownError,
    ControlContext,
    QueuedCommand,
    command_rejection,
    merge_live_controls,
)
from twin_core.database import DatabaseUnavailableError
from twin_core.model import ControlInputs
from twin_core.projection import ControlChange
from twin_core.state import ModelRegistry

NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


@pytest.fixture
def registry() -> ModelRegistry:
    registry = ModelRegistry(load_defaults(), {}, NoDatabaseOverrides())
    registry.replace_wellheads([1])
    registry.tick(NOW)
    return registry


def test_shutdown_requires_a_separate_reset() -> None:
    active = merge_live_controls(
        ControlInputs(), ControlChange(emergency_shutdown=True)
    )
    with pytest.raises(ValueError, match="Reset"):
        merge_live_controls(active, ControlChange(pump_on=True))
    with pytest.raises(ValueError, match="separate"):
        merge_live_controls(
            active, ControlChange(emergency_shutdown=False, pump_on=True)
        )
    reset = merge_live_controls(active, ControlChange(emergency_shutdown=False))
    assert not reset.emergency_shutdown
    assert reset.swab_valve_open == active.swab_valve_open


@pytest.mark.parametrize("choke", [-1, 101, float("inf"), float("nan")])
def test_live_choke_reuses_model_bounds(choke: float) -> None:
    with pytest.raises(ValueError):
        merge_live_controls(
            ControlInputs(), ControlChange(choke_position_percent=choke)
        )


@pytest.mark.parametrize(
    "authority",
    [
        None,
        ("USER", "ACTIVE", False, 2),
        ("OPERATIONS", "DISABLED", False, 2),
        ("OPERATIONS", "ACTIVE", True, 2),
        ("OPERATIONS", "ACTIVE", False, 3),
    ],
)
def test_revoked_queued_authority_is_rejected(
    authority: tuple[str, str, bool, int] | None,
) -> None:
    controls = ControlInputs()
    context = ControlContext(1, "0.3.0", "a" * 64, "running", controls, 0)
    active = ActiveControls(0, "0.3.0", "a" * 64, controls)
    command = QueuedCommand(
        "queued",
        NOW + timedelta(seconds=30),
        0,
        "0.3.0",
        "a" * 64,
        "OPERATIONS",
        2,
        ControlChange(pump_on=False),
    )
    assert (
        command_rejection(command, context, active, authority, NOW)
        == "authority_revoked"
    )


def test_command_changes_inputs_before_the_next_process_step(
    registry: ModelRegistry,
) -> None:
    context = registry.control_context(1)
    assert context is not None
    target = replace(context.controls, master_valve_open=False)
    active = ActiveControls(
        1, context.model_version, context.parameter_fingerprint, target
    )
    old = registry.get(1)

    def apply() -> None:
        registry.apply_control(
            1, lambda _: CommandOutcome("applied", "applied", active)
        )

    registry.tick(NOW + timedelta(seconds=1), apply)
    state = registry.get(1)
    assert state is not None and state.process is not None
    assert not state.process.master_valve_open
    assert state.process.flow_rate_bpd == 0
    assert old is not None and old.process is not None and old.process.master_valve_open


def test_known_failure_keeps_controls_and_unknown_commit_holds_the_well(
    registry: ModelRegistry,
) -> None:
    initial = registry.control_context(1)

    def failure(_: ControlContext | None) -> CommandOutcome:
        raise DatabaseUnavailableError("rollback confirmed")

    with pytest.raises(DatabaseUnavailableError):
        registry.apply_control(1, failure)
    assert registry.control_context(1) == initial

    def unknown(_: ControlContext | None) -> CommandOutcome:
        raise ControlCommitUnknownError("reply lost")

    with pytest.raises(ControlCommitUnknownError):
        registry.apply_control(1, unknown)
    held = registry.get(1)
    assert held is not None and held.status == "unavailable"
    registry.tick(NOW + timedelta(seconds=1))
    current = registry.get(1)
    assert current is not None and current.process == held.process
    assert initial is not None
    registry.install_controls(
        1,
        ActiveControls(
            1,
            initial.model_version,
            initial.parameter_fingerprint,
            replace(initial.controls, pump_on=False),
        ),
    )
    registry.tick(NOW + timedelta(seconds=2))
    recovered = registry.get(1)
    assert recovered is not None and recovered.status == "running"
    assert recovered.process is not None and not recovered.process.pump_on


def test_no_new_well_ticks_before_active_controls_load() -> None:
    registry = ModelRegistry(load_defaults(), {}, NoDatabaseOverrides())
    registry.require_durable_controls()
    registry.replace_wellheads([1])
    assert registry.tick(NOW) == (0, 0)
    context = registry.control_context(1)
    assert context is not None
    registry.install_controls(
        1,
        ActiveControls(
            0, context.model_version, context.parameter_fingerprint, context.controls
        ),
    )
    assert registry.tick(NOW + timedelta(seconds=1)) == (1, 0)
