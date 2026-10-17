"""Historical branches preserve exact identity, saved controls and live state."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from twin_core.config import NoDatabaseOverrides, ServiceSettings, load_defaults
from twin_core.database import DatabaseUnavailableError, HistorianReader
from twin_core.historical import (
    MAX_SNAPSHOT_AGE,
    SnapshotCandidate,
    SnapshotConflictError,
    SnapshotMissingError,
    SnapshotSelection,
    historical_base,
)
from twin_core.metrics import TwinMetrics
from twin_core.model import ControlInputs, ModelParameters, initial_state
from twin_core.persistence import TwinPersistence
from twin_core.projection import (
    ControlChange,
    ProjectionBase,
    ScheduledControl,
    project,
)
from twin_core.recovery import parameter_fingerprint
from twin_core.runtime import ProjectionBusyError, TwinRuntime
from twin_core.state import ModelRegistry

NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
AT = NOW - timedelta(days=2)
IDENTIFIER = "9007199254740993"
SELECTION = SnapshotSelection(IDENTIFIER, AT)


@pytest.fixture
def base() -> ProjectionBase:
    defaults = load_defaults()
    parameters = ModelParameters.from_values(
        {key: spec.default for key, spec in defaults.parameters.items()}
    )
    controls = ControlInputs(choke_position_percent=42)
    return ProjectionBase(
        1,
        defaults.version,
        parameter_fingerprint(parameters),
        initial_state(1, parameters, controls, AT),
        controls,
        parameters,
        None,
        None,
    )


def candidate(base: ProjectionBase) -> SnapshotCandidate:
    return SnapshotCandidate(SELECTION, base.model_version, base.parameter_fingerprint)


@pytest.mark.parametrize(
    "identifier", [1, 9007199254740993, "0", "01", "1e3", "x", "9223372036854775808"]
)
def test_identity_rejects_rounded_or_invalid_ids(identifier: object) -> None:
    with pytest.raises(ValueError):
        SnapshotSelection.from_payload(
            {"snapshotId": identifier, "simulatedAt": AT.isoformat()}
        )


def test_identity_preserves_bigint_and_requires_exact_utc_fields() -> None:
    assert SnapshotSelection.from_payload(SELECTION.to_payload()) == SELECTION
    for value in [
        {**SELECTION.to_payload(), "extra": True},
        {"snapshotId": IDENTIFIER},
        {**SELECTION.to_payload(), "simulatedAt": "2026-10-15T01:00:00+01:00"},
    ]:
        with pytest.raises(ValueError):
            SnapshotSelection.from_payload(value)


def test_historical_projection_keeps_snapshot_clock_and_controls(
    base: ProjectionBase,
) -> None:
    payload = base.state.to_payload()
    saved = historical_base(
        1, candidate(base), payload, base.parameters, base.model_version, NOW
    )
    assert saved.controls.choke_position_percent == 42
    assert saved.state == base.state
    result = project(
        "scenario",
        "test",
        saved,
        NOW,
        300,
        60,
        (),
        name="Close choke",
        controls=(ScheduledControl(0, ControlChange(choke_position_percent=0)),),
    )
    assert result.points[-1].at == AT + timedelta(seconds=300)
    assert result.generated_at == NOW
    assert result.points[-1].values["flow_rate"] == 0
    assert result.points[-1].baseline_values["flow_rate"] > 0  # type: ignore[index]
    assert saved.state == base.state and payload == base.state.to_payload()
    assert result.to_payload()["branchOrigin"] == {
        "kind": "snapshot",
        **SELECTION.to_payload(),
    }
    assert "historical_snapshot" in result.confidence_reasons
    with pytest.raises(ValueError, match="current state"):
        project("forecast", "test", saved, NOW, 300, 60, ())


@pytest.mark.parametrize(
    "change",
    ["version", "parameters", "timestamp", "numeric", "bounds", "future", "old"],
)
def test_incompatible_state_never_becomes_a_branch(
    base: ProjectionBase, change: str
) -> None:
    item = candidate(base)
    payload = base.state.to_payload()
    if change == "version":
        item = replace(item, model_version="0.0.1")
    elif change == "parameters":
        item = replace(item, parameter_fingerprint="different")
    elif change == "timestamp":
        payload["simulatedAt"] = NOW.isoformat()
    elif change == "numeric":
        payload["tubingPressurePsi"] = float("nan")
    elif change == "bounds":
        payload["chokePositionPercent"] = 101
    else:
        at = (
            NOW + timedelta(seconds=1)
            if change == "future"
            else NOW - MAX_SNAPSHOT_AGE - timedelta(seconds=1)
        )
        item = replace(item, selection=SnapshotSelection(IDENTIFIER, at))
    with pytest.raises(SnapshotConflictError):
        historical_base(1, item, payload, base.parameters, base.model_version, NOW)


def settings() -> ServiceSettings:
    return ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host="unused",
        database_port=5432,
        database_name="unused",
        database_user="unused",
        database_password=os.getenv("TEST_DB_PASSWORD", "unused"),
        environment_overrides={},
    )


def test_exact_database_lookup_and_bounded_finder(
    base: ProjectionBase, monkeypatch: pytest.MonkeyPatch
) -> None:
    persistence = TwinPersistence(settings())
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    monkeypatch.setattr(persistence, "_connect", lambda **_: connection)
    cursor.fetchone.return_value = (
        base.model_version,
        base.parameter_fingerprint,
        base.state.to_payload(),
    )
    result = persistence.selected_snapshot(7, SELECTION)
    assert result is not None and result[0].selection.snapshot_id == IDENTIFIER
    assert cursor.execute.call_args.args[1] == (7, int(IDENTIFIER), AT)
    cursor.fetchall.return_value = [
        (int(IDENTIFIER), AT, base.model_version, base.parameter_fingerprint)
    ]
    assert persistence.find_snapshots(7, AT - timedelta(days=1), AT)[0] == candidate(
        base
    )
    assert cursor.execute.call_args.args[1][-1] == 21


def test_runtime_historical_branch_works_without_fresh_tick_and_releases_capacity(
    base: ProjectionBase,
) -> None:
    defaults = load_defaults()
    registry = ModelRegistry(defaults, {}, NoDatabaseOverrides())
    registry.replace_wellheads([1])
    registry.tick(NOW)
    parameters = registry.parameters_for(1)
    assert parameters is not None
    base = replace(
        base,
        parameters=parameters,
        parameter_fingerprint=parameter_fingerprint(parameters),
        state=initial_state(1, parameters, base.controls, AT),
    )
    persistence = MagicMock(spec=TwinPersistence)
    persistence.selected_snapshot.return_value = (
        candidate(base),
        base.state.to_payload(),
    )
    persistence.load_alarm_rules.return_value = ()
    runtime = TwinRuntime(
        settings(),
        defaults,
        registry,
        MagicMock(spec=HistorianReader),
        TwinMetrics(),
        persistence=persistence,
        wall=lambda: NOW + timedelta(hours=1),
    )
    before = registry.projection_base(1)
    result = runtime.create_projection(
        "scenario",
        1,
        2,
        300,
        60,
        name="Historical",
        controls=(ScheduledControl(0, ControlChange(pump_on=False)),),
        base_snapshot=SELECTION,
    )
    assert registry.projection_base(1) == before
    assert result.base.controls.choke_position_percent == 42
    persistence.write_projection.assert_called_once_with(result, 2)
    persistence.selected_snapshot.return_value = None
    for _ in range(3):
        with pytest.raises(SnapshotMissingError):
            runtime.create_projection(
                "scenario", 1, 2, 300, 60, base_snapshot=SELECTION
            )
    persistence.selected_snapshot.side_effect = DatabaseUnavailableError("offline")
    for _ in range(3):
        with pytest.raises(DatabaseUnavailableError):
            runtime.create_projection(
                "scenario", 1, 2, 300, 60, base_snapshot=SELECTION
            )
    assert runtime._projection_slots.acquire(blocking=False)
    assert runtime._projection_slots.acquire(blocking=False)
    with pytest.raises(ProjectionBusyError):
        runtime.create_projection("scenario", 1, 2, 300, 60, base_snapshot=SELECTION)
    runtime._projection_slots.release()
    runtime._projection_slots.release()


def test_saved_run_contains_origin_and_complete_base(
    base: ProjectionBase, monkeypatch: pytest.MonkeyPatch
) -> None:
    saved = historical_base(
        1,
        candidate(base),
        base.state.to_payload(),
        base.parameters,
        base.model_version,
        NOW,
    )
    result = project(
        "scenario",
        "test",
        saved,
        NOW,
        300,
        60,
        (),
        name="Stop",
        controls=(ScheduledControl(0, ControlChange(pump_on=False)),),
    )
    persistence = TwinPersistence(settings())
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    monkeypatch.setattr(persistence, "_connect", lambda **_: connection)
    monkeypatch.setattr("twin_core.persistence.execute_batch", lambda *_: None)
    persistence.write_projection(result, 1)
    values = cursor.execute.call_args.args[1]
    stored_base = values[10].adapted
    assert stored_base["branchOrigin"] == {"kind": "snapshot", **SELECTION.to_payload()}
    assert stored_base["simulatedAt"] == AT.isoformat()
    assert values[12].adapted["choke_position_percent"] == 42
    connection.commit.assert_called_once()
