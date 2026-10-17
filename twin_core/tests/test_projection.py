"""Forecast and scenario behavior using the real one-second model."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest
from twin_core.config import load_defaults
from twin_core.model import ControlInputs, ModelParameters, initial_state
from twin_core.projection import (
    MODEL_SIGNALS,
    AlarmRule,
    ControlChange,
    ProjectionBase,
    ScheduledControl,
    project,
    signal_values,
    validate_window,
)
from twin_core.recovery import parameter_fingerprint

from model_source import MODEL_SIGNAL_FIELDS, _parameters


@pytest.fixture
def base() -> ProjectionBase:
    defaults = load_defaults()
    parameters = ModelParameters.from_values(
        {key: spec.default for key, spec in defaults.parameters.items()}
    )
    controls = ControlInputs()
    state = initial_state(1, parameters, controls, datetime.now(timezone.utc))
    return ProjectionBase(
        1,
        defaults.version,
        parameter_fingerprint(parameters),
        state,
        controls,
        parameters,
        None,
        None,
    )


def test_alarm_signal_values_match_the_gateway_map(base: ProjectionBase) -> None:
    """Project the same values and rounding that the historian receives."""
    assert set(MODEL_SIGNALS) == set(MODEL_SIGNAL_FIELDS)
    state = replace(base.state, choke_position_percent=37.6)
    gateway_values = _parameters(state.to_payload())
    assert signal_values(state) == {
        code: float(value) for code, value in gateway_values.items()
    }


def test_forecast_uses_existing_model_without_changing_its_base(
    base: ProjectionBase,
) -> None:
    before = base.state
    rules = (AlarmRule(7, "flow_rate", "WARNING", ">", 0.0),)

    result = project(
        "forecast", "run-1", base, datetime.now(timezone.utc), 300, 60, rules
    )

    assert base.state == before
    assert len(result.points) == 5
    assert [point.elapsed_seconds for point in result.points] == [
        60,
        120,
        180,
        240,
        300,
    ]
    assert all(point.alarm_risk == "warning" for point in result.points)
    assert all(point.triggered_rule_ids == (7,) for point in result.points)
    assert result.points[-1].at > base.state.simulated_at
    assert result.confidence_score < 0.65
    assert "synthetic_uncalibrated" in result.confidence_reasons


def test_scenario_has_matching_baseline_and_isolated_choke_effect(
    base: ProjectionBase,
) -> None:
    controls = (ScheduledControl(0, ControlChange(choke_position_percent=0.0)),)

    result = project(
        "scenario",
        "run-2",
        base,
        datetime.now(timezone.utc),
        300,
        60,
        (),
        name="Close choke",
        controls=controls,
    )

    last = result.points[-1]
    assert last.baseline_values is not None
    assert last.values["flow_rate"] == 0.0
    assert last.baseline_values["flow_rate"] > 0.0
    assert last.values["tubing_pressure"] > last.baseline_values["tubing_pressure"]
    assert last.alarm_risk == "unknown"
    assert "sharp_control_change" in result.confidence_reasons
    assert base.state.choke_position_percent == 100.0


def test_scheduled_change_starts_after_its_offset(base: ProjectionBase) -> None:
    controls = (ScheduledControl(60, ControlChange(emergency_shutdown=True)),)

    result = project(
        "scenario",
        "run-3",
        base,
        datetime.now(timezone.utc),
        300,
        60,
        (),
        name="Stop after one minute",
        controls=controls,
    )

    assert result.points[0].values == result.points[0].baseline_values
    assert result.points[1].values["flow_rate"] == 0.0
    assert result.points[1].baseline_values is not None
    assert result.points[1].baseline_values["flow_rate"] > 0.0


@pytest.mark.parametrize(
    ("horizon", "step"),
    [(True, 60), (299, 60), (7201, 60), (300, 4), (301, 60)],
)
def test_window_rejects_unbounded_or_partial_samples(
    horizon: object, step: object
) -> None:
    with pytest.raises(ValueError):
        validate_window(horizon, step)


def test_scenario_rejects_duplicate_control_offsets(base: ProjectionBase) -> None:
    steps = (
        ScheduledControl(0, ControlChange(pump_on=False)),
        ScheduledControl(0, ControlChange(emergency_shutdown=True)),
    )
    with pytest.raises(ValueError, match="offsets"):
        project(
            "scenario",
            "run-4",
            base,
            datetime.now(timezone.utc),
            300,
            60,
            (),
            name="Bad schedule",
            controls=steps,
        )
