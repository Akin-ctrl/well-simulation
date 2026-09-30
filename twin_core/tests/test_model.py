"""Behavior tests for the pure reduced-order wellhead model."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from twin_core.config import load_defaults, resolve_parameters
from twin_core.model import (
    ControlInputs,
    ModelParameters,
    initial_state,
    step_model,
)

START = datetime(2026, 9, 29, tzinfo=timezone.utc)


def _parameters(wellhead_id: int = 1) -> ModelParameters:
    """Use the versioned defaults with stable per-well variation."""
    return ModelParameters.from_values(
        resolve_parameters(load_defaults(), {}, {}, wellhead_id)
    )


def _start() -> tuple[ModelParameters, ControlInputs]:
    """Return one shared baseline for causal comparisons."""
    return _parameters(), ControlInputs()


def test_initial_balance_is_stable_and_well_variation_is_repeatable() -> None:
    parameters, controls = _start()
    initial = initial_state(1, parameters, controls, START)
    repeat = initial_state(1, parameters, controls, START)
    assert initial == repeat
    assert 5 <= initial.water_cut_percent <= 35
    assert 0.85 <= initial.blockage_factor <= 1
    assert 1 <= initial.corrosion_factor <= 1.01
    inflow = parameters.productivity_index * (
        parameters.reservoir_pressure_proxy - initial.tubing_pressure_psi
    )
    assert abs(inflow - initial.flow_rate_bpd) < 0.01
    assert (
        len(
            {
                _parameters(wellhead_id).reservoir_pressure_proxy
                for wellhead_id in range(1, 13)
            }
        )
        == 12
    )


def test_closing_choke_reduces_flow_and_raises_upstream_pressure() -> None:
    parameters, open_controls = _start()
    current = initial_state(1, parameters, open_controls, START)
    open_next = step_model(
        current, open_controls, parameters, START + timedelta(seconds=1)
    )
    closed_next = step_model(
        current,
        replace(open_controls, choke_position_percent=20),
        parameters,
        START + timedelta(seconds=1),
    )
    assert closed_next.flow_rate_bpd < open_next.flow_rate_bpd
    assert closed_next.tubing_pressure_psi > open_next.tubing_pressure_psi


def test_blockage_reduces_outflow_capacity() -> None:
    parameters, controls = _start()
    current = initial_state(1, parameters, controls, START)
    more_blocked = replace(current, blockage_factor=parameters.min_blockage_factor)
    normal = step_model(current, controls, parameters, START + timedelta(seconds=1))
    blocked = step_model(
        more_blocked, controls, parameters, START + timedelta(seconds=1)
    )
    assert blocked.flow_rate_bpd < normal.flow_rate_bpd


def test_pump_off_reduces_flow_over_time() -> None:
    parameters, controls = _start()
    initial = initial_state(1, parameters, controls, START)
    flowing = initial
    pump_off = initial
    off_controls = replace(controls, pump_on=False)
    for second in range(1, 121):
        at = START + timedelta(seconds=second)
        flowing = step_model(flowing, controls, parameters, at)
        pump_off = step_model(pump_off, off_controls, parameters, at)
    assert pump_off.pump_flow_factor < 1
    assert pump_off.pump_flow_factor > parameters.pump_off_flow_factor
    assert pump_off.flow_rate_bpd < flowing.flow_rate_bpd


@pytest.mark.parametrize(
    "controls",
    [
        ControlInputs(master_valve_open=False),
        ControlInputs(wing_valve_open=False),
        ControlInputs(emergency_shutdown=True),
        ControlInputs(choke_position_percent=0),
    ],
)
def test_closed_production_path_has_zero_outflow(controls: ControlInputs) -> None:
    parameters, normal = _start()
    current = initial_state(1, parameters, normal, START)
    next_state = step_model(current, controls, parameters, START + timedelta(seconds=1))
    assert next_state.flow_rate_bpd == 0
    assert next_state.tubing_pressure_psi > current.tubing_pressure_psi


def test_thermal_water_and_degradation_are_bounded_and_slow() -> None:
    parameters, controls = _start()
    current = initial_state(1, parameters, controls, START)
    cold = replace(current, wellhead_temperature_f=parameters.ambient_temperature)
    next_state = step_model(cold, controls, parameters, START + timedelta(seconds=1))
    assert cold.wellhead_temperature_f < next_state.wellhead_temperature_f
    assert next_state.wellhead_temperature_f < current.wellhead_temperature_f
    assert current.water_cut_percent < next_state.water_cut_percent
    assert current.corrosion_factor < next_state.corrosion_factor
    assert current.blockage_factor > next_state.blockage_factor
    near_bounds = replace(
        current,
        water_cut_percent=100,
        blockage_factor=parameters.min_blockage_factor,
        corrosion_factor=parameters.max_corrosion_factor,
    )
    bounded = step_model(
        near_bounds, controls, parameters, START + timedelta(seconds=1)
    )
    assert bounded.water_cut_percent == 100
    assert bounded.blockage_factor == parameters.min_blockage_factor
    assert bounded.corrosion_factor == parameters.max_corrosion_factor


def test_gap_does_not_create_a_large_time_step() -> None:
    parameters, controls = _start()
    current = initial_state(1, parameters, controls, START)
    one_second = step_model(current, controls, parameters, START + timedelta(seconds=1))
    later = step_model(current, controls, parameters, START + timedelta(seconds=20))
    assert later.tubing_pressure_psi == one_second.tubing_pressure_psi
    assert later.flow_rate_bpd == one_second.flow_rate_bpd
    assert later.simulated_at - current.simulated_at == timedelta(seconds=20)


@pytest.mark.parametrize("position", [-1, 101, float("nan"), True])
def test_invalid_choke_is_rejected(position: float) -> None:
    parameters, controls = _start()
    current = initial_state(1, parameters, controls, START)
    with pytest.raises(ValueError, match="Choke position"):
        step_model(
            current,
            replace(controls, choke_position_percent=position),
            parameters,
            START + timedelta(seconds=1),
        )


def test_invalid_state_does_not_produce_an_output() -> None:
    parameters, controls = _start()
    current = initial_state(1, parameters, controls, START)
    with pytest.raises(ValueError, match="not finite"):
        step_model(
            replace(current, tubing_pressure_psi=float("nan")),
            controls,
            parameters,
            START + timedelta(seconds=1),
        )


def test_initial_pump_off_state_uses_residual_flow_factor() -> None:
    parameters, controls = _start()
    off = initial_state(1, parameters, replace(controls, pump_on=False), START)
    assert off.pump_flow_factor == parameters.pump_off_flow_factor
    assert off.pump_on is False


def test_impossible_balance_is_reported_instead_of_fabricated() -> None:
    parameters, controls = _start()
    constrained = replace(
        parameters,
        max_tubing_pressure=1000,
        productivity_index=10,
        choke_coefficient=1,
    )
    with pytest.raises(ValueError, match="No operating balance"):
        initial_state(1, constrained, controls, START)


def test_demo_fleet_stays_finite_for_one_model_hour() -> None:
    """Run varied wells long enough to expose integration drift or overflow."""
    import math

    controls = ControlInputs()
    for wellhead_id in range(1, 13):
        parameters = _parameters(wellhead_id)
        current = initial_state(wellhead_id, parameters, controls, START)
        for second in range(1, 3601):
            current = step_model(
                current,
                controls,
                parameters,
                START + timedelta(seconds=second),
            )
        assert all(
            math.isfinite(value)
            for value in (
                current.tubing_pressure_psi,
                current.casing_pressure_psi,
                current.flow_rate_bpd,
                current.wellhead_temperature_f,
                current.water_cut_percent,
                current.blockage_factor,
                current.corrosion_factor,
                current.annulus_pressure_psi,
                current.annulus_temperature_f,
                current.oil_rate_bpd,
                current.water_rate_bpd,
                current.gas_rate_scfd,
                current.gas_oil_ratio_scf_stb,
                current.sand_detector_ppm,
                current.corrosion_rate_mpy,
                current.h2s_level_ppm,
                current.co2_level_percent,
                current.vibration_mm_s,
            )
        )
        assert 0 <= current.tubing_pressure_psi <= parameters.max_tubing_pressure
        assert 0 <= current.flow_rate_bpd <= parameters.max_flow_rate
        assert 0 <= current.water_cut_percent <= 100
        assert parameters.min_blockage_factor <= current.blockage_factor <= 1
        assert 1 <= current.corrosion_factor <= parameters.max_corrosion_factor
        assert current.oil_rate_bpd + current.water_rate_bpd == pytest.approx(
            current.flow_rate_bpd
        )
        assert current.gas_rate_scfd == pytest.approx(
            current.oil_rate_bpd * current.gas_oil_ratio_scf_stb
        )
        assert current.annulus_pressure_psi >= 0
