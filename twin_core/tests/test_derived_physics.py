"""Independent physical invariants for the synthetic reference well."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import partial

import pytest
from twin_core.config import load_defaults, resolve_parameters
from twin_core.derived_physics import (
    ATMOSPHERIC_PRESSURE_PSI,
    FARADAY_C_PER_MOL,
    IRON_KG_PER_MOL,
    MILS_PER_METER,
    SECONDS_PER_YEAR,
    STEEL_KG_PER_M3,
    annulus_next_pressure,
    derive_readings,
    kelvin,
)
from twin_core.model import ControlInputs, ModelParameters, initial_state, step_model

START = datetime(2026, 9, 30, tzinfo=timezone.utc)


def baseline() -> ModelParameters:
    return ModelParameters.from_values(resolve_parameters(load_defaults(), {}, {}, 1))


def test_phase_balance_and_standard_gas_rate() -> None:
    state = initial_state(1, baseline(), ControlInputs(), START)
    assert state.oil_rate_bpd + state.water_rate_bpd == pytest.approx(
        state.flow_rate_bpd
    )
    assert state.gas_rate_scfd == pytest.approx(
        state.gas_oil_ratio_scf_stb * state.oil_rate_bpd
    )
    assert state.h2s_level_ppm == pytest.approx(20)
    assert state.co2_level_percent == pytest.approx(3)
    assert state.to_payload()["corrosionRateMpy"] == state.corrosion_rate_mpy


def test_pressure_release_and_sand_drawdown_have_causal_response() -> None:
    parameters = baseline()
    common = partial(
        derive_readings,
        flow_rate_bpd=1000.0,
        water_cut_percent=20.0,
        reservoir_pressure_psi=3000.0,
        wellhead_temperature_f=120.0,
        pump_flow_factor=1.0,
        pump_on=True,
        parameters=parameters,
    )
    high = common(tubing_pressure_psi=2500)
    low = common(tubing_pressure_psi=1000)
    assert low.gas_oil_ratio_scf_stb > high.gas_oil_ratio_scf_stb
    assert high.sand_detector_ppm == 0
    assert low.sand_detector_ppm > 0
    assert high.co2_level_percent == low.co2_level_percent


def test_annulus_obeys_absolute_temperature_gas_law() -> None:
    old = 150.0
    new = annulus_next_pressure(old, 80.0, 100.0)
    assert (new + ATMOSPHERIC_PRESSURE_PSI) / (
        old + ATMOSPHERIC_PRESSURE_PSI
    ) == pytest.approx(kelvin(100.0) / kelvin(80.0))
    parameters = baseline()
    initial = initial_state(1, parameters, ControlInputs(), START)
    next_state = step_model(
        initial, ControlInputs(), parameters, START + timedelta(seconds=1)
    )
    assert next_state.annulus_temperature_f > initial.annulus_temperature_f
    assert next_state.annulus_pressure_psi > initial.annulus_pressure_psi


def test_corrosion_rate_uses_faraday_units_and_water_wetting() -> None:
    parameters = baseline()
    common = partial(
        derive_readings,
        flow_rate_bpd=0.0,
        reservoir_pressure_psi=3000.0,
        tubing_pressure_psi=parameters.reference_co2_partial_pressure_psi
        / parameters.gas_co2_mole_fraction
        - ATMOSPHERIC_PRESSURE_PSI,
        wellhead_temperature_f=parameters.reference_temperature_f,
        pump_flow_factor=1.0,
        pump_on=False,
        parameters=parameters,
    )
    dry = common(water_cut_percent=0.0)
    wet = common(water_cut_percent=100.0)
    expected_current = parameters.reference_corrosion_current_a_m2 / (
        1 + parameters.water_wetting_half_fraction
    )
    expected_mpy = (
        expected_current
        * IRON_KG_PER_MOL
        / (2 * FARADAY_C_PER_MOL * STEEL_KG_PER_M3)
        * SECONDS_PER_YEAR
        * MILS_PER_METER
    )
    assert dry.corrosion_rate_mpy == 0
    assert wet.corrosion_rate_mpy == pytest.approx(expected_mpy)


def test_vibration_responds_to_pump_and_flow_and_zero_force() -> None:
    parameters = baseline()
    common = partial(
        derive_readings,
        water_cut_percent=20.0,
        reservoir_pressure_psi=3000.0,
        tubing_pressure_psi=1000.0,
        wellhead_temperature_f=120.0,
        pump_flow_factor=1.0,
        parameters=parameters,
    )
    quiet = common(flow_rate_bpd=0, pump_on=False)
    flowing = common(flow_rate_bpd=1000, pump_on=False)
    pumping = common(flow_rate_bpd=1000, pump_on=True)
    assert quiet.vibration_mm_s == 0
    assert quiet.gas_oil_ratio_scf_stb == 0
    assert quiet.h2s_level_ppm == 0
    assert quiet.co2_level_percent == 0
    assert flowing.vibration_mm_s > quiet.vibration_mm_s
    assert pumping.vibration_mm_s > flowing.vibration_mm_s


def test_zero_oil_and_closed_path_have_no_produced_gas() -> None:
    parameters = baseline()
    state = initial_state(1, parameters, ControlInputs(), START)
    closed = step_model(
        state,
        replace(ControlInputs(), master_valve_open=False),
        parameters,
        START + timedelta(seconds=1),
    )
    assert closed.flow_rate_bpd == 0
    assert closed.gas_rate_scfd == 0
    assert closed.sand_detector_ppm == 0
    assert closed.h2s_level_ppm == 0
    assert closed.co2_level_percent == 0
    water_only = derive_readings(
        flow_rate_bpd=100,
        water_cut_percent=100,
        reservoir_pressure_psi=3000,
        tubing_pressure_psi=1000,
        wellhead_temperature_f=120,
        pump_flow_factor=1,
        pump_on=True,
        parameters=parameters,
    )
    assert water_only.oil_rate_bpd == 0
    assert water_only.gas_rate_scfd == 0


def test_invalid_composition_and_nonfinite_state_fail_closed() -> None:
    parameters = baseline()
    with pytest.raises(ValueError, match="gas_h2s_mole_fraction"):
        ModelParameters.from_values(
            {
                **vars(parameters),
                "gas_h2s_mole_fraction": 0.6,
                "gas_co2_mole_fraction": 0.5,
            }
        )
    initial = initial_state(1, parameters, ControlInputs(), START)
    with pytest.raises(ValueError, match="not finite"):
        step_model(
            replace(initial, vibration_mm_s=float("nan")),
            ControlInputs(),
            parameters,
            START + timedelta(seconds=1),
        )
