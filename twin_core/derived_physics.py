"""Dimensioned reference-well diagnostics derived from one process state.

These equations are synthetic engineering surrogates. They are not calibrated
corrosion, sand, PVT, or machine-condition predictions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from twin_core.model import ModelParameters

FAHRENHEIT_TO_KELVIN_SCALE = 5.0 / 9.0
KELVIN_OFFSET_F = 459.67
FARADAY_C_PER_MOL = 96485.33212
IRON_KG_PER_MOL = 0.055845
STEEL_KG_PER_M3 = 7850.0
SECONDS_PER_YEAR = 365.25 * 86400.0
MILS_PER_METER = 39370.0787402
GAS_CONSTANT_J_PER_MOL_K = 8.314462618
ATMOSPHERIC_PRESSURE_PSI = 14.6959


def kelvin(fahrenheit: float) -> float:
    """Convert absolute Fahrenheit temperature to kelvin."""
    return (fahrenheit + KELVIN_OFFSET_F) * FAHRENHEIT_TO_KELVIN_SCALE


@dataclass(frozen=True)
class DerivedReadings:
    """Phase balance and seven diagnosed signals in historian units."""

    oil_rate_bpd: float
    water_rate_bpd: float
    gas_rate_scfd: float
    gas_oil_ratio_scf_stb: float
    sand_detector_ppm: float
    corrosion_rate_mpy: float
    h2s_level_ppm: float
    co2_level_percent: float
    vibration_mm_s: float


def derive_readings(
    *,
    flow_rate_bpd: float,
    water_cut_percent: float,
    reservoir_pressure_psi: float,
    tubing_pressure_psi: float,
    wellhead_temperature_f: float,
    pump_flow_factor: float,
    pump_on: bool,
    parameters: ModelParameters,
) -> DerivedReadings:
    """Calculate finite, dimensioned diagnostics from shared process inputs."""
    inputs = (
        flow_rate_bpd,
        water_cut_percent,
        reservoir_pressure_psi,
        tubing_pressure_psi,
        wellhead_temperature_f,
        pump_flow_factor,
    )
    if (
        not all(math.isfinite(value) for value in inputs)
        or flow_rate_bpd < 0
        or not 0 <= water_cut_percent <= 100
        or reservoir_pressure_psi < 0
        or tubing_pressure_psi < 0
        or wellhead_temperature_f <= -459.67
        or not 0 <= pump_flow_factor <= 1
        or not isinstance(pump_on, bool)
    ):
        raise ValueError("Invalid reference-well process input")
    water_fraction = water_cut_percent / 100.0
    oil_rate = flow_rate_bpd * (1.0 - water_fraction)
    water_rate = flow_rate_bpd * water_fraction
    if oil_rate > 0:
        # Empirical black-oil solution-gas relation at the flowing wellhead.
        pressure_ratio = min(
            (tubing_pressure_psi + ATMOSPHERIC_PRESSURE_PSI)
            / (parameters.bubble_point_pressure_psi + ATMOSPHERIC_PRESSURE_PSI),
            1.0,
        )
        temperature_ratio = kelvin(parameters.reference_temperature_f) / kelvin(
            wellhead_temperature_f
        )
        dissolved = (
            parameters.solution_gas_at_bubble_point_scf_stb
            * (pressure_ratio**parameters.solution_gas_pressure_exponent)
            * temperature_ratio
        )
        dissolved = min(dissolved, parameters.solution_gas_at_bubble_point_scf_stb)
        gas_oil_ratio = (
            parameters.free_gas_oil_ratio_scf_stb
            + parameters.solution_gas_at_bubble_point_scf_stb
            - dissolved
        )
    else:
        gas_oil_ratio = 0.0
    gas_rate = gas_oil_ratio * oil_rate
    h2s = parameters.gas_h2s_mole_fraction * 1_000_000.0 if gas_rate > 0 else 0.0
    co2 = parameters.gas_co2_mole_fraction * 100.0 if gas_rate > 0 else 0.0

    drawdown = max(reservoir_pressure_psi - tubing_pressure_psi, 0.0)
    excess_drawdown = max(drawdown - parameters.sand_onset_drawdown_psi, 0.0)
    sand_mass_kg_day = (
        parameters.sand_entrainment_kg_per_bbl_psi
        * excess_drawdown
        * flow_rate_bpd
        * flow_rate_bpd
        / parameters.max_flow_rate
    )
    liquid_mass_kg_day = parameters.liquid_density_kg_per_bbl * flow_rate_bpd
    sand_ppm = (
        1_000_000.0 * sand_mass_kg_day / liquid_mass_kg_day
        if liquid_mass_kg_day > 0
        else 0.0
    )

    # Uniform Fe -> Fe(II) wall loss from an assumed corrosion current density.
    # The current law is a bounded calibration surface, not a NORSOK M-506 model.
    wetting = water_fraction / (water_fraction + parameters.water_wetting_half_fraction)
    co2_partial_psi = parameters.gas_co2_mole_fraction * (
        tubing_pressure_psi + ATMOSPHERIC_PRESSURE_PSI
    )
    co2_factor = (
        co2_partial_psi / parameters.reference_co2_partial_pressure_psi
    ) ** parameters.co2_corrosion_exponent
    thermal_factor = math.exp(
        parameters.corrosion_activation_energy_j_mol
        / GAS_CONSTANT_J_PER_MOL_K
        * (
            1.0 / kelvin(parameters.reference_temperature_f)
            - 1.0 / kelvin(wellhead_temperature_f)
        )
    )
    flow_factor = 1.0 + parameters.corrosion_flow_factor * (
        flow_rate_bpd / parameters.max_flow_rate
    )
    current_a_m2 = (
        parameters.reference_corrosion_current_a_m2
        * wetting
        * co2_factor
        * thermal_factor
        * flow_factor
    )
    corrosion_m_per_second = (
        current_a_m2 * IRON_KG_PER_MOL / (2.0 * FARADAY_C_PER_MOL * STEEL_KG_PER_M3)
    )
    corrosion_mpy = corrosion_m_per_second * SECONDS_PER_YEAR * MILS_PER_METER

    omega = 2.0 * math.pi * parameters.vibration_excitation_hz
    natural_omega = 2.0 * math.pi * parameters.vibration_natural_hz
    mass = parameters.vibration_effective_mass_kg
    stiffness = mass * natural_omega**2
    damping = 2.0 * parameters.vibration_damping_ratio * mass * natural_omega
    force_n = (
        parameters.pump_excitation_force_n * pump_flow_factor if pump_on else 0.0
    ) + parameters.flow_excitation_force_n_per_bpd * flow_rate_bpd
    impedance = math.hypot(stiffness - mass * omega**2, damping * omega)
    vibration_mm_s = force_n * omega / (math.sqrt(2.0) * impedance) * 1000.0

    values = (
        oil_rate,
        water_rate,
        gas_rate,
        gas_oil_ratio,
        sand_ppm,
        corrosion_mpy,
        h2s,
        co2,
        vibration_mm_s,
    )
    if not all(math.isfinite(value) and value >= 0 for value in values):
        raise ValueError("Derived model calculation is invalid")
    return DerivedReadings(*values)


def annulus_next_pressure(
    pressure_psi: float,
    old_temperature_f: float,
    new_temperature_f: float,
) -> float:
    """Fixed-mass ideal gas: P2(abs) = P1(abs) * T2/T1."""
    absolute = (pressure_psi + ATMOSPHERIC_PRESSURE_PSI) * (
        kelvin(new_temperature_f) / kelvin(old_temperature_f)
    )
    result = absolute - ATMOSPHERIC_PRESSURE_PSI
    if not math.isfinite(result) or result < 0:
        raise ValueError("Annulus pressure calculation is invalid")
    return result
