"""Pure, one-second reduced-order wellhead equations for synthetic operation."""

from __future__ import annotations

import math
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Self

from twin_core.config import stable_fraction

MODEL_STEP_SECONDS = 1.0
EQUILIBRIUM_SEARCH_STEPS = 48


def _clamp(value: float, minimum: float, maximum: float) -> float:
    """Keep a calculated proxy inside its declared physical bounds."""
    return min(max(value, minimum), maximum)


def _utc(at: datetime) -> None:
    """Reject timestamps that cannot identify a UTC simulation instant."""
    offset = at.utcoffset()
    if offset is None:
        raise ValueError("Model time must have a UTC offset")
    if offset.total_seconds() != 0:
        raise ValueError("Model time must be UTC")


@dataclass(frozen=True)
class ModelParameters:
    """Typed, validated numeric inputs to the pure model step."""

    reservoir_pressure_proxy: float
    downstream_pressure: float
    productivity_index: float
    choke_coefficient: float
    pressure_gain_coefficient: float
    natural_loss_coefficient: float
    ambient_temperature: float
    thermal_response_rate: float
    flow_heating_coefficient: float
    stress_heating_coefficient: float
    water_cut_drift_rate: float
    corrosion_rate_coefficient: float
    blockage_growth_coefficient: float
    min_blockage_factor: float
    max_tubing_pressure: float
    max_flow_rate: float
    pump_off_flow_factor: float
    pump_response_rate: float
    casing_pressure_fraction: float
    casing_response_rate: float
    max_corrosion_factor: float

    @classmethod
    def from_values(cls, values: dict[str, float]) -> Self:
        """Require every model coefficient and reject unsupported keys."""
        expected = {field.name for field in fields(cls)}
        if set(values) != expected:
            raise ValueError("Model parameter keys do not match the model version")
        parameters = cls(**values)
        if not all(math.isfinite(value) for value in values.values()):
            raise ValueError("Model parameters must be finite")
        if parameters.reservoir_pressure_proxy <= parameters.downstream_pressure:
            raise ValueError("Reservoir proxy must exceed downstream pressure")
        if parameters.max_tubing_pressure <= parameters.downstream_pressure:
            raise ValueError("Pressure bound must exceed downstream pressure")
        if not 0 <= parameters.pump_off_flow_factor <= 1:
            raise ValueError("Pump-off flow factor is out of range")
        return parameters


@dataclass(frozen=True)
class ControlInputs:
    """Synthetic operating inputs, with no connection to physical actuators."""

    choke_position_percent: float = 100.0
    master_valve_open: bool = True
    wing_valve_open: bool = True
    swab_valve_open: bool = False
    pump_on: bool = True
    emergency_shutdown: bool = False

    def validate(self) -> None:
        """Reject malformed controls before any process state changes."""
        position = self.choke_position_percent
        if isinstance(position, bool) or not isinstance(position, (int, float)):
            raise ValueError("Choke position must be a number")
        if not math.isfinite(position) or not 0 <= position <= 100:
            raise ValueError("Choke position must be between 0 and 100 percent")
        booleans = (
            self.master_valve_open,
            self.wing_valve_open,
            self.swab_valve_open,
            self.pump_on,
            self.emergency_shutdown,
        )
        if not all(isinstance(value, bool) for value in booleans):
            raise ValueError("Valve, pump, and shutdown controls must be booleans")


@dataclass(frozen=True)
class ProcessState:
    """Calculated state for one synthetic well at one simulation instant."""

    simulated_at: datetime
    reservoir_pressure_proxy_psi: float
    tubing_pressure_psi: float
    casing_pressure_psi: float
    flow_rate_bpd: float
    choke_position_percent: float
    master_valve_open: bool
    wing_valve_open: bool
    swab_valve_open: bool
    pump_on: bool
    emergency_shutdown: bool
    pump_flow_factor: float
    wellhead_temperature_f: float
    water_cut_percent: float
    blockage_factor: float
    corrosion_factor: float

    def to_payload(self) -> dict[str, object]:
        """Return unit-labelled model state for the internal read API."""
        return {
            "source": "synthetic_reduced_order_model",
            "fieldCalibrated": False,
            "simulatedAt": self.simulated_at.isoformat(),
            "reservoirPressureProxyPsi": self.reservoir_pressure_proxy_psi,
            "tubingPressurePsi": self.tubing_pressure_psi,
            "casingPressurePsi": self.casing_pressure_psi,
            "flowRateBpd": self.flow_rate_bpd,
            "chokePositionPercent": self.choke_position_percent,
            "masterValveOpen": self.master_valve_open,
            "wingValveOpen": self.wing_valve_open,
            "swabValveOpen": self.swab_valve_open,
            "pumpOn": self.pump_on,
            "emergencyShutdown": self.emergency_shutdown,
            "pumpFlowFactor": self.pump_flow_factor,
            "wellheadTemperatureF": self.wellhead_temperature_f,
            "waterCutPercent": self.water_cut_percent,
            "blockageFactor": self.blockage_factor,
            "corrosionFactor": self.corrosion_factor,
        }


def _flow(
    pressure_psi: float,
    controls: ControlInputs,
    parameters: ModelParameters,
    pump_factor: float,
    blockage_factor: float,
) -> float:
    """Calculate bounded choke outflow in barrels per day."""
    if (
        not controls.master_valve_open
        or not controls.wing_valve_open
        or controls.emergency_shutdown
    ):
        return 0.0
    pressure_drop = max(pressure_psi - parameters.downstream_pressure, 0.0)
    flow = (
        parameters.choke_coefficient
        * (controls.choke_position_percent / 100.0)
        * pump_factor
        * blockage_factor
        * math.sqrt(pressure_drop)
    )
    return min(flow, parameters.max_flow_rate)


def initial_state(
    wellhead_id: int,
    parameters: ModelParameters,
    controls: ControlInputs,
    at: datetime,
) -> ProcessState:
    """Start one synthetic well near its simplified operating balance."""
    if wellhead_id <= 0:
        raise ValueError("Wellhead id must be positive")
    _utc(at)
    controls.validate()
    water_fraction = 0.05 + 0.30 * stable_fraction(wellhead_id, "water_cut")
    blockage = 0.85 + 0.15 * stable_fraction(wellhead_id, "blockage")
    blockage = max(blockage, parameters.min_blockage_factor)
    corrosion = 1.0 + 0.01 * stable_fraction(wellhead_id, "corrosion")
    corrosion = min(corrosion, parameters.max_corrosion_factor)
    lower = parameters.downstream_pressure
    upper = min(parameters.reservoir_pressure_proxy, parameters.max_tubing_pressure)
    initial_pump_factor = 1.0 if controls.pump_on else parameters.pump_off_flow_factor
    inflow_at_bound = parameters.productivity_index * max(
        parameters.reservoir_pressure_proxy - upper, 0.0
    )
    outflow_at_bound = _flow(upper, controls, parameters, initial_pump_factor, blockage)
    if inflow_at_bound > outflow_at_bound + 0.01:
        raise ValueError("No operating balance exists below the pressure bound")
    for _ in range(EQUILIBRIUM_SEARCH_STEPS):
        pressure = (lower + upper) / 2.0
        inflow = parameters.productivity_index * max(
            parameters.reservoir_pressure_proxy - pressure, 0.0
        )
        outflow = _flow(pressure, controls, parameters, initial_pump_factor, blockage)
        if inflow > outflow:
            lower = pressure
        else:
            upper = pressure
    tubing = (lower + upper) / 2.0
    flow = _flow(tubing, controls, parameters, initial_pump_factor, blockage)
    casing = parameters.downstream_pressure + parameters.casing_pressure_fraction * (
        tubing - parameters.downstream_pressure
    )
    temperature = (
        parameters.ambient_temperature
        + parameters.flow_heating_coefficient * flow / parameters.max_flow_rate
        + parameters.stress_heating_coefficient
        * tubing
        / parameters.max_tubing_pressure
    )
    return ProcessState(
        simulated_at=at,
        reservoir_pressure_proxy_psi=parameters.reservoir_pressure_proxy,
        tubing_pressure_psi=tubing,
        casing_pressure_psi=casing,
        flow_rate_bpd=flow,
        choke_position_percent=controls.choke_position_percent,
        master_valve_open=controls.master_valve_open,
        wing_valve_open=controls.wing_valve_open,
        swab_valve_open=controls.swab_valve_open,
        pump_on=controls.pump_on,
        emergency_shutdown=controls.emergency_shutdown,
        pump_flow_factor=initial_pump_factor,
        wellhead_temperature_f=temperature,
        water_cut_percent=water_fraction * 100.0,
        blockage_factor=blockage,
        corrosion_factor=corrosion,
    )


def step_model(
    current: ProcessState,
    controls: ControlInputs,
    parameters: ModelParameters,
    at: datetime,
    dt_seconds: float = MODEL_STEP_SECONDS,
) -> ProcessState:
    """Advance a well by one bounded, deterministic process step."""
    _utc(at)
    controls.validate()
    if not math.isfinite(dt_seconds) or dt_seconds != MODEL_STEP_SECONDS:
        raise ValueError("The model requires a one-second timestep")
    if at <= current.simulated_at:
        raise ValueError("Simulation time must advance")
    numeric_state = (
        current.reservoir_pressure_proxy_psi,
        current.tubing_pressure_psi,
        current.casing_pressure_psi,
        current.flow_rate_bpd,
        current.pump_flow_factor,
        current.wellhead_temperature_f,
        current.water_cut_percent,
        current.blockage_factor,
        current.corrosion_factor,
    )
    if not all(math.isfinite(value) for value in numeric_state):
        raise ValueError("Current model state is not finite")
    if not (
        0 <= current.tubing_pressure_psi <= parameters.max_tubing_pressure
        and 0 <= current.water_cut_percent <= 100
        and parameters.min_blockage_factor <= current.blockage_factor <= 1
        and 1 <= current.corrosion_factor <= parameters.max_corrosion_factor
        and parameters.pump_off_flow_factor <= current.pump_flow_factor <= 1
    ):
        raise ValueError("Current model state is outside its bounds")

    target_pump = 1.0 if controls.pump_on else parameters.pump_off_flow_factor
    pump_factor = (
        current.pump_flow_factor
        + parameters.pump_response_rate
        * (target_pump - current.pump_flow_factor)
        * dt_seconds
    )
    inflow = parameters.productivity_index * max(
        parameters.reservoir_pressure_proxy - current.tubing_pressure_psi, 0.0
    )
    outflow = _flow(
        current.tubing_pressure_psi,
        controls,
        parameters,
        pump_factor,
        current.blockage_factor,
    )
    tubing = _clamp(
        current.tubing_pressure_psi
        + (
            parameters.pressure_gain_coefficient * (inflow - outflow)
            - parameters.natural_loss_coefficient
        )
        * dt_seconds,
        0.0,
        parameters.max_tubing_pressure,
    )
    casing_target = parameters.downstream_pressure + (
        parameters.casing_pressure_fraction * (tubing - parameters.downstream_pressure)
    )
    casing = (
        current.casing_pressure_psi
        + parameters.casing_response_rate
        * (casing_target - current.casing_pressure_psi)
        * dt_seconds
    )
    flow_ratio = _clamp(outflow / parameters.max_flow_rate, 0.0, 1.0)
    pressure_ratio = _clamp(tubing / parameters.max_tubing_pressure, 0.0, 1.0)
    temperature_target = (
        parameters.ambient_temperature
        + parameters.flow_heating_coefficient * flow_ratio
        + parameters.stress_heating_coefficient * pressure_ratio
    )
    temperature = current.wellhead_temperature_f + (
        parameters.thermal_response_rate
        * (temperature_target - current.wellhead_temperature_f)
        * dt_seconds
    )
    water_fraction = _clamp(
        current.water_cut_percent / 100.0
        + parameters.water_cut_drift_rate * (1.0 + pressure_ratio) * dt_seconds,
        0.0,
        1.0,
    )
    corrosion = _clamp(
        current.corrosion_factor
        + parameters.corrosion_rate_coefficient
        * water_fraction
        * (1.0 + pressure_ratio)
        * dt_seconds,
        1.0,
        parameters.max_corrosion_factor,
    )
    blockage = _clamp(
        current.blockage_factor
        - parameters.blockage_growth_coefficient
        * (water_fraction + flow_ratio)
        * dt_seconds,
        parameters.min_blockage_factor,
        1.0,
    )
    outputs = (
        pump_factor,
        inflow,
        outflow,
        tubing,
        casing,
        temperature,
        water_fraction,
        corrosion,
        blockage,
    )
    if not all(math.isfinite(value) for value in outputs):
        raise ValueError("Model calculation is not finite")
    return ProcessState(
        simulated_at=at,
        reservoir_pressure_proxy_psi=parameters.reservoir_pressure_proxy,
        tubing_pressure_psi=tubing,
        casing_pressure_psi=casing,
        flow_rate_bpd=outflow,
        choke_position_percent=controls.choke_position_percent,
        master_valve_open=controls.master_valve_open,
        wing_valve_open=controls.wing_valve_open,
        swab_valve_open=controls.swab_valve_open,
        pump_on=controls.pump_on,
        emergency_shutdown=controls.emergency_shutdown,
        pump_flow_factor=pump_factor,
        wellhead_temperature_f=temperature,
        water_cut_percent=water_fraction * 100.0,
        blockage_factor=blockage,
        corrosion_factor=corrosion,
    )
