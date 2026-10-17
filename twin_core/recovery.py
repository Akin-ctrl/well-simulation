"""Pure reconstruction and bounded settling of a saved synthetic well state."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import fields, replace
from datetime import datetime, timedelta
from typing import Any, cast

from twin_core.derived_physics import annulus_next_pressure, derive_readings
from twin_core.model import (
    ControlInputs,
    ModelParameters,
    ProcessState,
    _flow,
    balance_pressure,
    step_model,
)

BOOLEAN_FIELDS = frozenset(
    {
        "master_valve_open",
        "wing_valve_open",
        "swab_valve_open",
        "pump_on",
        "emergency_shutdown",
    }
)


def parameter_fingerprint(parameters: ModelParameters) -> str:
    """Identify the complete resolved parameter set with stable JSON and SHA-256."""
    canonical = json.dumps(vars(parameters), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _payload_key(name: str) -> str:
    """Convert a snake-case state field to its public camel-case key."""
    first, *rest = name.split("_")
    return first + "".join(part.capitalize() for part in rest)


def _decode_field(name: str, raw: object) -> object:
    """Check one timestamp, boolean, or finite numeric model field."""
    key = _payload_key(name)
    if name == "simulated_at":
        if not isinstance(raw, str):
            raise ValueError("Snapshot time is missing")
        try:
            instant = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("Snapshot time is invalid") from exc
        if instant.utcoffset() != timedelta(0):
            raise ValueError("Snapshot time must be UTC")
        return instant
    if name in BOOLEAN_FIELDS:
        if not isinstance(raw, bool):
            raise ValueError(f"Snapshot {key} must be boolean")
        return raw
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"Snapshot {key} must be numeric")
    if not math.isfinite(raw):
        raise ValueError(f"Snapshot {key} must be finite")
    return float(raw)


def decode_process(payload: object) -> ProcessState:
    """Reject malformed snapshot JSON before it can become a live model state."""
    if not isinstance(payload, dict):
        raise ValueError("Snapshot state must be an object")
    if (
        payload.get("source") != "synthetic_reduced_order_model"
        or payload.get("fieldCalibrated") is not False
    ):
        raise ValueError("Snapshot source is not the synthetic model")
    values = {
        field.name: _decode_field(field.name, payload.get(_payload_key(field.name)))
        for field in fields(ProcessState)
    }
    # Each value has been checked against the field's expected runtime type.
    return ProcessState(**cast(dict[str, Any], values))


def saved_controls(state: ProcessState) -> ControlInputs:
    """Recover the actuator-like settings recorded with the process state."""
    controls = ControlInputs(
        choke_position_percent=state.choke_position_percent,
        master_valve_open=state.master_valve_open,
        wing_valve_open=state.wing_valve_open,
        swab_valve_open=state.swab_valve_open,
        pump_on=state.pump_on,
        emergency_shutdown=state.emergency_shutdown,
    )
    controls.validate()
    return controls


def _settled(saved: float, balance: float, gap: float, rate: float) -> float:
    """Move one fast state toward balance using its first-order response rate."""
    return balance + (saved - balance) * math.exp(-gap * rate)


def restore_state(
    saved: ProcessState,
    parameters: ModelParameters,
    controls: ControlInputs,
    gap_seconds: float,
    at: datetime,
    max_catchup_steps: int = 5,
) -> ProcessState:
    """Catch up short gaps and settle fast state across larger missing time."""
    if not math.isfinite(gap_seconds) or max_catchup_steps < 1:
        raise ValueError("Recovery gap or catch-up bound is invalid")
    if at.utcoffset() != timedelta(0):
        raise ValueError("Recovery time must be UTC")
    controls.validate()
    # Validate every saved physical bound through the existing model boundary.
    step_model(saved, controls, parameters, saved.simulated_at + timedelta(seconds=1))
    if gap_seconds < 0:
        return saved
    if gap_seconds <= max_catchup_steps:
        current = saved
        for second in range(1, math.floor(gap_seconds) + 1):
            current = step_model(
                current,
                controls,
                parameters,
                saved.simulated_at + timedelta(seconds=second),
            )
        return current

    target_pump = 1.0 if controls.pump_on else parameters.pump_off_flow_factor
    balance_tubing = balance_pressure(
        controls, parameters, target_pump, saved.blockage_factor
    )
    pressure_drop = max(balance_tubing - parameters.downstream_pressure, 0.0)
    balance_outflow = _flow(
        balance_tubing, controls, parameters, target_pump, saved.blockage_factor
    )
    if pressure_drop > 0 and balance_outflow < parameters.max_flow_rate:
        choke_slope = (
            parameters.choke_coefficient
            * (controls.choke_position_percent / 100.0)
            * target_pump
            * saved.blockage_factor
            / (2.0 * math.sqrt(pressure_drop))
        )
    else:
        choke_slope = 0.0
    pressure_rate = parameters.pressure_gain_coefficient * (
        parameters.productivity_index + choke_slope
    )
    tubing = _settled(
        saved.tubing_pressure_psi, balance_tubing, gap_seconds, pressure_rate
    )
    casing_balance = parameters.downstream_pressure + (
        parameters.casing_pressure_fraction
        * (balance_tubing - parameters.downstream_pressure)
    )
    casing = _settled(
        saved.casing_pressure_psi,
        casing_balance,
        gap_seconds,
        parameters.casing_response_rate,
    )
    balance_temperature = (
        parameters.ambient_temperature
        + parameters.flow_heating_coefficient
        * balance_outflow
        / parameters.max_flow_rate
        + parameters.stress_heating_coefficient
        * balance_tubing
        / parameters.max_tubing_pressure
    )
    temperature = _settled(
        saved.wellhead_temperature_f,
        balance_temperature,
        gap_seconds,
        parameters.thermal_response_rate,
    )
    annulus_balance = parameters.ambient_temperature + (
        parameters.annulus_thermal_coupling
        * (balance_temperature - parameters.ambient_temperature)
    )
    annulus_temperature = _settled(
        saved.annulus_temperature_f,
        annulus_balance,
        gap_seconds,
        parameters.annulus_thermal_response_rate,
    )
    annulus_pressure = annulus_next_pressure(
        saved.annulus_pressure_psi,
        saved.annulus_temperature_f,
        annulus_temperature,
    )
    pump_factor = _settled(
        saved.pump_flow_factor,
        target_pump,
        gap_seconds,
        parameters.pump_response_rate,
    )
    flow = _flow(tubing, controls, parameters, pump_factor, saved.blockage_factor)
    derived = derive_readings(
        flow_rate_bpd=flow,
        water_cut_percent=saved.water_cut_percent,
        reservoir_pressure_psi=parameters.reservoir_pressure_proxy,
        tubing_pressure_psi=tubing,
        wellhead_temperature_f=temperature,
        pump_flow_factor=pump_factor,
        pump_on=controls.pump_on,
        parameters=parameters,
    )
    result = replace(
        saved,
        simulated_at=at,
        tubing_pressure_psi=tubing,
        casing_pressure_psi=casing,
        flow_rate_bpd=flow,
        pump_flow_factor=pump_factor,
        wellhead_temperature_f=temperature,
        annulus_temperature_f=annulus_temperature,
        annulus_pressure_psi=annulus_pressure,
        **vars(derived),
    )
    step_model(result, controls, parameters, at + timedelta(seconds=1))
    return result
