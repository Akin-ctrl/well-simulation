"""Bounded forecasts and isolated control scenarios from the live model state."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from typing import Literal

from twin_core.model import ControlInputs, ModelParameters, ProcessState, step_model

MIN_HORIZON_SECONDS = 300
MAX_HORIZON_SECONDS = 7200
MIN_STEP_SECONDS = 5
MAX_STEP_SECONDS = 300
MAX_CONTROL_STEPS = 16
Risk = Literal["none", "warning", "critical", "unknown"]


class AlarmRuleConfigurationError(ValueError):
    """An active alarm rule cannot be projected from this model version."""


# These are the canonical parameterType codes and units already published by
# the gateway. Keep the mapping complete so every active alarm rule can be
# projected. A parity test checks it against the gateway's signal list.
MODEL_SIGNALS = {
    "tubing_pressure": "tubing_pressure_psi",
    "casing_pressure": "casing_pressure_psi",
    "annulus_pressure": "annulus_pressure_psi",
    "gas_oil_ratio": "gas_oil_ratio_scf_stb",
    "sand_detector": "sand_detector_ppm",
    "corrosion_rate": "corrosion_rate_mpy",
    "h2s_level": "h2s_level_ppm",
    "co2_level": "co2_level_percent",
    "vibration": "vibration_mm_s",
    "wellhead_temperature": "wellhead_temperature_f",
    "choke_valve_position": "choke_position_percent",
    "flow_rate": "flow_rate_bpd",
    "water_cut": "water_cut_percent",
    "master_valve_status": "master_valve_open",
    "wing_valve_status": "wing_valve_open",
    "swab_valve_status": "swab_valve_open",
    "emergency_shutdown": "emergency_shutdown",
    "pump_status": "pump_on",
}


@dataclass(frozen=True)
class ProjectionBase:
    """One consistent, immutable copy of a running well and its inputs."""

    wellhead_id: int
    model_version: str
    parameter_fingerprint: str
    state: ProcessState
    controls: ControlInputs
    parameters: ModelParameters
    restored_from: datetime | None
    last_recovery_event_at: datetime | None


@dataclass(frozen=True)
class ControlChange:
    """Scenario-only edits to actuator-like inputs; None means unchanged."""

    choke_position_percent: float | None = None
    master_valve_open: bool | None = None
    wing_valve_open: bool | None = None
    pump_on: bool | None = None
    emergency_shutdown: bool | None = None

    def apply(self, controls: ControlInputs) -> ControlInputs:
        """Build a new validated control set without changing the original."""
        changes = {
            key: value for key, value in asdict(self).items() if value is not None
        }
        updated = replace(controls, **changes)
        updated.validate()
        return updated

    def has_change(self) -> bool:
        """Reject empty scenario steps at the request boundary."""
        return any(value is not None for value in asdict(self).values())


@dataclass(frozen=True)
class ScheduledControl:
    """A control change applied before the next one-second model step."""

    offset_seconds: int
    change: ControlChange


@dataclass(frozen=True)
class AlarmRule:
    """One active historian threshold in its parameter's canonical unit."""

    rule_id: int
    parameter_code: str
    severity: Literal["WARNING", "CRITICAL"]
    operator: Literal[">", ">=", "<", "<=", "=="]
    threshold: float


@dataclass(frozen=True)
class ProjectionPoint:
    """One sampled future model state and optional unchanged baseline."""

    elapsed_seconds: int
    at: datetime
    values: dict[str, float]
    alarm_risk: Risk
    triggered_rule_ids: tuple[int, ...]
    baseline_values: dict[str, float] | None = None
    baseline_alarm_risk: Risk | None = None

    def to_payload(self) -> dict[str, object]:
        """Expose timestamps, values, and threshold projections explicitly."""
        payload: dict[str, object] = {
            "elapsedSeconds": self.elapsed_seconds,
            "timestamp": self.at.isoformat(),
            "values": self.values,
            "alarmRisk": self.alarm_risk,
            "triggeredRuleIds": self.triggered_rule_ids,
        }
        if self.baseline_values is not None:
            payload["baselineValues"] = self.baseline_values
            payload["baselineAlarmRisk"] = self.baseline_alarm_risk
        return payload


@dataclass(frozen=True)
class ProjectionResult:
    """A completed run that can be saved atomically and returned to the caller."""

    kind: Literal["forecast", "scenario"]
    run_id: str
    base: ProjectionBase
    generated_at: datetime
    horizon_seconds: int
    step_seconds: int
    points: tuple[ProjectionPoint, ...]
    confidence_score: float
    confidence_reasons: tuple[str, ...]
    alarm_rules: tuple[AlarmRule, ...]
    name: str | None = None
    controls: tuple[ScheduledControl, ...] = ()

    def to_payload(self) -> dict[str, object]:
        """Keep synthetic origin and heuristic uncertainty visible to clients."""
        return {
            "runId": self.run_id,
            "kind": self.kind,
            "wellheadId": self.base.wellhead_id,
            "name": self.name,
            "source": "synthetic_reduced_order_model",
            "fieldCalibrated": False,
            "modelVersion": self.base.model_version,
            "baseSimulatedAt": self.base.state.simulated_at.isoformat(),
            "generatedAt": self.generated_at.isoformat(),
            "horizonSeconds": self.horizon_seconds,
            "stepSeconds": self.step_seconds,
            "confidence": {
                "method": "heuristic_uncalibrated",
                "score": self.confidence_score,
                "label": "moderate" if self.confidence_score >= 0.4 else "low",
                "reasons": self.confidence_reasons,
            },
            "points": [point.to_payload() for point in self.points],
        }


def validate_window(horizon_seconds: object, step_seconds: object) -> tuple[int, int]:
    """Bound CPU and point count, and require an exact final sample."""
    if (
        type(horizon_seconds) is not int
        or not MIN_HORIZON_SECONDS <= horizon_seconds <= MAX_HORIZON_SECONDS
    ):
        raise ValueError("horizonSeconds must be between 300 and 7200")
    if (
        type(step_seconds) is not int
        or not MIN_STEP_SECONDS <= step_seconds <= MAX_STEP_SECONDS
    ):
        raise ValueError("stepSeconds must be between 5 and 300")
    if horizon_seconds % step_seconds:
        raise ValueError("horizonSeconds must be divisible by stepSeconds")
    return horizon_seconds, step_seconds


def signal_values(state: ProcessState) -> dict[str, float]:
    """Read the same model quantities that the gateway publishes as telemetry."""
    values = {
        code: float(getattr(state, field)) for code, field in MODEL_SIGNALS.items()
    }
    # The gateway publishes choke position as an integer register value.
    values["choke_valve_position"] = float(round(state.choke_position_percent))
    return values


def _breached(value: float, rule: AlarmRule) -> bool:
    """Use the same operators as the historian alarm trigger."""
    return {
        ">": value > rule.threshold,
        ">=": value >= rule.threshold,
        "<": value < rule.threshold,
        "<=": value <= rule.threshold,
        "==": value == rule.threshold,
    }[rule.operator]


def alarm_projection(
    values: dict[str, float], rules: tuple[AlarmRule, ...]
) -> tuple[Risk, tuple[int, ...]]:
    """Report projected threshold crossings, not a calibrated probability."""
    if not rules:
        return "unknown", ()
    triggered = tuple(
        rule.rule_id for rule in rules if _breached(values[rule.parameter_code], rule)
    )
    if any(rule.rule_id in triggered and rule.severity == "CRITICAL" for rule in rules):
        return "critical", triggered
    if triggered:
        return "warning", triggered
    return "none", ()


def _near_threshold(
    points: tuple[ProjectionPoint, ...], rules: tuple[AlarmRule, ...]
) -> bool:
    """Find any model value within ten percent of an alarm threshold."""
    return any(
        abs(point.values[rule.parameter_code] - rule.threshold)
        <= 0.1 * max(abs(rule.threshold), 1.0)
        for point in points
        for rule in rules
    )


def _sharp_change(initial: ControlInputs, steps: tuple[ScheduledControl, ...]) -> bool:
    """Name large choke movements and any binary actuator change."""
    previous = initial
    for step in steps:
        current = step.change.apply(previous)
        if (
            abs(current.choke_position_percent - previous.choke_position_percent) > 25
            or current.master_valve_open != previous.master_valve_open
            or current.wing_valve_open != previous.wing_valve_open
            or current.pump_on != previous.pump_on
            or current.emergency_shutdown != previous.emergency_shutdown
        ):
            return True
        previous = current
    return False


def validate_projection_request(
    kind: Literal["forecast", "scenario"],
    name: str | None,
    controls: tuple[ScheduledControl, ...],
    horizon_seconds: int,
    step_seconds: int,
) -> None:
    """Check schedule bounds before running the first model step."""
    validate_window(horizon_seconds, step_seconds)
    if kind == "scenario" and (not name or not controls):
        raise ValueError("A scenario needs a name and at least one control step")
    if kind == "forecast" and (name is not None or controls):
        raise ValueError("A forecast cannot change controls")
    if len(controls) > MAX_CONTROL_STEPS:
        raise ValueError("Too many scenario control steps")
    offsets = [step.offset_seconds for step in controls]
    if offsets != sorted(set(offsets)) or any(
        offset < 0 or offset >= horizon_seconds or offset % 60 for offset in offsets
    ):
        raise ValueError(
            "Control offsets must be unique ordered minutes in the horizon"
        )
    if any(not step.change.has_change() for step in controls):
        raise ValueError("Scenario control steps cannot be empty")


def _confidence(
    base: ProjectionBase,
    generated_at: datetime,
    horizon_seconds: int,
    points: tuple[ProjectionPoint, ...],
    rules: tuple[AlarmRule, ...],
    controls: tuple[ScheduledControl, ...],
) -> tuple[float, tuple[str, ...]]:
    """Explain a bounded heuristic score, never a statistical interval."""
    reasons = ["synthetic_uncalibrated", "forecast_horizon"]
    score = 0.65 - 0.25 * horizon_seconds / MAX_HORIZON_SECONDS
    if (
        base.restored_from is not None
        and base.last_recovery_event_at is not None
        and 0 <= (generated_at - base.last_recovery_event_at).total_seconds() <= 1800
    ):
        reasons.append("recent_recovery")
        score -= 0.1
    if not rules:
        reasons.append("no_active_alarm_rules")
        score -= 0.1
    elif _near_threshold(points, rules):
        reasons.append("near_alarm_threshold")
        score -= 0.1
    if _sharp_change(base.controls, controls):
        reasons.append("sharp_control_change")
        score -= 0.1
    score = round(max(0.1, min(0.65, score)), 3)
    if not math.isfinite(score):
        raise ArithmeticError("Projection confidence is not finite")
    return score, tuple(reasons)


def project(
    kind: Literal["forecast", "scenario"],
    run_id: str,
    base: ProjectionBase,
    generated_at: datetime,
    horizon_seconds: int,
    step_seconds: int,
    rules: tuple[AlarmRule, ...],
    *,
    name: str | None = None,
    controls: tuple[ScheduledControl, ...] = (),
) -> ProjectionResult:
    """Integrate the existing one-second model on private immutable copies."""
    validate_projection_request(kind, name, controls, horizon_seconds, step_seconds)

    current = base.state
    baseline = base.state
    scenario_controls = base.controls
    points: list[ProjectionPoint] = []
    step_by_offset = {step.offset_seconds: step.change for step in controls}
    for elapsed in range(1, horizon_seconds + 1):
        change = step_by_offset.get(elapsed - 1)
        if change is not None:
            scenario_controls = change.apply(scenario_controls)
        at = base.state.simulated_at + timedelta(seconds=elapsed)
        current = step_model(current, scenario_controls, base.parameters, at)
        if kind == "scenario":
            baseline = step_model(baseline, base.controls, base.parameters, at)
        if elapsed % step_seconds:
            continue
        values = signal_values(current)
        risk, triggered = alarm_projection(values, rules)
        baseline_values = signal_values(baseline) if kind == "scenario" else None
        baseline_risk = (
            alarm_projection(baseline_values, rules)[0]
            if baseline_values is not None
            else None
        )
        points.append(
            ProjectionPoint(
                elapsed, at, values, risk, triggered, baseline_values, baseline_risk
            )
        )

    sampled = tuple(points)
    score, reasons = _confidence(
        base, generated_at, horizon_seconds, sampled, rules, controls
    )
    return ProjectionResult(
        kind,
        run_id,
        base,
        generated_at,
        horizon_seconds,
        step_seconds,
        sampled,
        score,
        reasons,
        rules,
        name,
        controls,
    )
