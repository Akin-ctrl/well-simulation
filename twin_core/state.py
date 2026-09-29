"""Keep per-well lifecycle state separate from future process outputs."""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime

from twin_core.config import ModelDefaults, ParameterOverrideSource, resolve_parameters

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessState:
    """Physical state produced by the later reduced-order model.

    No ProcessState is created by the Milestone 4 skeleton. Every field here
    needs an actual model update before it can be served as a model result.
    """

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
    wellhead_temperature_f: float
    water_cut_percent: float
    blockage_factor: float
    corrosion_factor: float

    def to_payload(self) -> dict[str, object]:
        """Return the unit-labelled internal API representation."""
        return {
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
            "wellheadTemperatureF": self.wellhead_temperature_f,
            "waterCutPercent": self.water_cut_percent,
            "blockageFactor": self.blockage_factor,
            "corrosionFactor": self.corrosion_factor,
        }


@dataclass(frozen=True)
class WellheadState:
    """Current service state for one wellhead."""

    wellhead_id: int
    model_version: str
    status: str
    reason: str
    tick_index: int = 0
    last_tick_at: datetime | None = None
    process: ProcessState | None = None

    def to_payload(self) -> dict[str, object]:
        """Distinguish service ticks from physical simulation time."""
        return {
            "wellheadId": self.wellhead_id,
            "modelVersion": self.model_version,
            "status": self.status,
            "reason": self.reason,
            "tickIndex": self.tick_index,
            "lastTickAt": (
                self.last_tick_at.isoformat() if self.last_tick_at else None
            ),
            "process": self.process.to_payload() if self.process else None,
        }


class ModelRegistry:
    """Own model lifecycle state and validated parameters for active wells."""

    def __init__(
        self,
        defaults: ModelDefaults,
        environment_overrides: Mapping[str, object],
        override_source: ParameterOverrideSource,
    ) -> None:
        """Keep one authoritative state and parameter set per wellhead."""
        self.defaults = defaults
        self._environment_overrides = environment_overrides
        self._override_source = override_source
        self._states: dict[int, WellheadState] = {}
        self._parameters: dict[int, dict[str, float]] = {}
        self._lock = threading.RLock()

    def replace_wellheads(self, wellhead_ids: Sequence[int]) -> None:
        """Adopt active database wells and validate each well's parameters."""
        if len(set(wellhead_ids)) != len(wellhead_ids) or any(
            wellhead_id <= 0 for wellhead_id in wellhead_ids
        ):
            raise ValueError("Wellhead ids must be unique positive integers")

        overrides = self._override_source.load(wellhead_ids, self.defaults.version)
        unknown_wells = set(overrides) - set(wellhead_ids)
        if unknown_wells:
            raise ValueError("Override source returned an unknown wellhead")

        with self._lock:
            next_states: dict[int, WellheadState] = {}
            next_parameters: dict[int, dict[str, float]] = {}
            for wellhead_id in wellhead_ids:
                prior = self._states.get(wellhead_id)
                try:
                    next_parameters[wellhead_id] = resolve_parameters(
                        self.defaults,
                        self._environment_overrides,
                        overrides.get(wellhead_id, {}),
                    )
                    reason = "model_not_implemented"
                except ValueError as exc:
                    reason = "invalid_parameters"
                    logger.error(
                        "Model parameters rejected",
                        extra={"wellhead_id": wellhead_id, "reason": str(exc)},
                    )
                next_states[wellhead_id] = (
                    replace(prior, status="unavailable", reason=reason)
                    if prior
                    else WellheadState(
                        wellhead_id=wellhead_id,
                        model_version=self.defaults.version,
                        status="unavailable",
                        reason=reason,
                    )
                )
            self._states = next_states
            self._parameters = next_parameters

    def tick(self, at: datetime) -> None:
        """Record one service tick without inventing a process update."""
        with self._lock:
            self._states = {
                wellhead_id: replace(
                    state, tick_index=state.tick_index + 1, last_tick_at=at
                )
                for wellhead_id, state in self._states.items()
            }

    def get(self, wellhead_id: int) -> WellheadState | None:
        """Return the immutable current state, if the wellhead is active."""
        with self._lock:
            return self._states.get(wellhead_id)

    def count(self) -> int:
        """Return the number of active wellheads in memory."""
        with self._lock:
            return len(self._states)

    def invalid_count(self) -> int:
        """Return the number of wells with rejected model parameters."""
        with self._lock:
            return sum(
                state.reason == "invalid_parameters" for state in self._states.values()
            )
