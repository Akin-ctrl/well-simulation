"""Own one validated, in-memory process state for each active wellhead."""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from twin_core.config import ModelDefaults, ParameterOverrideSource, resolve_parameters
from twin_core.model import (
    ControlInputs,
    ModelParameters,
    ProcessState,
    initial_state,
    step_model,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WellheadState:
    """Current lifecycle and process state for one synthetic wellhead."""

    wellhead_id: int
    model_version: str
    status: str
    reason: str
    tick_index: int = 0
    skipped_ticks: int = 0
    last_tick_at: datetime | None = None
    process: ProcessState | None = None

    def to_payload(self) -> dict[str, object]:
        """Distinguish model output, service ticks, and skipped time."""
        return {
            "wellheadId": self.wellhead_id,
            "modelVersion": self.model_version,
            "status": self.status,
            "reason": self.reason,
            "tickIndex": self.tick_index,
            "skippedTicks": self.skipped_ticks,
            "lastTickAt": (
                self.last_tick_at.isoformat() if self.last_tick_at else None
            ),
            "process": self.process.to_payload() if self.process else None,
        }


class ModelRegistry:
    """Keep active well states and immutable parameter sets behind one lock."""

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
        self._parameters: dict[int, ModelParameters] = {}
        self._controls: dict[int, ControlInputs] = {}
        self._lock = threading.RLock()

    def replace_wellheads(self, wellhead_ids: Sequence[int]) -> None:
        """Adopt active wells without resetting unchanged process state."""
        if len(set(wellhead_ids)) != len(wellhead_ids) or any(
            wellhead_id <= 0 for wellhead_id in wellhead_ids
        ):
            raise ValueError("Wellhead ids must be unique positive integers")
        overrides = self._override_source.load(wellhead_ids, self.defaults.version)
        if set(overrides) - set(wellhead_ids):
            raise ValueError("Override source returned an unknown wellhead")

        with self._lock:
            next_states: dict[int, WellheadState] = {}
            next_parameters: dict[int, ModelParameters] = {}
            next_controls: dict[int, ControlInputs] = {}
            for wellhead_id in wellhead_ids:
                prior = self._states.get(wellhead_id)
                try:
                    values = resolve_parameters(
                        self.defaults,
                        self._environment_overrides,
                        overrides.get(wellhead_id, {}),
                        wellhead_id,
                    )
                    parameters = ModelParameters.from_values(values)
                except ValueError as exc:
                    logger.error(
                        "Model parameters rejected",
                        extra={"wellhead_id": wellhead_id, "reason": str(exc)},
                    )
                    next_states[wellhead_id] = (
                        replace(
                            prior, status="unavailable", reason="invalid_parameters"
                        )
                        if prior
                        else WellheadState(
                            wellhead_id,
                            self.defaults.version,
                            "unavailable",
                            "invalid_parameters",
                        )
                    )
                    continue
                previous_parameters = self._parameters.get(wellhead_id)
                if prior is not None and (
                    prior.reason == "restart_required"
                    or (
                        prior.reason == "invalid_parameters"
                        and prior.process is not None
                    )
                    or (
                        previous_parameters is not None
                        and previous_parameters != parameters
                    )
                ):
                    next_states[wellhead_id] = replace(
                        prior, status="unavailable", reason="restart_required"
                    )
                    if previous_parameters is not None:
                        next_parameters[wellhead_id] = previous_parameters
                elif prior is not None and prior.reason == "invalid_parameters":
                    next_states[wellhead_id] = replace(
                        prior, status="initializing", reason="model_not_started"
                    )
                    next_parameters[wellhead_id] = parameters
                else:
                    next_states[wellhead_id] = prior or WellheadState(
                        wellhead_id,
                        self.defaults.version,
                        "initializing",
                        "model_not_started",
                    )
                    next_parameters[wellhead_id] = parameters
                next_controls[wellhead_id] = self._controls.get(
                    wellhead_id, ControlInputs()
                )
            self._states = next_states
            self._parameters = next_parameters
            self._controls = next_controls

    def tick(self, at: datetime) -> tuple[int, int]:
        """Advance each available well exactly one second at a UTC instant."""
        advanced = 0
        failures = 0
        with self._lock:
            next_states: dict[int, WellheadState] = {}
            for wellhead_id, state in self._states.items():
                updated = replace(
                    state, tick_index=state.tick_index + 1, last_tick_at=at
                )
                if state.status not in ("initializing", "running"):
                    next_states[wellhead_id] = updated
                    continue
                parameters = self._parameters.get(wellhead_id)
                if parameters is None:
                    next_states[wellhead_id] = replace(
                        updated, status="unavailable", reason="invalid_parameters"
                    )
                    continue
                controls = self._controls[wellhead_id]
                try:
                    current = state.process or initial_state(
                        wellhead_id,
                        parameters,
                        controls,
                        at - timedelta(seconds=1),
                    )
                    process = step_model(current, controls, parameters, at)
                except (ArithmeticError, ValueError):
                    failures += 1
                    logger.error(
                        "Model step failed", extra={"wellhead_id": wellhead_id}
                    )
                    next_states[wellhead_id] = replace(
                        updated, status="degraded", reason="model_step_failed"
                    )
                    continue
                advanced += 1
                next_states[wellhead_id] = replace(
                    updated,
                    status="running",
                    reason="cold_start_no_snapshot",
                    process=process,
                )
            self._states = next_states
        return advanced, failures

    def record_gap(self, skipped: int) -> None:
        """Count unmodelled scheduler time without advancing process state."""
        if skipped < 0:
            raise ValueError("Skipped ticks cannot be negative")
        with self._lock:
            self._states = {
                wellhead_id: replace(state, skipped_ticks=state.skipped_ticks + skipped)
                for wellhead_id, state in self._states.items()
            }

    def get(self, wellhead_id: int) -> WellheadState | None:
        """Return immutable current state for an active wellhead."""
        with self._lock:
            return self._states.get(wellhead_id)

    def snapshot(self) -> tuple[WellheadState, ...]:
        """Return one consistent view of every active model state."""
        with self._lock:
            return tuple(self._states[key] for key in sorted(self._states))

    def count(self) -> int:
        """Return the number of active wellheads in memory."""
        with self._lock:
            return len(self._states)

    def running_count(self) -> int:
        """Return the number of wells with a current running model."""
        with self._lock:
            return sum(state.status == "running" for state in self._states.values())

    def invalid_count(self) -> int:
        """Return the number of wells with rejected model parameters."""
        with self._lock:
            return sum(
                state.reason == "invalid_parameters" for state in self._states.values()
            )
