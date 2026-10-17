"""Exact snapshot identity and compatibility for private historical scenarios."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from twin_core.model import ModelParameters, step_model
from twin_core.projection import ProjectionBase
from twin_core.recovery import decode_process, parameter_fingerprint, saved_controls

MAX_SNAPSHOT_AGE = timedelta(days=89)
SNAPSHOT_SEARCH_WINDOW = timedelta(days=1)
SNAPSHOT_PAGE_SIZE = 20


class SnapshotMissingError(ValueError):
    """The selected composite key does not exist for this well."""


class SnapshotConflictError(ValueError):
    """The saved state cannot run with this model and parameter set."""


def utc_instant(value: object) -> datetime:
    """Require an explicit UTC timestamp at the HTTP boundary."""
    if (
        not isinstance(value, str)
        or re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)",
            value,
        )
        is None
    ):
        raise ValueError("A UTC timestamp is required")
    instant = datetime.fromisoformat(value)
    if instant.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must use UTC")
    return instant


@dataclass(frozen=True)
class SnapshotSelection:
    """Both parts of a retained snapshot's primary key."""

    snapshot_id: str
    simulated_at: datetime

    @classmethod
    def from_payload(cls, value: object) -> SnapshotSelection:
        """Reject rounded numbers, extra keys and incomplete identities."""
        if not isinstance(value, dict) or set(value) != {"snapshotId", "simulatedAt"}:
            raise ValueError("Snapshot identity requires snapshotId and simulatedAt")
        identifier = value["snapshotId"]
        if (
            not isinstance(identifier, str)
            or re.fullmatch(r"[1-9][0-9]{0,18}", identifier) is None
            or int(identifier) > 9_223_372_036_854_775_807
        ):
            raise ValueError("Snapshot identifier must be a positive bigint string")
        return cls(identifier, utc_instant(value["simulatedAt"]))

    def to_payload(self) -> dict[str, str]:
        """Preserve every identifier digit and the exact UTC instant."""
        return {
            "snapshotId": self.snapshot_id,
            "simulatedAt": self.simulated_at.isoformat(),
        }


@dataclass(frozen=True)
class SnapshotCandidate:
    """Small finder metadata; full state is read only after selection."""

    selection: SnapshotSelection
    model_version: str
    parameter_fingerprint: str

    def reason(
        self, version: str, fingerprint: str
    ) -> Literal["compatible", "model_version_changed", "parameters_changed"]:
        """Explain whether installed equations and inputs match the saved state."""
        if self.model_version != version:
            return "model_version_changed"
        if self.parameter_fingerprint != fingerprint:
            return "parameters_changed"
        return "compatible"


def historical_base(
    wellhead_id: int,
    candidate: SnapshotCandidate,
    payload: object,
    parameters: ModelParameters,
    model_version: str,
    now: datetime,
) -> ProjectionBase:
    """Validate and copy the selected state without recovery toward wall time."""
    fingerprint = parameter_fingerprint(parameters)
    if candidate.reason(model_version, fingerprint) != "compatible":
        raise SnapshotConflictError("Snapshot model or parameters changed")
    at = candidate.selection.simulated_at
    if not now - MAX_SNAPSHOT_AGE <= at <= now:
        raise SnapshotConflictError("Snapshot is outside the supported age window")
    try:
        state = decode_process(payload)
        if state.simulated_at != at:
            raise ValueError("Snapshot payload and row timestamps disagree")
        controls = saved_controls(state)
        step_model(state, controls, parameters, at + timedelta(seconds=1))
    except (ValueError, ArithmeticError) as exc:
        raise SnapshotConflictError("Snapshot state is invalid") from exc
    return ProjectionBase(
        wellhead_id,
        model_version,
        fingerprint,
        state,
        controls,
        parameters,
        None,
        None,
        candidate.selection.snapshot_id,
    )
