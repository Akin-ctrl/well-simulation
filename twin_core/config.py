"""Load and validate versioned model defaults and service settings."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

DEFAULTS_PATH = Path(__file__).with_name("model_defaults.json")


@dataclass(frozen=True)
class ParameterSpec:
    """One model parameter, its unit, and its permitted values."""

    key: str
    description: str
    unit: str
    default: float
    minimum: float
    maximum: float
    change_live: bool
    requires_restart: bool

    def validate(self, value: object) -> float:
        """Return a finite value inside this parameter's declared range."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{self.key} must be a number")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"{self.key} must be finite")
        if not self.minimum <= number <= self.maximum:
            raise ValueError(
                f"{self.key} must be between {self.minimum} and {self.maximum} "
                f"{self.unit}"
            )
        return number


@dataclass(frozen=True)
class ModelDefaults:
    """Versioned parameter definitions loaded from the repository."""

    version: str
    description: str
    parameters: Mapping[str, ParameterSpec]


@dataclass(frozen=True)
class ParameterOverride:
    """A per-well value with its stored unit and origin."""

    value: float
    unit: str
    source: str


class ParameterOverrideSource(Protocol):
    """Boundary for per-well overrides stored in the future model tables."""

    def load(
        self, wellhead_ids: Sequence[int], model_version: str
    ) -> Mapping[int, Mapping[str, ParameterOverride]]:
        """Return overrides keyed by wellhead id and parameter key."""


class NoDatabaseOverrides:
    """Use defaults until Milestone 7 adds the model parameter table."""

    def load(
        self, _wellhead_ids: Sequence[int], _model_version: str
    ) -> Mapping[int, Mapping[str, ParameterOverride]]:
        """Return no per-well overrides while no model table exists."""
        return {}


def _object(value: object, name: str) -> dict[str, object]:
    """Check that a JSON value is an object with string keys."""
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be an object with string keys")
    return cast(dict[str, object], value)


def _string(value: object, name: str) -> str:
    """Check a required, nonempty string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _number(value: object, name: str) -> float:
    """Check a finite JSON number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def load_defaults(path: Path = DEFAULTS_PATH) -> ModelDefaults:
    """Read the authoritative model defaults and reject malformed metadata."""
    document = _object(json.loads(path.read_text(encoding="utf-8")), "defaults")
    version = _string(document.get("modelVersion"), "modelVersion")
    description = _string(document.get("description"), "description")
    raw_parameters = _object(document.get("parameters"), "parameters")
    if not raw_parameters:
        raise ValueError("parameters must not be empty")

    parameters: dict[str, ParameterSpec] = {}
    for key, raw in raw_parameters.items():
        item = _object(raw, key)
        change_live = item.get("changeLive")
        requires_restart = item.get("requiresRestart")
        if not isinstance(change_live, bool):
            raise ValueError(f"{key}.changeLive must be a boolean")
        if not isinstance(requires_restart, bool):
            raise ValueError(f"{key}.requiresRestart must be a boolean")
        if change_live and requires_restart:
            raise ValueError(f"{key} cannot change live and require restart")
        spec = ParameterSpec(
            key=key,
            description=_string(item.get("description"), f"{key}.description"),
            unit=_string(item.get("unit"), f"{key}.unit"),
            default=_number(item.get("default"), f"{key}.default"),
            minimum=_number(item.get("minimum"), f"{key}.minimum"),
            maximum=_number(item.get("maximum"), f"{key}.maximum"),
            change_live=change_live,
            requires_restart=requires_restart,
        )
        if spec.minimum > spec.maximum:
            raise ValueError(f"{key} has a reversed range")
        spec.validate(spec.default)
        parameters[key] = spec

    return ModelDefaults(version, description, parameters)


def parse_environment_overrides(raw: str) -> Mapping[str, object]:
    """Parse optional global parameter overrides from JSON."""
    if not raw.strip():
        return {}
    return _object(json.loads(raw), "TWIN_MODEL_OVERRIDES_JSON")


def resolve_parameters(
    defaults: ModelDefaults,
    environment: Mapping[str, object],
    per_well: Mapping[str, ParameterOverride],
) -> dict[str, float]:
    """Apply global values, then per-well values with matching units."""
    resolved = {key: spec.default for key, spec in defaults.parameters.items()}
    for source in (environment, per_well):
        unknown = set(source) - set(defaults.parameters)
        if unknown:
            raise ValueError(f"Unknown model parameter: {sorted(unknown)[0]}")
    for key, value in environment.items():
        resolved[key] = defaults.parameters[key].validate(value)
    for key, override in per_well.items():
        spec = defaults.parameters[key]
        if override.unit != spec.unit:
            raise ValueError(f"{key} has the wrong unit")
        if not override.source.strip():
            raise ValueError(f"{key} must name its override source")
        resolved[key] = spec.validate(override.value)
    return resolved


def _required_env(name: str) -> str:
    """Read a required setting without including its value in errors."""
    value = os.getenv(name, "")
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _positive_int(name: str, default: str) -> int:
    """Read a positive integer setting."""
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True)
class ServiceSettings:
    """Settings needed to run the internal service and read the historian."""

    bind_host: str
    port: int
    max_catchup_steps: int
    fleet_refresh_seconds: int
    telemetry_freshness_seconds: int
    database_host: str
    database_port: int
    database_name: str
    database_user: str
    database_password: str
    environment_overrides: Mapping[str, object]

    @classmethod
    def from_env(cls) -> ServiceSettings:
        """Read service settings, failing before startup on invalid values."""
        telemetry_interval = _positive_int("TELEMETRY_INTERVAL_SECONDS", "5")
        return cls(
            # The listener stays on the private Compose network.
            bind_host=os.getenv("TWIN_BIND_HOST", "0.0.0.0"),  # noqa: S104
            port=_positive_int("TWIN_SERVICE_PORT", "8000"),
            max_catchup_steps=_positive_int("TWIN_MAX_CATCHUP_STEPS", "5"),
            fleet_refresh_seconds=_positive_int("TWIN_FLEET_REFRESH_SECONDS", "30"),
            telemetry_freshness_seconds=_positive_int(
                "TWIN_TELEMETRY_FRESHNESS_SECONDS", str(3 * telemetry_interval)
            ),
            database_host=os.getenv("POSTGRES_HOST", "db"),
            database_port=_positive_int("POSTGRES_PORT", "5432"),
            database_name=_required_env("POSTGRES_DB"),
            database_user=_required_env("TWIN_DB_USER"),
            database_password=_required_env("TWIN_DB_PASSWORD"),
            environment_overrides=parse_environment_overrides(
                os.getenv("TWIN_MODEL_OVERRIDES_JSON", "")
            ),
        )
