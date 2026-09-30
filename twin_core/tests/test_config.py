"""Tests for versioned parameter metadata and override precedence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from twin_core.config import ParameterOverride, load_defaults, resolve_parameters


def test_all_synthetic_defaults_load_with_valid_ranges() -> None:
    defaults = load_defaults()
    assert defaults.version == "0.2.0"
    assert len(defaults.parameters) == 21
    assert defaults.parameters["ambient_temperature"].unit == "degrees F"
    for spec in defaults.parameters.values():
        assert spec.minimum <= spec.default <= spec.maximum
        assert spec.change_live is False
        assert spec.requires_restart is True


def test_per_well_override_wins_after_environment_override() -> None:
    defaults = load_defaults()
    values = resolve_parameters(
        defaults,
        {"downstream_pressure": 300},
        {"downstream_pressure": ParameterOverride(350, "psi", "database")},
    )
    assert values["downstream_pressure"] == 350
    assert values["reservoir_pressure_proxy"] == 3200


@pytest.mark.parametrize(
    "override",
    [
        {"unknown_parameter": 1},
        {"downstream_pressure": float("nan")},
        {"downstream_pressure": float("inf")},
        {"downstream_pressure": True},
        {"downstream_pressure": 1001},
    ],
)
def test_bad_override_is_rejected(override: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        resolve_parameters(load_defaults(), override, {})


def test_per_well_unit_mismatch_is_rejected() -> None:
    with pytest.raises(ValueError, match="wrong unit"):
        resolve_parameters(
            load_defaults(),
            {},
            {"downstream_pressure": ParameterOverride(350, "bar", "database")},
        )


def test_invalid_versioned_default_is_rejected(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "model_defaults.json"
    document = json.loads(source.read_text(encoding="utf-8"))
    document["parameters"]["max_flow_rate"]["default"] = 50001
    path = tmp_path / "defaults.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="max_flow_rate"):
        load_defaults(path)


def test_per_well_variation_is_stable_and_overrides_win() -> None:
    defaults = load_defaults()
    first = resolve_parameters(defaults, {}, {}, 7)
    assert first == resolve_parameters(defaults, {}, {}, 7)
    assert (
        first["reservoir_pressure_proxy"]
        != resolve_parameters(defaults, {}, {}, 8)["reservoir_pressure_proxy"]
    )
    assert 0.92 * 3200 <= first["reservoir_pressure_proxy"] <= 1.08 * 3200
    assert 0.85 * 1.2 <= first["productivity_index"] <= 1.15 * 1.2
    assert 0.90 * 18 <= first["choke_coefficient"] <= 1.10 * 18
    overridden = resolve_parameters(
        defaults,
        {"reservoir_pressure_proxy": 3300},
        {"reservoir_pressure_proxy": ParameterOverride(3400, "psi", "database")},
        7,
    )
    assert overridden["reservoir_pressure_proxy"] == 3400
