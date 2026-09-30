"""Verify model-to-Modbus conversion and the fail-closed snapshot boundary."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from model_source import parse_model_snapshot, supported_mappings
from telemetry_common import ParameterMapping

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def snapshot() -> dict[str, Any]:
    """Build a complete representative model fleet response."""
    process = {
        "source": "synthetic_reduced_order_model",
        "fieldCalibrated": False,
        "simulatedAt": NOW.isoformat(),
        "tubingPressurePsi": 1800.0,
        "casingPressurePsi": 1200.0,
        "annulusPressurePsi": 150.0,
        "gasOilRatioScfStb": 400.0,
        "sandDetectorPpm": 12.0,
        "corrosionRateMpy": 0.5,
        "h2sLevelPpm": 150.0,
        "co2LevelPercent": 3.0,
        "vibrationMmS": 0.8,
        "wellheadTemperatureF": 102.0,
        "chokePositionPercent": 52.5,
        "flowRateBpd": 1200.0,
        "waterCutPercent": 20.0,
        "masterValveOpen": True,
        "wingValveOpen": True,
        "swabValveOpen": False,
        "emergencyShutdown": False,
        "pumpOn": True,
    }
    return {
        "source": "synthetic_reduced_order_model",
        "fieldCalibrated": False,
        "modelVersion": "0.3.0",
        "wellheads": [{"wellheadId": 1, "status": "running", "process": process}],
    }


def test_complete_snapshot_maps_all_seeded_values() -> None:
    batch = parse_model_snapshot(snapshot(), {1}, NOW, 10)
    assert batch.simulated_at == NOW
    params = batch.telemetry[0]["parameters"]
    assert isinstance(params, dict)
    assert len(params) == 18
    assert params["choke_valve_position"] == 52
    assert params["master_valve_status"] == 1
    assert params["h2s_level"] == 150.0
    assert params["annulus_pressure"] == 150.0


@pytest.mark.parametrize(
    "fault", ["missing", "old", "future", "mixed", "nan", "unready"]
)
def test_rejects_incomplete_or_untrustworthy_model_ticks(fault: str) -> None:
    payload = snapshot()
    well = payload["wellheads"][0]
    process = well["process"]
    if fault == "missing":
        payload["wellheads"] = []
    elif fault == "old":
        process["simulatedAt"] = (NOW - timedelta(seconds=11)).isoformat()
    elif fault == "future":
        process["simulatedAt"] = (NOW + timedelta(seconds=3)).isoformat()
    elif fault == "mixed":
        other = deepcopy(well)
        other["wellheadId"] = 2
        other["process"]["simulatedAt"] = (NOW - timedelta(seconds=1)).isoformat()
        payload["wellheads"].append(other)
    elif fault == "nan":
        process["flowRateBpd"] = float("nan")
    else:
        well["status"] = "degraded"
    with pytest.raises(ValueError):
        parse_model_snapshot(payload, {1}, NOW, 10)


def test_mapping_rejects_wrong_types_and_includes_derived_signals() -> None:
    pressure = ParameterMapping(1, 1, 1, "tubing_pressure", 0, 1, "float")
    h2s = ParameterMapping(2, 1, 11, "h2s_level", 20, 1, "float")
    assert supported_mappings([pressure, h2s]) == [pressure, h2s]
    bad = ParameterMapping(3, 1, 1, "tubing_pressure", 0, 1, "integer")
    with pytest.raises(ValueError, match="type mismatch"):
        supported_mappings([bad])


def test_rejects_overlapping_or_reserved_registers() -> None:
    first = ParameterMapping(1, 1, 1, "tubing_pressure", 0, 1, "float")
    overlap = ParameterMapping(2, 1, 2, "casing_pressure", 1, 1, "float")
    reserved = ParameterMapping(3, 1, 2, "casing_pressure", 1900, 1, "float")
    with pytest.raises(ValueError, match="overlap"):
        supported_mappings([first, overlap])
    with pytest.raises(ValueError, match="heartbeat"):
        supported_mappings([reserved])


def test_source_timeout_clears_gateway_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An offline twin must not leave the gateway reporting a live source."""
    import modbus_gateway as gateway
    from telemetry_common import DatabaseSettings, MetadataReloader, ServiceHealth

    mapping = ParameterMapping(1, 1, 1, "tubing_pressure", 0, 1, "float")
    store = gateway.RegisterStore([mapping])
    database = DatabaseSettings("unused", 5432, "unused", "unused", "unused")
    reloader = MetadataReloader(database, 5, clock=lambda: 0.0)
    health = ServiceHealth(ready=True)
    settings = gateway.Settings(
        "127.0.0.1", 5020, database, "http://twin-core:8000/model-snapshot", 5
    )

    def timeout(*_args: object) -> None:
        health.alive = False
        raise TimeoutError("source offline")

    monkeypatch.setattr(gateway, "fetch_model_snapshot", timeout)
    monkeypatch.setattr("modbus_gateway.time.sleep", lambda _seconds: None)
    gateway.run_model_source(store, reloader, [mapping], health, settings)
    assert health.ready is False
    assert store.context[1].getValues(3, 1900, 2) == [0, 0]


def test_real_model_payload_covers_every_gateway_signal() -> None:
    """The model and gateway contracts must agree on all seeded signals."""
    from twin_core.config import load_defaults, resolve_parameters
    from twin_core.model import ControlInputs, ModelParameters, initial_state

    parameters = ModelParameters.from_values(
        resolve_parameters(load_defaults(), {}, {}, 1)
    )
    process = initial_state(1, parameters, ControlInputs(), NOW).to_payload()
    payload = {
        "source": "synthetic_reduced_order_model",
        "fieldCalibrated": False,
        "modelVersion": "0.3.0",
        "wellheads": [{"wellheadId": 1, "status": "running", "process": process}],
    }
    batch = parse_model_snapshot(payload, {1}, NOW, 10)
    values = batch.telemetry[0]["parameters"]
    assert isinstance(values, dict)
    assert len(values) == 18
    assert values["gas_oil_ratio"] == pytest.approx(process["gasOilRatioScfStb"])
    assert values["corrosion_rate"] == pytest.approx(process["corrosionRateMpy"])


@pytest.mark.parametrize(
    "field",
    [
        "annulusPressurePsi",
        "gasOilRatioScfStb",
        "sandDetectorPpm",
        "corrosionRateMpy",
        "h2sLevelPpm",
        "co2LevelPercent",
        "vibrationMmS",
    ],
)
def test_missing_diagnostic_rejects_entire_snapshot(field: str) -> None:
    payload = snapshot()
    del payload["wellheads"][0]["process"][field]
    with pytest.raises(ValueError, match="Invalid model value"):
        parse_model_snapshot(payload, {1}, NOW, 10)
