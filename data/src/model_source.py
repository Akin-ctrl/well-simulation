"""Validate one complete twin-core fleet tick before it reaches Modbus."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit
from urllib.request import urlopen

from telemetry_common import HEARTBEAT_REGISTER, ParameterMapping

# The gateway owns the conversion from model fields to the existing Modbus map.
# Types and units are defined by parameterType metadata, not inferred here.
MODEL_SIGNAL_FIELDS = {
    "tubing_pressure": "tubingPressurePsi",
    "casing_pressure": "casingPressurePsi",
    "annulus_pressure": "annulusPressurePsi",
    "gas_oil_ratio": "gasOilRatioScfStb",
    "sand_detector": "sandDetectorPpm",
    "corrosion_rate": "corrosionRateMpy",
    "h2s_level": "h2sLevelPpm",
    "co2_level": "co2LevelPercent",
    "vibration": "vibrationMmS",
    "wellhead_temperature": "wellheadTemperatureF",
    "choke_valve_position": "chokePositionPercent",
    "flow_rate": "flowRateBpd",
    "water_cut": "waterCutPercent",
    "master_valve_status": "masterValveOpen",
    "wing_valve_status": "wingValveOpen",
    "swab_valve_status": "swabValveOpen",
    "emergency_shutdown": "emergencyShutdown",
    "pump_status": "pumpOn",
}
BOOLEAN_SIGNALS = frozenset(
    {
        "master_valve_status",
        "wing_valve_status",
        "swab_valve_status",
        "emergency_shutdown",
        "pump_status",
    }
)
MAX_SNAPSHOT_BYTES = 1_000_000


@dataclass(frozen=True)
class ModelBatch:
    """A checked model instant and all mapped readings at that instant."""

    simulated_at: datetime
    telemetry: list[dict[str, object]]


def supported_mappings(mappings: list[ParameterMapping]) -> list[ParameterMapping]:
    """Select dimensioned model signals and reject an unsafe database map."""
    supported: list[ParameterMapping] = []
    occupied: set[tuple[int, int]] = set()
    for mapping in mappings:
        if mapping.parameter_code not in MODEL_SIGNAL_FIELDS:
            continue
        expected_type = (
            "boolean"
            if mapping.parameter_code in BOOLEAN_SIGNALS
            else "integer"
            if mapping.parameter_code == "choke_valve_position"
            else "float"
        )
        if mapping.data_type != expected_type:
            raise ValueError(f"Model mapping type mismatch: {mapping.parameter_code}")
        if not 1 <= mapping.modbus_unit_id <= 247:
            raise ValueError("Invalid Modbus unit id")
        if not 0 <= mapping.modbus_register < HEARTBEAT_REGISTER - 1:
            raise ValueError("Model register overlaps the heartbeat or is out of range")
        for offset in (mapping.modbus_register, mapping.modbus_register + 1):
            address = (mapping.modbus_unit_id, offset)
            if address in occupied:
                raise ValueError("Model register mappings overlap")
            occupied.add(address)
        supported.append(mapping)
    return supported


def _object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _source_is_model(value: dict[str, Any]) -> bool:
    """Check the source declaration on a fleet or process object."""
    return (
        value.get("source") == "synthetic_reduced_order_model"
        and value.get("fieldCalibrated") is False
    )


def _process_time(process: dict[str, Any]) -> datetime:
    """Parse a model timestamp with a real UTC offset."""
    raw = process.get("simulatedAt")
    if not isinstance(raw, str):
        raise ValueError("Missing model time")
    try:
        at = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("Invalid model time") from exc
    offset = at.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("Model time must be UTC")
    return at


def _parameters(process: dict[str, Any]) -> dict[str, float | int]:
    """Convert only modeled fields into the canonical register units."""
    parameters: dict[str, float | int] = {}
    for code, field in MODEL_SIGNAL_FIELDS.items():
        value = process.get(field)
        if code in BOOLEAN_SIGNALS:
            if not isinstance(value, bool):
                raise ValueError(f"Invalid model value: {code}")
            parameters[code] = int(value)
        elif (
            isinstance(value, bool)
            or not isinstance(value, (float, int))
            or not math.isfinite(value)
        ):
            raise ValueError(f"Invalid model value: {code}")
        elif code == "choke_valve_position":
            if not 0 <= value <= 100:
                raise ValueError("Invalid choke position")
            parameters[code] = round(value)
        else:
            if code != "wellhead_temperature" and value < 0:
                raise ValueError(f"Negative model value: {code}")
            parameters[code] = float(value)
    return parameters


def _wellhead(item: object) -> tuple[int, datetime, dict[str, float | int]]:
    """Validate one running well and return its supported model values."""
    well = _object(item, "wellhead")
    wellhead_id = well.get("wellheadId")
    if type(wellhead_id) is not int or wellhead_id <= 0:
        raise ValueError("Invalid wellhead id")
    if well.get("status") != "running":
        raise ValueError("Model wellhead is not running")
    process = _object(well.get("process"), "process")
    if not _source_is_model(process):
        raise ValueError("Unexpected process source")
    return wellhead_id, _process_time(process), _parameters(process)


def parse_model_snapshot(
    payload: object, expected_ids: set[int], now: datetime, max_age_seconds: int
) -> ModelBatch:
    """Reject missing, stale, future, mixed-tick, or malformed model output."""
    root = _object(payload, "snapshot")
    if not _source_is_model(root):
        raise ValueError("Unexpected model source")
    if not isinstance(root.get("modelVersion"), str) or not root["modelVersion"]:
        raise ValueError("Missing model version")
    wellheads = root.get("wellheads")
    if not isinstance(wellheads, list) or not expected_ids:
        raise ValueError("Missing model fleet")
    telemetry: list[dict[str, object]] = []
    found: set[int] = set()
    common_time: datetime | None = None
    for item in wellheads:
        wellhead_id, simulated_at, parameters = _wellhead(item)
        if wellhead_id in found:
            raise ValueError("Duplicate wellhead id")
        found.add(wellhead_id)
        if common_time is not None and simulated_at != common_time:
            raise ValueError("Mixed model tick times")
        common_time = simulated_at
        if wellhead_id in expected_ids:
            telemetry.append({"wellhead_id": wellhead_id, "parameters": parameters})
    if not expected_ids.issubset(found) or common_time is None:
        raise ValueError("Model snapshot omits a mapped wellhead")
    age = (now - common_time).total_seconds()
    if age < -2 or age > max_age_seconds:
        raise ValueError("Model snapshot is stale or in the future")
    return ModelBatch(common_time, telemetry)


def fetch_model_snapshot(
    url: str, expected_ids: set[int], max_age_seconds: int
) -> ModelBatch:
    """Fetch one bounded internal HTTP response and validate it fully."""
    parsed = urlsplit(url)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path != "/model-snapshot"
    ):
        raise ValueError("Invalid twin-core snapshot URL")
    with urlopen(url, timeout=3) as response:  # noqa: S310
        if response.status != 200:
            raise ValueError("Twin-core snapshot request failed")
        body = response.read(MAX_SNAPSHOT_BYTES + 1)
    if len(body) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Twin-core snapshot is too large")
    return parse_model_snapshot(
        json.loads(body), expected_ids, datetime.now(timezone.utc), max_age_seconds
    )
