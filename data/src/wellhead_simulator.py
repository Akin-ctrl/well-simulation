"""Generate synthetic wellhead telemetry from database metadata."""

from __future__ import annotations

import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2

from telemetry_common import (
    DatabaseSettings,
    configure_logging,
    connect_db,
    telemetry_interval_seconds,
)

EXCURSION_PROBABILITY = 0.1
EXCURSION_LOW_FACTOR = 0.8
EXCURSION_HIGH_FACTOR = 1.2
FLOAT_PRECISION = 2

# Telemetry is written to stdout for the gateway, so logs must go to stderr.
logger = configure_logging("wellhead_simulator", stream=sys.stderr)


@dataclass(frozen=True)
class SimulatedParameter:
    """One parameter to synthesise, with its configured normal range."""

    code: str
    normal_min: float
    normal_max: float
    data_type: str


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the simulator."""

    interval_seconds: int
    database: DatabaseSettings

    @classmethod
    def from_env(cls) -> Settings:
        """Load and validate service configuration."""
        return cls(
            interval_seconds=telemetry_interval_seconds(),
            database=DatabaseSettings.from_env(),
        )


_SIMULATION_METADATA_QUERY = """
SELECT wh.wellhead_id, pt.code, pt.normal_min, pt.normal_max, pt.data_type
FROM wellHead wh
JOIN device d ON wh.device_id = d.device_id
JOIN deviceParameterMapping dpm ON d.device_id = dpm.device_id
JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
WHERE wh.status = 'active' AND dpm.active = TRUE;
"""


def get_simulation_metadata(
    settings: DatabaseSettings,
) -> dict[int, list[SimulatedParameter]]:
    """Fetch active wellheads and their parameter ranges."""
    with connect_db(settings) as conn, conn.cursor() as cursor:
        cursor.execute(_SIMULATION_METADATA_QUERY)
        rows = cursor.fetchall()

    config: dict[int, list[SimulatedParameter]] = {}
    for wellhead_id, code, normal_min, normal_max, data_type in rows:
        config.setdefault(wellhead_id, []).append(
            SimulatedParameter(
                code=code,
                normal_min=normal_min,
                normal_max=normal_max,
                data_type=data_type,
            )
        )
    return config


def generate_parameter_value(parameter: SimulatedParameter) -> float | int:
    """Generate a synthetic value inside or near the configured normal range.

    Roughly one value in ten is pushed outside the normal range so alarm rules
    have something to fire on. These are illustrative values, not a physical
    model of the wellhead.
    """
    # The S311 suppressions below are deliberate: this is a telemetry
    # simulator, so randomness is cosmetic and carries no security weight.
    if random.random() < EXCURSION_PROBABILITY:  # noqa: S311
        value = random.uniform(  # noqa: S311
            parameter.normal_min * EXCURSION_LOW_FACTOR,
            parameter.normal_max * EXCURSION_HIGH_FACTOR,
        )
    else:
        value = random.uniform(parameter.normal_min, parameter.normal_max)  # noqa: S311

    if parameter.data_type == "float":
        return round(value, FLOAT_PRECISION)
    if parameter.data_type == "boolean":
        return random.choice([0, 1])  # noqa: S311
    return int(value)


def build_batch(config: dict[int, list[SimulatedParameter]]) -> list[dict[str, object]]:
    """Build one telemetry batch covering every configured wellhead."""
    timestamp = datetime.now(timezone.utc).isoformat()
    return [
        {
            "timestamp": timestamp,
            "wellhead_id": wellhead_id,
            "parameters": {
                parameter.code: generate_parameter_value(parameter)
                for parameter in parameters
            },
        }
        for wellhead_id, parameters in config.items()
    ]


def emit_telemetry(payload: list[dict[str, object]]) -> None:
    """Write one JSON telemetry batch to stdout for the Modbus gateway."""
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def run_simulation(
    settings: Settings, config: dict[int, list[SimulatedParameter]]
) -> None:
    """Run the synthetic telemetry loop.

    The parameter set is re-read each interval, so adding a wellhead or changing
    a normal range takes effect without a restart (ADR 0035).
    """
    logger.info(
        "Starting wellhead simulator",
        extra={
            "wellheads": len(config),
            "interval_seconds": settings.interval_seconds,
        },
    )
    while True:
        emit_telemetry(build_batch(config))
        time.sleep(settings.interval_seconds)

        try:
            reloaded = get_simulation_metadata(settings.database)
        except psycopg2.Error:
            logger.exception("Metadata reload failed; keeping the current parameters")
            continue

        if reloaded and reloaded != config:
            config = reloaded
            logger.info(
                "Simulation metadata reloaded", extra={"wellheads": len(config)}
            )


def main() -> int:
    """Start the wellhead simulator."""
    settings = Settings.from_env()

    try:
        config = get_simulation_metadata(settings.database)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while loading simulation metadata")
        return 1

    if not config:
        logger.error("No simulation metadata found in database")
        return 1

    # No operational endpoints here. The simulator runs as a child process of
    # the Modbus gateway, inside the same container and sharing its environment,
    # so binding a port would collide with the gateway's. Its liveness is
    # already covered: the gateway exits when the simulator does.
    run_simulation(settings, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
