"""Generate synthetic wellhead telemetry from database metadata."""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("wellhead_simulator")


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    interval_seconds: int


def required_env(name: str, default: str | None = None) -> str:
    """Return an environment variable or raise a clear configuration error."""
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    """Load and validate service configuration."""
    return Settings(
        db_host=required_env("POSTGRES_HOST", "db"),
        db_port=int(required_env("POSTGRES_PORT", "5432")),
        db_name=required_env("POSTGRES_DB"),
        db_user=required_env("POSTGRES_USER"),
        db_password=required_env("POSTGRES_PASSWORD"),
        interval_seconds=int(required_env("SIMULATOR_INTERVAL_SECONDS", "30")),
    )


def connect_db(settings: Settings):
    """Open a PostgreSQL connection using service settings."""
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        dbname=settings.db_name,
    )


def get_simulation_metadata(settings: Settings) -> dict[int, list[dict[str, Any]]]:
    """Fetch active wellheads and parameter metadata from the database."""
    query = """
    SELECT wh.wellhead_id, pt.code, pt.normal_min, pt.normal_max, pt.data_type
    FROM wellHead wh
    JOIN device d ON wh.device_id = d.device_id
    JOIN deviceParameterMapping dpm ON d.device_id = dpm.device_id
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    WHERE wh.status = 'active' AND dpm.active = TRUE;
    """
    with connect_db(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    sim_config: dict[int, list[dict[str, Any]]] = {}
    for wellhead_id, param_code, min_value, max_value, data_type in rows:
        sim_config.setdefault(wellhead_id, []).append(
            {
                "code": param_code,
                "min": min_value,
                "max": max_value,
                "type": data_type,
            }
        )
    return sim_config


def generate_parameter_value(parameter: dict[str, Any]) -> float | int:
    """Generate a synthetic value inside or near the configured normal range."""
    min_value = parameter["min"]
    max_value = parameter["max"]
    if random.random() < 0.1:
        value = random.uniform(min_value * 0.8, max_value * 1.2)
    else:
        value = random.uniform(min_value, max_value)

    if parameter["type"] == "float":
        return round(value, 2)
    if parameter["type"] == "boolean":
        return random.choice([0, 1])
    return int(value)


def emit_telemetry(payload: list[dict[str, Any]]) -> None:
    """Write one JSON telemetry batch to stdout for the Modbus gateway."""
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def run_simulation(
    config: dict[int, list[dict[str, Any]]],
    interval_seconds: int,
) -> None:
    """Run the synthetic telemetry loop."""
    logger.info("Starting wellhead simulator with database metadata")
    while True:
        all_data = []
        for wellhead_id, parameters in config.items():
            data_point = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "wellhead_id": wellhead_id,
                "parameters": {
                    parameter["code"]: generate_parameter_value(parameter)
                    for parameter in parameters
                },
            }
            all_data.append(data_point)

        emit_telemetry(all_data)
        time.sleep(interval_seconds)


def main() -> None:
    """Start the wellhead simulator."""
    settings = load_settings()
    try:
        simulation_config = get_simulation_metadata(settings)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed while loading simulation metadata")
        return

    if not simulation_config:
        logger.error("No simulation metadata found in database")
        return

    run_simulation(simulation_config, settings.interval_seconds)


if __name__ == "__main__":
    main()
