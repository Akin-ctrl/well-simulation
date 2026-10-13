"""Projection persistence checks against an explicitly supplied disposable DB."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse
from uuid import uuid4

import psycopg2
import pytest
from twin_core.config import ServiceSettings, load_defaults, resolve_parameters
from twin_core.database import DatabaseUnavailableError
from twin_core.model import ControlInputs, ModelParameters, initial_state
from twin_core.persistence import TwinPersistence
from twin_core.projection import (
    ControlChange,
    ProjectionBase,
    ScheduledControl,
    project,
)
from twin_core.recovery import parameter_fingerprint


@pytest.fixture
def projection_database() -> tuple[TwinPersistence, str, str, int]:
    """Use only a database named by the opt-in integration test variable."""
    writer_dsn = os.getenv("TEST_PROJECTION_DATABASE_URL")
    if not writer_dsn:
        pytest.skip("TEST_PROJECTION_DATABASE_URL is not set")
    verify_dsn = os.getenv("TEST_PROJECTION_VERIFY_DATABASE_URL", writer_dsn)
    url = urlparse(writer_dsn)
    assert url.hostname and url.port and url.path and url.username
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host=url.hostname,
        database_port=url.port,
        database_name=url.path.lstrip("/"),
        database_user=unquote(url.username),
        database_password=unquote(url.password or ""),
        environment_overrides={},
    )
    with psycopg2.connect(writer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT min(wellhead_id) FROM wellhead")
        row = cur.fetchone()
        assert row is not None
        wellhead_id = int(row[0])
    return TwinPersistence(settings), writer_dsn, verify_dsn, wellhead_id


def test_forecast_and_scenario_commit_complete_runs_and_rollback_duplicates(
    projection_database: tuple[TwinPersistence, str, str, int],
) -> None:
    """Prove run metadata and sampled rows are one atomic database write."""
    store, writer_dsn, verify_dsn, wellhead_id = projection_database
    now = datetime.now(timezone.utc)
    defaults = load_defaults()
    parameters = ModelParameters.from_values(
        resolve_parameters(defaults, {}, {}, wellhead_id)
    )
    controls = ControlInputs()
    state = initial_state(wellhead_id, parameters, controls, now)
    base = ProjectionBase(
        wellhead_id,
        defaults.version,
        parameter_fingerprint(parameters),
        state,
        controls,
        parameters,
        None,
        None,
    )
    rules = store.load_alarm_rules()
    assert rules
    forecast = project("forecast", str(uuid4()), base, now, 300, 60, rules)
    scenario = project(
        "scenario",
        str(uuid4()),
        base,
        now,
        300,
        60,
        rules,
        name="Close choke",
        controls=(ScheduledControl(0, ControlChange(choke_position_percent=0)),),
    )
    store.write_projection(forecast, 1)
    store.write_projection(scenario, 1)
    with psycopg2.connect(verify_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(flow_rate_bpd) FROM prediction_point WHERE run_id=%s",
            (forecast.run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        count, flow = row
        assert count == 5
        assert flow == pytest.approx(
            min(point.values["flow_rate"] for point in forecast.points)
        )
        cur.execute(
            "SELECT count(*), min(baseline_alarm_risk) FROM scenario_point "
            "WHERE run_id=%s",
            (scenario.run_id,),
        )
        row = cur.fetchone()
        assert row is not None
        count, baseline_risk = row
        assert count == 5
        assert baseline_risk in ("none", "warning", "critical", "unknown")

    failed_id = str(uuid4())
    duplicate = replace(
        forecast, run_id=failed_id, points=(forecast.points[0], forecast.points[0])
    )
    with pytest.raises(DatabaseUnavailableError):
        store.write_projection(duplicate, 1)
    with psycopg2.connect(verify_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM prediction_run WHERE run_id=%s", (failed_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    if writer_dsn != verify_dsn:
        with psycopg2.connect(writer_dsn) as conn, conn.cursor() as cur:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute("SELECT count(*) FROM prediction_run")
            conn.rollback()
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(
                    "UPDATE prediction_run SET confidence_score = 0 WHERE run_id = %s",
                    (forecast.run_id,),
                )
