"""Projection persistence checks with fresh schemas and test-owned snapshots."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import pytest
from test_schema_database import pytestmark
from twin_core.config import ServiceSettings, load_defaults, resolve_parameters
from twin_core.database import DatabaseUnavailableError
from twin_core.historical import historical_base
from twin_core.model import ControlInputs, ModelParameters, initial_state
from twin_core.persistence import TwinPersistence
from twin_core.projection import (
    ControlChange,
    ProjectionBase,
    ScheduledControl,
    project,
)
from twin_core.recovery import parameter_fingerprint
from twin_core.state import WellheadState

__all__ = ["pytestmark"]

ProjectionDatabase = tuple[
    TwinPersistence, ServiceSettings, psycopg2.extensions.connection, int
]


@pytest.fixture
def projection_database(
    installed: psycopg2.extensions.connection, model_settings: ServiceSettings
) -> ProjectionDatabase:
    """Use the schema and restricted role created for this test alone."""
    with installed.cursor() as cur:
        cur.execute("SELECT min(wellhead_id) FROM wellhead")
        row = cur.fetchone()
        assert row is not None
        wellhead_id = int(row[0])
    return TwinPersistence(model_settings), model_settings, installed, wellhead_id


def test_forecast_and_scenario_commit_complete_runs_and_rollback_duplicates(
    projection_database: ProjectionDatabase,
) -> None:
    """Prove run metadata and sampled rows are one atomic database write."""
    store, settings, admin, wellhead_id = projection_database
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
    with admin.cursor() as cur:
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
    with admin.cursor() as cur:
        cur.execute("SELECT count(*) FROM prediction_run WHERE run_id=%s", (failed_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    with (
        closing(
            psycopg2.connect(
                host=settings.database_host,
                port=settings.database_port,
                dbname=settings.database_name,
                user=settings.database_user,
                password=settings.database_password,
            )
        ) as conn,
        conn.cursor() as cur,
    ):
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT count(*) FROM prediction_run")
        conn.rollback()
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(
                "UPDATE prediction_run SET confidence_score = 0 WHERE run_id = %s",
                (forecast.run_id,),
            )


def test_historical_branch_reads_exact_source_and_preserves_it(
    projection_database: ProjectionDatabase,
) -> None:
    """Use a real retained source and the restricted model role for the branch."""
    store, _, admin, wellhead_id = projection_database
    now = datetime.now(timezone.utc)
    defaults = load_defaults()
    parameters = ModelParameters.from_values(
        resolve_parameters(defaults, {}, {}, wellhead_id)
    )
    saved_at = now - timedelta(minutes=1)
    fingerprint = parameter_fingerprint(parameters)
    well = WellheadState(
        wellhead_id,
        defaults.version,
        "running",
        "test_snapshot",
        process=initial_state(wellhead_id, parameters, ControlInputs(), saved_at),
    )
    assert store.write_snapshots(((well, fingerprint),)) == 1
    candidates = store.find_snapshots(wellhead_id, saved_at, now)
    assert len(candidates) == 1
    matching = candidates[0]
    assert matching.parameter_fingerprint == fingerprint
    assert matching.selection.simulated_at == saved_at
    source = store.selected_snapshot(wellhead_id, matching.selection)
    assert source is not None
    assert store.selected_snapshot(wellhead_id + 10000, matching.selection) is None
    assert (
        store.selected_snapshot(
            wellhead_id,
            replace(
                matching.selection,
                simulated_at=matching.selection.simulated_at
                + timedelta(microseconds=1),
            ),
        )
        is None
    )
    base = historical_base(
        wellhead_id, source[0], source[1], parameters, defaults.version, now
    )
    result = project(
        "scenario",
        str(uuid4()),
        base,
        now,
        300,
        60,
        store.load_alarm_rules(),
        name="Historical database check",
        controls=(ScheduledControl(0, ControlChange(pump_on=False)),),
    )
    try:
        store.write_projection(result, 1)
        assert store.selected_snapshot(wellhead_id, matching.selection) == source
        with admin.cursor() as cur:
            cur.execute(
                "SELECT base_state, control_values FROM scenario_run WHERE run_id=%s",
                (result.run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert (
                row[0]["branchOrigin"]["snapshotId"] == matching.selection.snapshot_id
            )
            assert (
                row[1]["choke_position_percent"] == base.controls.choke_position_percent
            )
            cur.execute(
                "SELECT count(*), min(forecast_at), max(forecast_at) "
                "FROM scenario_point WHERE run_id=%s",
                (result.run_id,),
            )
            assert cur.fetchone() == (
                5,
                matching.selection.simulated_at + timedelta(seconds=60),
                matching.selection.simulated_at + timedelta(seconds=300),
            )
    finally:
        with admin.cursor() as cur:
            cur.execute("DELETE FROM scenario_point WHERE run_id=%s", (result.run_id,))
            cur.execute("DELETE FROM scenario_run WHERE run_id=%s", (result.run_id,))
