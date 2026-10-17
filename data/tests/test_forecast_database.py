"""Exercise claims and atomic results through the restricted model DB role."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from test_schema_database import database, installed, pytestmark
from twin_core.config import ServiceSettings, load_defaults
from twin_core.database import DatabaseUnavailableError
from twin_core.forecast_store import ForecastClaimError, ForecastStore
from twin_core.model import ControlInputs, ModelParameters, initial_state
from twin_core.persistence import TwinPersistence
from twin_core.projection import ProjectionBase, ProjectionResult, project
from twin_core.recovery import parameter_fingerprint

from schema import provision_twin_reader
from telemetry_common import DatabaseSettings

__all__ = ["database", "installed", "pytestmark"]


@pytest.fixture
def scheduled(
    installed: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[ForecastStore, TwinPersistence, int]]:
    role = "forecast_test_" + uuid4().hex
    password = uuid4().hex
    monkeypatch.setenv("TWIN_DB_USER", role)
    monkeypatch.setenv("TWIN_DB_PASSWORD", password)
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host=installed.info.host,
        database_port=installed.info.port,
        database_name=installed.info.dbname,
        database_user=role,
        database_password=password,
        environment_overrides={},
    )
    admin = DatabaseSettings(
        installed.info.host,
        installed.info.port,
        installed.info.dbname,
        installed.info.user,
        "unused",
    )
    # Provisioning uses the connected database name for the CONNECT grant.
    admin = replace(admin, name=installed.info.dbname)
    installed.autocommit = False
    provision_twin_reader(installed, admin)
    installed.autocommit = True
    with installed.cursor() as cur:
        cur.execute(
            "INSERT INTO users(email,user_name,encrypted_password,role) "
            "VALUES('scheduler@test.local','scheduler','unused','ADMIN') RETURNING id"
        )
        row = cur.fetchone()
        assert row is not None
        owner = row[0]
        cur.execute(
            "INSERT INTO forecast_schedule(schedule_id,wellhead_id,owner_user_id,"
            "owner_role,enabled,interval_seconds,horizon_seconds,step_seconds,"
            "revision,next_due_at,reason) VALUES(%s,1,%s,'ADMIN',true,300,300,60,1,"
            "clock_timestamp()-interval '30 days','Test schedule')",
            (str(uuid4()), owner),
        )
    try:
        yield ForecastStore(settings), TwinPersistence(settings), owner
    finally:
        with installed.cursor() as cur:
            cur.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def forecast() -> ProjectionResult:
    now = datetime.now(timezone.utc)
    defaults = load_defaults()
    parameters = ModelParameters.from_values(
        {key: value.default for key, value in defaults.parameters.items()}
    )
    controls = ControlInputs()
    base = ProjectionBase(
        1,
        defaults.version,
        parameter_fingerprint(parameters),
        initial_state(1, parameters, controls, now),
        controls,
        parameters,
        None,
        None,
    )
    return project("forecast", str(uuid4()), base, now, 300, 60, ())


def test_competing_claims_keep_inputs_after_schedule_edits(
    installed: psycopg2.extensions.connection,
    scheduled: tuple[ForecastStore, TwinPersistence, int],
) -> None:
    jobs, writer, owner = scheduled
    with ThreadPoolExecutor(max_workers=2) as pool:
        claimed = list(pool.map(lambda _: jobs.claim(), range(2)))
    actual = [job for job in claimed if job is not None]
    assert len(actual) == 1
    job = actual[0]
    assert job.missed_intervals >= 8639
    with installed.cursor() as cur:
        cur.execute(
            "UPDATE forecast_schedule SET enabled=false, next_due_at=NULL, "
            "horizon_seconds=600, revision=2 WHERE schedule_id=%s",
            (job.schedule_id,),
        )
    # Disabling affects future claims; this job keeps its copied five-minute input.
    result = replace(forecast(), scheduled_job=job)
    writer.write_projection(result, owner)
    assert jobs.fail(job, "uncertain_commit") == "complete"
    assert jobs.claim() is None
    with installed.cursor() as cur:
        cur.execute("SELECT enabled,horizon_seconds,revision FROM forecast_schedule")
        assert cur.fetchone() == (False, 600, 2)
        cur.execute(
            "SELECT status,run_id FROM forecast_job WHERE job_id=%s", (job.job_id,)
        )
        assert cur.fetchone() == ("complete", result.run_id)
        cur.execute(
            "SELECT count(*) FROM prediction_point WHERE run_id=%s", (result.run_id,)
        )
        assert cur.fetchone() == (5,)
        cur.execute(
            "SELECT base_state FROM prediction_run WHERE run_id=%s", (result.run_id,)
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0]["executionOrigin"]["jobId"] == job.job_id
    with pytest.raises(ForecastClaimError):
        writer.write_projection(replace(result, run_id=str(uuid4())), owner)


def test_duplicate_point_failure_rolls_back_result_and_completion(
    installed: psycopg2.extensions.connection,
    scheduled: tuple[ForecastStore, TwinPersistence, int],
) -> None:
    jobs, writer, owner = scheduled
    job = jobs.claim()
    assert job is not None
    result = forecast()
    result = replace(
        result, scheduled_job=job, points=(result.points[0], result.points[0])
    )
    with pytest.raises(DatabaseUnavailableError):
        writer.write_projection(result, owner)
    with installed.cursor() as cur:
        cur.execute("SELECT status FROM forecast_job WHERE job_id=%s", (job.job_id,))
        assert cur.fetchone() == ("running",)
        cur.execute(
            "SELECT count(*) FROM prediction_run WHERE run_id=%s", (result.run_id,)
        )
        assert cur.fetchone() == (0,)


@pytest.mark.parametrize("when", ["before_claim", "before_save"])
def test_revocation_blocks_work_and_disables_future_claims(
    installed: psycopg2.extensions.connection,
    scheduled: tuple[ForecastStore, TwinPersistence, int],
    when: str,
) -> None:
    jobs, writer, owner = scheduled
    job = jobs.claim() if when == "before_save" else None
    with installed.cursor() as cur:
        cur.execute("UPDATE users SET role='USER' WHERE id=%s", (owner,))
    if when == "before_claim":
        job = jobs.claim()
        assert job is not None and job.status == "skipped"
    else:
        assert job is not None
        with pytest.raises(ForecastClaimError, match="authority_revoked"):
            writer.write_projection(replace(forecast(), scheduled_job=job), owner)
        assert jobs.fail(job, "authority_revoked") == "failed"
    with installed.cursor() as cur:
        cur.execute("SELECT enabled,next_due_at FROM forecast_schedule")
        assert cur.fetchone() == (False, None)
        cur.execute("SELECT count(*) FROM prediction_run")
        assert cur.fetchone() == (0,)


def test_expired_and_mismatched_claims_cannot_save(
    installed: psycopg2.extensions.connection,
    scheduled: tuple[ForecastStore, TwinPersistence, int],
) -> None:
    jobs, writer, owner = scheduled
    job = jobs.claim()
    assert job is not None
    with pytest.raises(ForecastClaimError):
        writer.write_projection(
            replace(forecast(), scheduled_job=replace(job, horizon_seconds=600)), owner
        )
    with installed.cursor() as cur:
        cur.execute(
            "UPDATE forecast_job SET started_at=clock_timestamp()-interval '2 minutes',"
            "lease_expires_at=clock_timestamp()-interval '1 minute' WHERE job_id=%s",
            (job.job_id,),
        )
    with pytest.raises(ForecastClaimError):
        writer.write_projection(replace(forecast(), scheduled_job=job), owner)
    assert jobs.expire() == 1
    assert jobs.expire() == 0
    assert jobs.claim() is None
    with installed.cursor() as cur:
        cur.execute("SELECT status,reason_code FROM forecast_job")
        assert cur.fetchone() == ("failed", "worker_interrupted")


def test_expiry_during_result_insert_rolls_back_points_and_job(
    installed: psycopg2.extensions.connection,
    scheduled: tuple[ForecastStore, TwinPersistence, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from twin_core.forecast_job import ScheduledForecastJob
    from twin_core.forecast_store import complete_job

    jobs, writer, owner = scheduled
    job = jobs.claim()
    assert job is not None
    result = replace(forecast(), scheduled_job=job)

    with installed.cursor() as cur:
        cur.execute(
            "UPDATE forecast_job SET lease_expires_at=clock_timestamp() "
            "+ interval '1 second' WHERE job_id=%s",
            (job.job_id,),
        )
    reached_completion = []

    def expire_before_completion(
        cursor: psycopg2.extensions.cursor,
        claimed: ScheduledForecastJob,
        run_id: str,
    ) -> None:
        reached_completion.append(True)
        # The restricted worker cannot change its lease. Let the database clock
        # cross the short fixture deadline after all points were inserted.
        cursor.execute(
            "SELECT pg_sleep(greatest(0, extract(epoch FROM "
            "lease_expires_at-clock_timestamp())) + 0.02) "
            "FROM forecast_job WHERE job_id=%s",
            (claimed.job_id,),
        )
        complete_job(cursor, claimed, run_id)

    monkeypatch.setattr("twin_core.persistence.complete_job", expire_before_completion)
    with pytest.raises(ForecastClaimError, match="claim_expired"):
        writer.write_projection(result, owner)
    assert reached_completion == [True]
    with installed.cursor() as cur:
        cur.execute("SELECT status FROM forecast_job WHERE job_id=%s", (job.job_id,))
        assert cur.fetchone() == ("running",)
        cur.execute(
            "SELECT count(*) FROM prediction_point WHERE run_id=%s", (result.run_id,)
        )
        assert cur.fetchone() == (0,)
        cur.execute(
            "SELECT count(*) FROM prediction_run WHERE run_id=%s", (result.run_id,)
        )
        assert cur.fetchone() == (0,)
