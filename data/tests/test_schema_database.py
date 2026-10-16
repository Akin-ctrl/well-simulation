"""Schema lifecycle tests confined to databases created and owned by this suite."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import closing
from dataclasses import replace
from io import StringIO
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import pytest
from psycopg2 import sql

from schema import CONTRACT_PATH, SCHEMA_PATH, SEEDS_DIR
from schema_catalog import read_catalog
from schema_engine import accept_engine
from schema_install import (
    LEDGER_DDL,
    SchemaContract,
    adopt,
    apply_seeds,
    check_installed,
    has_table,
    install_fresh,
    installer_lock,
    verify_history,
    verify_structure,
)
from schema_sql import SqlFile, discover

pytestmark = pytest.mark.skipif(
    os.getenv("SCHEMA_DATABASE_TEST") != "1",
    reason="Set SCHEMA_DATABASE_TEST=1 to create disposable schema test databases",
)
SOURCE = SqlFile.load(SCHEMA_PATH)
CONTRACT = SchemaContract.load(CONTRACT_PATH, SOURCE)
SEEDS = discover(SEEDS_DIR)


def connect_test_database(name: str) -> psycopg2.extensions.connection:
    """Use test server settings, with a database name owned by this suite."""
    return psycopg2.connect(
        host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
        port=os.getenv("TEST_POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=name,
    )


@pytest.fixture
def database() -> Iterator[psycopg2.extensions.connection]:
    # The database name never comes from POSTGRES_DB or another external target.
    name = "well_schema_test_" + uuid4().hex
    with closing(connect_test_database("postgres")) as maintenance:
        maintenance.autocommit = True
        with maintenance.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with (
                closing(connect_test_database(name)) as conn,
                installer_lock(conn),
            ):
                yield conn
        finally:
            with maintenance.cursor() as cursor:
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


@pytest.fixture
def installed(
    database: psycopg2.extensions.connection,
) -> psycopg2.extensions.connection:
    install_fresh(database, SOURCE, CONTRACT)
    apply_seeds(database, SEEDS)
    return database


@pytest.fixture
def legacy(installed: psycopg2.extensions.connection) -> psycopg2.extensions.connection:
    # The restored historical database is also verified during release. Here
    # the catalog is identical, so only the pre-adoption bookkeeping differs.
    with installed.cursor() as cursor:
        cursor.execute("DROP TABLE schema_installation")
        cursor.executemany(
            "INSERT INTO schema_migrations (filename, checksum) VALUES (%s,%s)",
            list(CONTRACT.legacy_checksums.items()),
        )
    return installed


def test_fresh_schema_matches_generated_reference(
    installed: psycopg2.extensions.connection,
) -> None:
    assert read_catalog(installed) == CONTRACT.catalog
    check_installed(installed, CONTRACT, SEEDS)
    assert apply_seeds(installed, SEEDS) == 0
    with installed.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM wellhead")
        assert cursor.fetchone() == (12,)


def test_adoption_preserves_rows_objects_and_history(
    legacy: psycopg2.extensions.connection,
) -> None:
    with legacy.cursor() as cursor:
        cursor.execute(
            "UPDATE wellhead SET name='preserved-test-well' WHERE wellhead_id=1"
        )
        cursor.execute("SELECT * FROM schema_migrations ORDER BY filename")
        history = cursor.fetchall()
        cursor.execute(
            "SELECT 'users'::regclass::oid, 'parameterreading'::regclass::oid"
        )
        identities = cursor.fetchone()
    adopt(legacy, SOURCE, CONTRACT, SEEDS)
    adopt(legacy, SOURCE, CONTRACT, SEEDS)
    with legacy.cursor() as cursor:
        cursor.execute("SELECT * FROM schema_migrations ORDER BY filename")
        assert cursor.fetchall() == history
        cursor.execute(
            "SELECT 'users'::regclass::oid, 'parameterreading'::regclass::oid"
        )
        assert cursor.fetchone() == identities
        cursor.execute("SELECT name FROM wellhead WHERE wellhead_id=1")
        assert cursor.fetchone() == ("preserved-test-well",)
    check_installed(legacy, CONTRACT, SEEDS)


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE schema_migrations SET checksum='wrong' "
        "WHERE filename='0001_core_schema.sql'",
        "DELETE FROM schema_migrations WHERE filename='0001_core_schema.sql'",
        "INSERT INTO schema_migrations VALUES ('unknown.sql','unknown',now())",
        "ALTER TABLE users ALTER COLUMN session_version SET DEFAULT 9",
    ],
)
def test_bad_legacy_state_is_refused_without_adoption(
    legacy: psycopg2.extensions.connection,
    mutation: str,
) -> None:
    with legacy.cursor() as cursor:
        cursor.execute(mutation)
    with pytest.raises(RuntimeError):
        adopt(legacy, SOURCE, CONTRACT, SEEDS)
    assert not has_table(legacy, "schema_installation")


@pytest.mark.parametrize(
    ("mutation", "section"),
    [
        ("ALTER TABLE users ALTER COLUMN session_version SET DEFAULT 9", "columns"),
        ("ALTER TABLE users ADD CHECK (session_version>=0)", "constraints"),
        ("DROP INDEX ix_control_inbox", "indexes"),
        (
            "ALTER TABLE control_command DISABLE TRIGGER enforce_control_lifecycle",
            "triggers",
        ),
        ("ALTER FUNCTION lock_control_state(integer) SECURITY INVOKER", "functions"),
        (
            "GRANT EXECUTE ON FUNCTION lock_control_state(integer) TO PUBLIC",
            "functions",
        ),
        (
            "ALTER DOMAIN model_version_text DROP CONSTRAINT model_version_text_check",
            "types",
        ),
        ("ALTER TABLE users ENABLE ROW LEVEL SECURITY", "relations"),
        (
            "SELECT set_chunk_time_interval('parameterreading', INTERVAL '2 days')",
            "dimensions",
        ),
        (
            "SELECT alter_job(job_id, schedule_interval=>INTERVAL '9 days') "
            "FROM timescaledb_information.jobs "
            "WHERE hypertable_name='parameterreading'",
            "jobs",
        ),
        (
            "ALTER TABLE parameterreading "
            "SET (timescaledb.compress_orderby='timestamp_utc ASC')",
            "compression",
        ),
        (
            "ALTER MATERIALIZED VIEW mv_2min_pressure_trends "
            "SET (timescaledb.materialized_only=false)",
            "aggregates",
        ),
    ],
)
def test_actual_drift_is_detected_despite_matching_source_checksum(
    installed: psycopg2.extensions.connection,
    mutation: str,
    section: str,
) -> None:
    with installed.cursor() as cursor:
        cursor.execute(mutation)
    with pytest.raises(RuntimeError, match=section):
        check_installed(installed, CONTRACT, SEEDS)


def test_check_works_in_read_only_session(
    installed: psycopg2.extensions.connection,
) -> None:
    with installed.cursor() as cursor:
        cursor.execute("SET default_transaction_read_only=on")
    check_installed(installed, CONTRACT, SEEDS)


def test_legacy_invalidation_target_is_checked_when_supported(
    installed: psycopg2.extensions.connection,
) -> None:
    with installed.cursor() as cursor:
        cursor.execute(
            "SELECT to_regprocedure("
            "'_timescaledb_functions.continuous_agg_invalidation_trigger()')"
        )
        supported = cursor.fetchone() != (None,)
        if not supported:
            # New TimescaleDB releases remove this internal trigger. The
            # generated target contract must not keep requiring it.
            assert not any(
                "ts_cagg_invalidation_trigger" in row
                for row in CONTRACT.catalog["triggers"]
            )
            check_installed(installed, CONTRACT, SEEDS)
            return
        cursor.execute(
            "DROP TRIGGER ts_cagg_invalidation_trigger ON parameterreading; "
            "CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE "
            "OR UPDATE ON parameterreading FOR EACH ROW EXECUTE FUNCTION "
            "_timescaledb_functions.continuous_agg_invalidation_trigger('2')"
        )
    with pytest.raises(RuntimeError, match="triggers"):
        check_installed(installed, CONTRACT, SEEDS)


def test_concurrent_installer_has_bounded_wait(
    database: psycopg2.extensions.connection,
) -> None:
    with (
        closing(connect_test_database(database.info.dbname)) as other,
        pytest.raises(RuntimeError, match="holds the database lock"),
        installer_lock(other, timeout_seconds=0.02),
    ):
        pytest.fail("Second installer acquired the lock")


def test_interrupted_install_cannot_be_mistaken_for_ready(
    database: psycopg2.extensions.connection,
) -> None:
    broken = replace(SOURCE, sql=SOURCE.sql + "\nSELECT missing_schema_function();")
    with pytest.raises(psycopg2.Error):
        install_fresh(database, broken, CONTRACT)
    assert has_table(database, "schema_installation")
    with pytest.raises(RuntimeError, match="incomplete"):
        check_installed(database, CONTRACT, SEEDS)
    with pytest.raises(RuntimeError, match="not empty"):
        install_fresh(database, SOURCE, CONTRACT)


def test_unknown_version_is_rejected_before_install(
    database: psycopg2.extensions.connection,
) -> None:
    wrong = replace(CONTRACT, versions={"postgres_major": "0", "timescaledb": "0"})
    with pytest.raises(RuntimeError, match="versions differ"):
        install_fresh(database, SOURCE, wrong)
    assert not has_table(database, "schema_installation")


def test_changed_baseline_requires_conversion(
    installed: psycopg2.extensions.connection,
) -> None:
    with pytest.raises(RuntimeError, match="reviewed conversion"):
        check_installed(installed, replace(CONTRACT, source_checksum="changed"), SEEDS)


def test_empty_database_cannot_be_adopted(
    database: psycopg2.extensions.connection,
) -> None:
    with pytest.raises(RuntimeError, match="history is missing"):
        adopt(database, SOURCE, CONTRACT, SEEDS)
    assert not has_table(database, "schema_installation")


def test_history_alone_does_not_prove_structure(
    database: psycopg2.extensions.connection,
) -> None:
    with database.cursor() as cursor:
        cursor.execute(LEDGER_DDL)
        cursor.executemany(
            "INSERT INTO schema_migrations (filename,checksum) VALUES (%s,%s)",
            list(CONTRACT.legacy_checksums.items()),
        )
    verify_history(database, CONTRACT, SEEDS, require_legacy=True)
    with pytest.raises(RuntimeError, match="Schema differs"):
        verify_structure(database, CONTRACT)


def test_generated_timescale_ids_do_not_change_the_contract(
    database: psycopg2.extensions.connection,
) -> None:
    with database.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        cursor.execute("CREATE TABLE schema_id_probe (sampled_at TIMESTAMPTZ)")
        cursor.execute("SELECT create_hypertable('schema_id_probe', 'sampled_at')")
        cursor.execute("DROP TABLE schema_id_probe")
    install_fresh(database, SOURCE, CONTRACT)
    assert read_catalog(database) == CONTRACT.catalog


def test_failed_adoption_rolls_back_baseline_record(
    legacy: psycopg2.extensions.connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_completion(
        conn: psycopg2.extensions.connection,
        checksum: str,
    ) -> None:
        del conn, checksum
        raise RuntimeError("Injected completion failure")

    monkeypatch.setattr("schema_install._mark_ready", fail_completion)
    with pytest.raises(RuntimeError, match="completion failure"):
        adopt(legacy, SOURCE, CONTRACT, SEEDS)
    assert not has_table(legacy, "schema_installation")
    verify_history(legacy, CONTRACT, SEEDS, require_legacy=True)


@pytest.fixture
def previous_engine(installed: psycopg2.extensions.connection) -> SchemaContract:
    # Exercise acceptance against the real target catalog. The release rehearsal
    # separately uses an actual pg_upgrade and the saved pre-upgrade reference.
    previous = replace(
        CONTRACT, versions={"postgres_major": "14", "timescaledb": "2.19.2"}
    )
    with installed.cursor() as cursor:
        cursor.execute(
            "UPDATE schema_installation SET contract_checksum=%s",
            (previous.checksum,),
        )
    return previous


def test_engine_acceptance_preserves_data_and_audits_once(
    installed: psycopg2.extensions.connection, previous_engine: SchemaContract
) -> None:
    with installed.cursor() as cursor:
        cursor.execute("SELECT * FROM users ORDER BY id")
        accounts = cursor.fetchall()
        cursor.execute("SELECT * FROM schema_migrations ORDER BY filename")
        history = cursor.fetchall()
    accept_engine(installed, previous_engine, CONTRACT, SEEDS)
    accept_engine(installed, previous_engine, CONTRACT, SEEDS)
    check_installed(installed, CONTRACT, SEEDS)
    with installed.cursor() as cursor:
        cursor.execute("SELECT * FROM users ORDER BY id")
        assert cursor.fetchall() == accounts
        cursor.execute("SELECT * FROM schema_migrations ORDER BY filename")
        assert cursor.fetchall() == history
        cursor.execute(
            "SELECT actor_identifier=current_user, detail FROM auditevent "
            "WHERE action='schema.engine.accepted'"
        )
        events = cursor.fetchall()
    assert len(events) == 1
    assert events[0][0] is True
    assert events[0][1]["previous_contract"] == previous_engine.checksum
    assert events[0][1]["target_versions"] == CONTRACT.versions


@pytest.mark.parametrize(
    "mutation",
    [
        "ALTER TABLE users ALTER COLUMN session_version SET DEFAULT 9",
        "UPDATE schema_installation SET contract_checksum='unexpected'",
        "UPDATE schema_installation SET status='installing'",
        "UPDATE schema_migrations SET checksum='changed'",
    ],
)
def test_engine_acceptance_rejects_drift_without_changing_identity(
    installed: psycopg2.extensions.connection,
    previous_engine: SchemaContract,
    mutation: str,
) -> None:
    with installed.cursor() as cursor:
        cursor.execute(mutation)
        cursor.execute("SELECT * FROM schema_installation")
        baseline = cursor.fetchall()
    with pytest.raises(RuntimeError):
        accept_engine(installed, previous_engine, CONTRACT, SEEDS)
    with installed.cursor() as cursor:
        cursor.execute("SELECT * FROM schema_installation")
        assert cursor.fetchall() == baseline
        cursor.execute(
            "SELECT count(*) FROM auditevent WHERE action='schema.engine.accepted'"
        )
        assert cursor.fetchone() == (0,)


def test_engine_acceptance_rolls_back_if_audit_write_fails(
    installed: psycopg2.extensions.connection, previous_engine: SchemaContract
) -> None:
    # Exhausting this sequence leaves the catalog definition unchanged but makes
    # the actual audit insert fail, exercising the transaction's rollback.
    with installed.cursor() as cursor:
        cursor.execute(
            "SELECT setval('auditevent_audit_event_id_seq', 9223372036854775807)"
        )
    with pytest.raises(psycopg2.Error):
        accept_engine(installed, previous_engine, CONTRACT, SEEDS)
    with installed.cursor() as cursor:
        cursor.execute("SELECT contract_checksum FROM schema_installation")
        assert cursor.fetchone() == (previous_engine.checksum,)


@pytest.mark.parametrize("changed", ["source", "history", "versions"])
def test_engine_acceptance_rejects_unrelated_reference_changes(
    installed: psycopg2.extensions.connection,
    previous_engine: SchemaContract,
    changed: str,
) -> None:
    candidate = {
        "source": replace(previous_engine, source_checksum="different"),
        "history": replace(previous_engine, legacy_checksums={}),
        "versions": replace(previous_engine, versions=CONTRACT.versions),
    }[changed]
    with pytest.raises(RuntimeError):
        accept_engine(installed, candidate, CONTRACT, SEEDS)


def test_compressed_null_backup_value_can_be_restored(
    database: psycopg2.extensions.connection,
) -> None:
    # TimescaleDB 2.19.3 emits this valid all-null compressed batch in pg_dump,
    # then fails to decode it during COPY. This is the observed restore defect
    # in upstream issue 8893, independently reproduced during the upgrade.
    with database.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        cursor.execute(
            "CREATE TEMP TABLE null_batch_probe "
            "(value _timescaledb_internal.compressed_data)"
        )
        cursor.copy_expert("COPY null_batch_probe FROM STDIN", StringIO("Bg==\n"))
        cursor.execute("SELECT value::text FROM null_batch_probe")
        assert cursor.fetchall() == [("Bg==",)]
