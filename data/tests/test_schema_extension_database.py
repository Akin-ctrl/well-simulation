"""Prove additive schema installation preserves data and rolls back on failure."""

from __future__ import annotations

import hashlib
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from test_schema_database import (
    CONTRACT,
    SEEDS,
    SOURCE,
    connect_test_database,
    database,
    installed,
    pytestmark,
)

from schema_catalog import read_catalog
from schema_extend import extend_schema
from schema_install import (
    SchemaContract,
    check_installed,
    install_fresh,
    installer_lock,
)
from schema_sql import SqlFile

# Reuse the opt-in, suite-owned database fixtures, never an application database.
__all__ = ["database", "installed", "pytestmark"]


@pytest.fixture
def extension() -> tuple[SqlFile, SchemaContract]:
    text = SOURCE.sql + (
        "\nCREATE TABLE extension_probe (id INT PRIMARY KEY, value "
        "TEXT);\nCREATE INDEX ix_extension_probe ON extension_probe(value);\n"
    )
    target = SqlFile(
        Path("schema.sql"), text, hashlib.sha256(text.encode()).hexdigest()
    )
    name = "well_extension_reference_" + uuid4().hex
    with closing(connect_test_database("postgres")) as admin:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with closing(connect_test_database(name)) as conn, installer_lock(conn):
                install_fresh(conn, target, None)
                contract = replace(
                    CONTRACT,
                    source_checksum=target.checksum,
                    catalog=read_catalog(conn),
                )
                yield target, contract
        finally:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def test_extension_preserves_rows_objects_history_and_is_repeatable(
    installed: psycopg2.extensions.connection,
    extension: tuple[SqlFile, SchemaContract],
) -> None:
    target, contract = extension
    with installed.cursor() as cur:
        cur.execute("UPDATE wellhead SET name='retained-well' WHERE wellhead_id=1")
        cur.execute("SELECT 'users'::regclass::oid, 'parameterreading'::regclass::oid")
        identities = cur.fetchone()
        cur.execute("SELECT * FROM schema_migrations ORDER BY filename")
        history = cur.fetchall()
    extend_schema(installed, SOURCE, target, CONTRACT, contract, SEEDS)
    extend_schema(installed, SOURCE, target, CONTRACT, contract, SEEDS)
    check_installed(installed, contract, SEEDS)
    with installed.cursor() as cur:
        cur.execute("SELECT name FROM wellhead WHERE wellhead_id=1")
        assert cur.fetchone() == ("retained-well",)
        cur.execute("SELECT 'users'::regclass::oid, 'parameterreading'::regclass::oid")
        assert cur.fetchone() == identities
        cur.execute("SELECT * FROM schema_migrations ORDER BY filename")
        assert cur.fetchall() == history
        cur.execute("SELECT count(*) FROM auditevent WHERE action='schema.extended'")
        assert cur.fetchone() == (1,)


def test_extension_catalog_mismatch_rolls_back_every_addition(
    installed: psycopg2.extensions.connection,
    extension: tuple[SqlFile, SchemaContract],
) -> None:
    target, contract = extension
    bad = {
        **contract.catalog,
        "columns": [
            row
            for row in contract.catalog["columns"]
            if '"extension_probe", "value"' not in row
        ],
    }
    with pytest.raises(RuntimeError, match="Schema differs"):
        extend_schema(
            installed, SOURCE, target, CONTRACT, replace(contract, catalog=bad), SEEDS
        )
    check_installed(installed, CONTRACT, SEEDS)
    with installed.cursor() as cur:
        cur.execute("SELECT to_regclass('extension_probe')")
        assert cur.fetchone() == (None,)


def test_extension_audit_failure_rolls_back_identity_and_tables(
    installed: psycopg2.extensions.connection,
    extension: tuple[SqlFile, SchemaContract],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, contract = extension
    from schema_install import verify_structure

    def fail_audit(
        conn: psycopg2.extensions.connection, reference: SchemaContract
    ) -> None:
        verify_structure(conn, reference)
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE auditevent ADD CONSTRAINT test_audit_failure "
                "CHECK (action <> 'schema.extended')"
            )

    monkeypatch.setattr("schema_extend.verify_structure", fail_audit)
    with pytest.raises(psycopg2.errors.CheckViolation):
        extend_schema(installed, SOURCE, target, CONTRACT, contract, SEEDS)
    check_installed(installed, CONTRACT, SEEDS)
    with installed.cursor() as cur:
        cur.execute("SELECT to_regclass('extension_probe')")
        assert cur.fetchone() == (None,)
