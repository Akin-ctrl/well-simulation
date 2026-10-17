"""Give model integration tests fresh schemas and real service permissions."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import psycopg2.extensions
import pytest
from psycopg2 import sql
from test_schema_database import database, installed
from twin_core.config import ServiceSettings

from schema import provision_api_role, provision_twin_reader
from telemetry_common import DatabaseSettings

__all__ = ["database", "installed"]


@pytest.fixture
def model_settings(
    installed: psycopg2.extensions.connection, monkeypatch: pytest.MonkeyPatch
) -> Iterator[ServiceSettings]:
    """Provision unique roles only inside the database owned by this test."""
    twin_role = "twin_test_" + uuid4().hex
    api_role = "api_test_" + uuid4().hex
    password = uuid4().hex
    monkeypatch.setenv("TWIN_DB_USER", twin_role)
    monkeypatch.setenv("TWIN_DB_PASSWORD", password)
    monkeypatch.setenv("API_DB_USER", api_role)
    monkeypatch.setenv("API_DB_PASSWORD", uuid4().hex)
    settings = ServiceSettings(
        bind_host="127.0.0.1",
        port=0,
        max_catchup_steps=5,
        fleet_refresh_seconds=30,
        telemetry_freshness_seconds=15,
        database_host=installed.info.host,
        database_port=installed.info.port,
        database_name=installed.info.dbname,
        database_user=twin_role,
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
    installed.autocommit = False
    try:
        provision_twin_reader(installed, admin)
        provision_api_role(installed, admin)
        yield settings
    finally:
        # A failed assertion may leave an aborted transaction. Close it before
        # revoking these test-owned grants and deleting the cluster-wide roles.
        installed.rollback()
        installed.autocommit = True
        with installed.cursor() as cursor:
            for role in (api_role, twin_role):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone() is not None:
                    cursor.execute(
                        sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role))
                    )
                    cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
