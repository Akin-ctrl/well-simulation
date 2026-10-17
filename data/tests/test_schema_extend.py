"""Reject unsafe extension shapes before opening a write transaction."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from schema import CONTRACT_PATH
from schema_extend import extension_statements
from schema_install import SchemaContract
from schema_sql import SqlFile


def source(text: str) -> SqlFile:
    return SqlFile(Path("schema.sql"), text, hashlib.sha256(text.encode()).hexdigest())


def previous_contract() -> SchemaContract:
    # This fixture only exercises declaration validation. The full target catalog
    # is generated and checked by the separate database extension tests.
    raw = json.loads(CONTRACT_PATH.read_text())
    return SchemaContract(
        raw["source_checksum"], raw["versions"], raw["legacy_checksums"], raw["catalog"]
    )


def test_new_tables_and_their_indexes_accept_quoted_semicolons() -> None:
    old = source("-- existing\nSELECT 1;\n")
    target = source(
        old.sql
        + (
            "/* new */ CREATE TABLE new_work (id UUID PRIMARY KEY, reason "
            "TEXT DEFAULT ';'); CREATE INDEX ix_new_work ON new_work(reason)"
        )
    )
    statements, tables = extension_statements(old, target, previous_contract())
    assert tables == ["new_work"]
    assert len(statements) == 2
    assert "DEFAULT ';'" in statements[0]


@pytest.mark.parametrize(
    "suffix",
    [
        "DELETE FROM users",
        "ALTER TABLE users ADD COLUMN danger TEXT",
        "DROP TABLE users",
        "CREATE TABLE users (id INT)",
        "CREATE TABLE new_work AS SELECT * FROM users",
        "CREATE TABLE new_work (id INT) INHERITS (users)",
        "CREATE TABLE new_work (id INT); CREATE INDEX ix_old ON users(id)",
        "CREATE TABLE new_work (id INT); CREATE INDEX CONCURRENTLY ix_new ON "
        "new_work(id)",
        "CREATE TABLE new_work (id INT); CREATE TABLE new_work (other INT)",
        "CREATE FUNCTION changed() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
    ],
)
def test_unsupported_or_existing_targets_are_refused(suffix: str) -> None:
    old = source("SELECT 1;\n")
    with pytest.raises(RuntimeError):
        extension_statements(old, source(old.sql + suffix), previous_contract())


def test_changed_source_prefix_is_refused() -> None:
    with pytest.raises(RuntimeError, match="exact previous"):
        extension_statements(
            source("SELECT 1;"),
            source("SELECT 2; CREATE TABLE new_work(id INT)"),
            previous_contract(),
        )
