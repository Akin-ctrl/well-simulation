"""Add reviewed new tables from the canonical SQL without replaying old DDL."""

from __future__ import annotations

import json
import re

import psycopg2.extensions
from psycopg2.extras import Json

from schema_install import SchemaContract, check_installed, verify_structure
from schema_sql import SqlFile, skip_region, split_statements

IDENTIFIER = r"[a-zA-Z_][a-zA-Z0-9_]{0,62}"
TABLE = re.compile(rf"CREATE\s+TABLE\s+(?:public\.)?({IDENTIFIER})\s*\(", re.I)
INDEX = re.compile(
    rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+({IDENTIFIER})\s+ON\s+"
    rf"(?:public\.)?({IDENTIFIER})\s*\(",
    re.I,
)


def _without_leading_comments(statement: str) -> str:
    """Ignore comments at the statement boundary, never quoted SQL values."""
    remaining = statement.lstrip()
    while remaining.startswith(("--", "/*")):
        end = skip_region(remaining, 0)
        if end is None:
            raise ValueError("Invalid schema comment")
        remaining = remaining[end:].lstrip()
    return remaining


def _ordinary_table(statement: str, opening: int) -> bool:
    """Require one parenthesized definition without inheritance or partition tails."""
    depth = 0
    index = opening
    while index < len(statement):
        end = skip_region(statement, index)
        if end is not None:
            index = end
            continue
        if statement[index] == "(":
            depth += 1
        elif statement[index] == ")":
            depth -= 1
            if depth == 0:
                return not _without_leading_comments(statement[index + 1 :])
        index += 1
    return False


def extension_statements(
    previous: SqlFile, target: SqlFile, contract: SchemaContract
) -> tuple[list[str], list[str]]:
    """Accept only new ordinary tables and indexes on tables in this extension."""
    if not target.sql.startswith(previous.sql) or target.sql == previous.sql:
        raise RuntimeError("Extension requires the exact previous schema prefix")
    existing = {json.loads(row)[0] for row in contract.catalog["relations"]} | {
        json.loads(row)[1] for row in contract.catalog["indexes"]
    }
    names = set(existing)
    tables: list[str] = []
    statements: list[str] = []
    for raw in split_statements(target.sql[len(previous.sql) :]):
        statement = _without_leading_comments(raw)
        if not statement:
            continue
        table = TABLE.match(statement)
        index = INDEX.match(statement)
        if table is not None:
            name = table[1].lower()
            if not _ordinary_table(statement, table.end() - 1):
                raise RuntimeError("Only ordinary new table definitions are supported")
            tables.append(name)
        elif index is not None and index[2].lower() in tables:
            name = index[1].lower()
        else:
            raise RuntimeError("Extension permits only new tables and their indexes")
        if name in names:
            raise RuntimeError("Extension object name already exists")
        names.add(name)
        statements.append(statement)
    if not tables:
        raise RuntimeError("Extension must add at least one table")
    return statements, tables


def extend_schema(
    conn: psycopg2.extensions.connection,
    previous_source: SqlFile,
    source: SqlFile,
    previous: SchemaContract,
    target: SchemaContract,
    seeds: list[SqlFile],
) -> None:
    """Verify, extend, compare and audit within the caller's installer lock."""
    if (
        previous.versions != target.versions
        or previous.legacy_checksums != target.legacy_checksums
        or previous.source_checksum != previous_source.checksum
        or target.source_checksum != source.checksum
    ):
        raise RuntimeError("Schema extension cannot change engines or legacy history")
    statements, tables = extension_statements(previous_source, source, previous)
    if any(
        not set(rows) <= set(target.catalog[section])
        for section, rows in previous.catalog.items()
    ):
        raise RuntimeError("Target contract changes an existing object definition")
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT source_checksum, contract_checksum, status "
            "FROM public.schema_installation"
        )
        installed = cursor.fetchall()
    if installed == [(target.source_checksum, target.checksum, "ready")]:
        check_installed(conn, target, seeds)
        return
    check_installed(conn, previous, seeds)
    autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '3000ms'")
            cursor.execute("SET LOCAL statement_timeout = '30000ms'")
            for statement in statements:
                cursor.execute(statement)
            verify_structure(conn, target)
            cursor.execute(
                "UPDATE public.schema_installation SET source_checksum=%s, "
                "contract_checksum=%s, completed_at=clock_timestamp(), "
                "completed_statements=%s WHERE singleton AND "
                "source_checksum=%s AND contract_checksum=%s AND status='ready'",
                (
                    source.checksum,
                    target.checksum,
                    len(split_statements(source.sql)),
                    previous.source_checksum,
                    previous.checksum,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Installed identity changed during extension")
            cursor.execute(
                "INSERT INTO public.auditevent "
                "(actor_identifier, action, subject_type, subject_id, outcome, detail) "
                "VALUES (current_user, 'schema.extended', 'database', "
                "current_database(), 'success', %s)",
                (
                    Json(
                        {
                            "previous_source": previous.source_checksum,
                            "target_source": target.source_checksum,
                            "previous_contract": previous.checksum,
                            "target_contract": target.checksum,
                            "tables": tables,
                        }
                    ),
                ),
            )
    finally:
        conn.autocommit = autocommit
