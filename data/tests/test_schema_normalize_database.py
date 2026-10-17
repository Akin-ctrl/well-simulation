"""Prove the dump/restore parser equivalence on the pinned PostgreSQL engine."""

from __future__ import annotations

import psycopg2.extensions
from psycopg2 import sql
from test_schema_database import database, pytestmark

from schema_normalize import normalize_bounded_minutes

__all__ = ["database", "pytestmark"]


def test_dumped_duration_check_compares_equal_after_reparse(
    database: psycopg2.extensions.connection,
) -> None:
    with database.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE duration_probe (interval_seconds INT CHECK "
            "(interval_seconds BETWEEN 300 AND 86400 AND interval_seconds % 60 = 0))"
        )
        cursor.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid='duration_probe'::regclass"
        )
        original_row = cursor.fetchone()
        assert original_row is not None
        original = original_row[0]
        # The definition comes from this test's own table, as it would in pg_dump.
        cursor.execute(
            sql.SQL("CREATE TABLE restored_probe (interval_seconds INT {})").format(
                sql.SQL(original)
            )
        )
        cursor.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid='restored_probe'::regclass"
        )
        restored_row = cursor.fetchone()
        assert restored_row is not None
        restored = restored_row[0]
        assert original != restored
        assert normalize_bounded_minutes(original) == restored
        cursor.execute(
            "SELECT bool_and(((a AND b) AND c) IS NOT DISTINCT FROM (a AND b AND c)) "
            "FROM (VALUES (true),(false),(NULL::boolean)) x(a) "
            "CROSS JOIN (VALUES (true),(false),(NULL::boolean)) y(b) "
            "CROSS JOIN (VALUES (true),(false),(NULL::boolean)) z(c)"
        )
        assert cursor.fetchone() == (True,)
        database.rollback()
