"""Install one schema or adopt a verified legacy database without replaying DDL."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import psycopg2.extensions

from schema_catalog import (
    QUERIES,
    Catalog,
    database_versions,
    differences,
    read_catalog,
)
from schema_sql import SqlFile, split_statements

LOCK_ID = 846291735  # Shared by every cooperating installer for this database.
STATE_DDL = """
CREATE TABLE public.schema_installation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    source_checksum TEXT NOT NULL,
    contract_checksum TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('installing', 'ready')),
    method TEXT NOT NULL CHECK (method IN ('init', 'adopt', 'reference')),
    completed_statements INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    installed_by TEXT NOT NULL DEFAULT current_user
)
"""
LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    filename TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def _string_map(value: object) -> dict[str, str]:
    """Validate generated identity maps before using their entries."""
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ValueError("Invalid schema reference identity map")
    return cast(dict[str, str], value)


def _catalog(value: object) -> Catalog:
    """Require every supported comparison section and valid serialized rows."""
    if not isinstance(value, dict) or value.keys() != QUERIES.keys():
        raise ValueError("Schema reference comparison sections differ")
    for rows in value.values():
        if not isinstance(rows, list):
            raise ValueError("Invalid schema reference rows")
        for row in rows:
            if not isinstance(row, str):
                raise ValueError("Invalid schema reference row")
            decoded = json.loads(row)
            if not isinstance(decoded, list) or not decoded:
                raise ValueError("Invalid schema reference row")
    return cast(Catalog, value)


@dataclass(frozen=True)
class SchemaContract:
    """Generated reference plus the exact legacy history eligible for adoption."""

    source_checksum: str
    versions: dict[str, str]
    legacy_checksums: dict[str, str]
    catalog: Catalog

    def document(self) -> str:
        """Serialize the generated record deterministically for review."""
        return (
            json.dumps(
                {
                    "format": 1,
                    "source_checksum": self.source_checksum,
                    "versions": self.versions,
                    "legacy_checksums": self.legacy_checksums,
                    "catalog": self.catalog,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )

    @property
    def checksum(self) -> str:
        """Identify the complete reference, including the comparison results."""
        return hashlib.sha256(self.document().encode()).hexdigest()

    @classmethod
    def load(cls, path: Path, source: SqlFile) -> SchemaContract:
        """Reject stale or malformed generated references before database writes."""
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("format") != 1:
            raise RuntimeError("Unsupported schema contract format")
        for key in ("versions", "legacy_checksums", "catalog"):
            if not isinstance(raw.get(key), dict):
                raise RuntimeError(f"Invalid schema contract: {key}")
        if raw.get("source_checksum") != source.checksum:
            raise RuntimeError("schema.sql changed; rebuild and verify its reference")
        return cls(
            source.checksum,
            _string_map(raw["versions"]),
            _string_map(raw["legacy_checksums"]),
            _catalog(raw["catalog"]),
        )


@contextmanager
def installer_lock(
    conn: psycopg2.extensions.connection, timeout_seconds: float = 10.0
) -> Iterator[None]:
    """Hold a session lock across commits, with a bounded acquisition wait."""
    conn.autocommit = True
    deadline = time.monotonic() + timeout_seconds
    with conn.cursor() as cursor:
        cursor.execute("SET lock_timeout = '10s'")
        cursor.execute("SET statement_timeout = '60s'")
        while True:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
            acquired = cursor.fetchone()
            if acquired is not None and acquired[0]:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Another schema installer holds the database lock")
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    try:
        yield
    finally:
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))


def has_table(conn: psycopg2.extensions.connection, name: str) -> bool:
    """Look up a bookkeeping table without creating it."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{name}",))
        row = cursor.fetchone()
        return row is not None and bool(row[0])


def require_empty(conn: psycopg2.extensions.connection) -> None:
    """Refuse installation if any user object or installer history exists."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND NOT EXISTS (
                SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass
                  AND d.objid=c.oid AND d.deptype='e')
            UNION ALL
            SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='public' AND NOT EXISTS (
                SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass
                  AND d.objid=p.oid AND d.deptype='e')
            UNION ALL
            SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
            WHERE n.nspname='public' AND t.typtype IN ('e','d') LIMIT 1
        """)
        if cursor.fetchone() is not None:
            raise RuntimeError("Database is not empty; use check or verified adopt")


def verify_structure(
    conn: psycopg2.extensions.connection, contract: SchemaContract
) -> None:
    """Fail on unsupported versions or any unchecked structural difference."""
    if database_versions(conn) != contract.versions:
        raise RuntimeError(
            "Database versions differ from the verified schema reference"
        )
    changes = differences(contract.catalog, read_catalog(conn))
    if changes:
        raise RuntimeError("Schema differs: " + "; ".join(changes))


def verify_history(
    conn: psycopg2.extensions.connection,
    contract: SchemaContract,
    seeds: list[SqlFile],
    *,
    require_legacy: bool,
) -> None:
    """Validate existing ledger entries without rewriting their history."""
    if not has_table(conn, "schema_migrations"):
        if require_legacy:
            raise RuntimeError("Legacy schema history is missing")
        return
    with conn.cursor() as cursor:
        cursor.execute("SELECT filename, checksum FROM public.schema_migrations")
        recorded = dict(cursor.fetchall())
    expected = contract.legacy_checksums | {seed.name: seed.checksum for seed in seeds}
    for name, checksum in recorded.items():
        if expected.get(name) != checksum:
            raise RuntimeError(f"Unknown or changed schema history: {name}")
    if require_legacy and not contract.legacy_checksums.keys() <= recorded.keys():
        raise RuntimeError("Legacy schema history is incomplete")


def _begin_install(
    conn: psycopg2.extensions.connection, source: SqlFile, checksum: str, method: str
) -> None:
    """Record an incomplete installation before executing schema statements."""
    with conn.cursor() as cursor:
        cursor.execute(STATE_DDL)
        cursor.execute(
            "INSERT INTO public.schema_installation "
            "(source_checksum, contract_checksum, status, method) "
            "VALUES (%s, %s, 'installing', %s)",
            (source.checksum, checksum, method),
        )


def _mark_ready(conn: psycopg2.extensions.connection, checksum: str) -> None:
    """Finish only after structural verification succeeds."""
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE public.schema_installation SET status='ready', "
            "contract_checksum=%s, completed_at=clock_timestamp() WHERE singleton",
            (checksum,),
        )


def install_fresh(
    conn: psycopg2.extensions.connection,
    source: SqlFile,
    contract: SchemaContract | None,
) -> None:
    """Apply each statement once; interrupted installations require investigation."""
    statements = split_statements(source.sql)
    require_empty(conn)
    if contract is not None:
        versions = database_versions(conn)
        if versions["timescaledb"] == "absent":
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT default_version FROM pg_available_extensions "
                    "WHERE name='timescaledb'"
                )
                available = cursor.fetchone()
                versions["timescaledb"] = str(available[0]) if available else "absent"
        if versions != contract.versions:
            raise RuntimeError("Database versions differ from the schema reference")
    with conn:
        _begin_install(
            conn,
            source,
            contract.checksum if contract else "",
            "init" if contract else "reference",
        )
    conn.autocommit = True
    with conn.cursor() as cursor:
        for number, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            cursor.execute(
                "UPDATE public.schema_installation SET completed_statements=%s "
                "WHERE singleton",
                (number,),
            )
    if contract is not None:
        verify_structure(conn, contract)
        _mark_ready(conn, contract.checksum)


def check_installed(
    conn: psycopg2.extensions.connection, contract: SchemaContract, seeds: list[SqlFile]
) -> None:
    """Check adopted identity and actual structure without applying schema SQL."""
    if not has_table(conn, "schema_installation"):
        raise RuntimeError(
            "No canonical baseline recorded; verify and adopt this database"
        )
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT source_checksum, contract_checksum, status, method "
            "FROM public.schema_installation"
        )
        rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][2] != "ready":
        raise RuntimeError("Schema installation is incomplete; inspect before recovery")
    if tuple(rows[0][:2]) != (contract.source_checksum, contract.checksum):
        raise RuntimeError("Schema identity changed; a reviewed conversion is required")
    verify_history(conn, contract, seeds, require_legacy=rows[0][3] == "adopt")
    verify_structure(conn, contract)


def adopt(
    conn: psycopg2.extensions.connection,
    source: SqlFile,
    contract: SchemaContract,
    seeds: list[SqlFile],
) -> None:
    """Record a matching legacy installation, retaining every application object."""
    if has_table(conn, "schema_installation"):
        check_installed(conn, contract, seeds)
        return
    verify_history(conn, contract, seeds, require_legacy=True)
    verify_structure(conn, contract)
    with conn:
        _begin_install(conn, source, contract.checksum, "adopt")
        _mark_ready(conn, contract.checksum)


def apply_seeds(conn: psycopg2.extensions.connection, seeds: list[SqlFile]) -> int:
    """Apply each separate fixture once, recording its unchanged checksum."""
    with conn.cursor() as cursor:
        cursor.execute(LEDGER_DDL)
        cursor.execute("SELECT filename, checksum FROM public.schema_migrations")
        recorded = dict(cursor.fetchall())
    count = 0
    for seed in seeds:
        if seed.name in recorded:
            if recorded[seed.name] != seed.checksum:
                raise RuntimeError(f"Seed changed after application: {seed.name}")
            continue
        with conn, conn.cursor() as cursor:
            cursor.execute(seed.sql)
            cursor.execute(
                "INSERT INTO public.schema_migrations (filename, checksum) "
                "VALUES (%s, %s)",
                (seed.name, seed.checksum),
            )
        count += 1
    return count


def build_reference(
    conn: psycopg2.extensions.connection, source: SqlFile, legacy_path: Path
) -> SchemaContract:
    """Generate a contract exclusively from a fresh installation of the source."""
    legacy = _string_map(json.loads(legacy_path.read_text())["legacy_checksums"])
    install_fresh(conn, source, None)
    contract = SchemaContract(
        source.checksum, database_versions(conn), legacy, read_catalog(conn)
    )
    _mark_ready(conn, contract.checksum)
    return contract
