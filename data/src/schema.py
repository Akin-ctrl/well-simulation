"""Apply ordered SQL migrations to the historian database.

Replaces mounting the schema into /docker-entrypoint-initdb.d, which Postgres
runs only on first initialisation of the data volume. Under that arrangement
every schema change required `docker compose down -v` and destroyed the
historian, which was documented nowhere.

Applied migrations are recorded in schema_migrations with a checksum, so a
re-run is a no-op and an edit to an already-applied file is an error rather
than a silent divergence.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import sys
from dataclasses import dataclass
from pathlib import Path

import bcrypt
import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from telemetry_common import DatabaseSettings, configure_logging, connect_db

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "sql" / "migrations"
SEEDS_DIR = Path(__file__).resolve().parent.parent / "sql" / "seeds"

# TimescaleDB refuses to create a continuous aggregate inside a transaction
# block, so those files opt out of the surrounding transaction.
NO_TRANSACTION_DIRECTIVE = "-- migrate:no-transaction"

# Bootstrap administrator. The password is generated on first run, never stored
# in the repository, and logged exactly once.
ADMIN_EMAIL = "admin@example.com"
ADMIN_USERNAME = "bootstrap-admin"
ADMIN_PASSWORD_BYTES = 18
BCRYPT_ROUNDS = 11

SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

logger = configure_logging("migrator", stream=sys.stdout)


@dataclass(frozen=True)
class Migration:
    """One migration file and how it must be applied."""

    path: Path
    sql: str
    checksum: str

    @property
    def name(self) -> str:
        """Return the filename used as the applied-migration key."""
        return self.path.name

    @property
    def in_transaction(self) -> bool:
        """Whether this file may be wrapped in a transaction."""
        return NO_TRANSACTION_DIRECTIVE not in self.sql

    @classmethod
    def load(cls, path: Path) -> Migration:
        """Read a migration file and compute its checksum."""
        sql = path.read_text(encoding="utf-8")
        return cls(
            path=path,
            sql=sql,
            checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        )


DOLLAR_TAG_PATTERN = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def _end_of_region(sql: str, start: int, terminator: str) -> int:
    """Return the index just past `terminator`, or the end of the script."""
    found = sql.find(terminator, start)
    return len(sql) if found == -1 else found + len(terminator)


def _skip_region(sql: str, index: int) -> int | None:
    """If a non-splittable region starts at `index`, return the index past it.

    Comments, string literals, and dollar-quoted bodies may all contain
    semicolons that are not statement separators.
    """
    rest = sql[index:]

    if rest.startswith("--"):
        return _end_of_region(sql, index, "\n")
    if rest.startswith("/*"):
        return _end_of_region(sql, index + 2, "*/")
    if rest.startswith("'"):
        return _end_of_region(sql, index + 1, "'")

    tag = DOLLAR_TAG_PATTERN.match(rest)
    if tag:
        return _end_of_region(sql, index + len(tag.group(0)), tag.group(0))

    return None


def split_statements(sql: str) -> list[str]:
    """Split a script into individual statements on top-level semicolons.

    Needed for files applied outside a transaction: Postgres wraps a
    multi-statement simple query in an implicit transaction block, which is
    exactly what a continuous aggregate refuses to be created inside. Sending
    one statement per round trip avoids that.
    """
    statements: list[str] = []
    start = 0
    index = 0

    while index < len(sql):
        skipped = _skip_region(sql, index)
        if skipped is not None:
            index = skipped
            continue

        if sql[index] == ";":
            statement = sql[start:index].strip()
            if statement:
                statements.append(statement)
            index += 1
            start = index
            continue

        index += 1

    trailing = sql[start:].strip()
    if trailing:
        statements.append(trailing)

    return statements


def discover(directory: Path) -> list[Migration]:
    """Return every .sql file in a directory, in filename order."""
    if not directory.is_dir():
        return []
    return [Migration.load(path) for path in sorted(directory.glob("*.sql"))]


def applied_migrations(conn: psycopg2.extensions.connection) -> dict[str, str]:
    """Return the filename to checksum map of already-applied migrations."""
    with conn.cursor() as cursor:
        cursor.execute(SCHEMA_MIGRATIONS_DDL)
        conn.commit()
        cursor.execute("SELECT filename, checksum FROM schema_migrations")
        return dict(cursor.fetchall())


def apply(conn: psycopg2.extensions.connection, migration: Migration) -> None:
    """Apply one migration and record it."""
    # psycopg2 opens a transaction implicitly on the first statement, and
    # autocommit cannot be changed while one is open. Everything before this
    # point is already committed, so closing the idle transaction is safe.
    conn.rollback()

    previous_autocommit = conn.autocommit
    conn.autocommit = not migration.in_transaction
    try:
        with conn.cursor() as cursor:
            if migration.in_transaction:
                cursor.execute(migration.sql)
            else:
                for statement in split_statements(migration.sql):
                    cursor.execute(statement)
            cursor.execute(
                "INSERT INTO schema_migrations (filename, checksum) VALUES (%s, %s)",
                (migration.name, migration.checksum),
            )
        if migration.in_transaction:
            conn.commit()
    except Exception:
        if migration.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.autocommit = previous_autocommit


def run(conn: psycopg2.extensions.connection, migrations: list[Migration]) -> int:
    """Apply every outstanding migration. Returns how many were applied."""
    already = applied_migrations(conn)
    applied_count = 0

    for migration in migrations:
        recorded = already.get(migration.name)
        if recorded is not None:
            if recorded != migration.checksum:
                raise RuntimeError(
                    f"{migration.name} has changed since it was applied. "
                    "Migrations are immutable; add a new file instead."
                )
            continue

        logger.info(
            "Applying migration",
            extra={
                "migration": migration.name,
                "transactional": migration.in_transaction,
            },
        )
        apply(conn, migration)
        applied_count += 1

    return applied_count


def bootstrap_admin(conn: psycopg2.extensions.connection) -> None:
    """Create the first administrator if no account holds the ADMIN role.

    ADR 0033 makes creating a user an administrative action, which leaves the
    question of where the first administrator comes from. Seeding one with a
    committed password would put a privileged credential in the repository, so
    the password is generated here and logged exactly once.

    Doing this in the migrator rather than SQL is what allows the password to be
    generated at all. Verified compatible with the API's bcryptjs.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM users WHERE role = 'ADMIN' LIMIT 1")
        if cursor.fetchone() is not None:
            return

        password = secrets.token_urlsafe(ADMIN_PASSWORD_BYTES)
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=BCRYPT_ROUNDS))

        # Bare DO NOTHING rather than a named constraint: email and user_name
        # are both unique, and either could already be taken.
        cursor.execute(
            """
            INSERT INTO users (email, user_name, first_name, role, encrypted_password)
            VALUES (%s, %s, %s, 'ADMIN', %s)
            ON CONFLICT DO NOTHING
            """,
            (ADMIN_EMAIL, ADMIN_USERNAME, "Bootstrap", hashed.decode()),
        )
        created = cursor.rowcount == 1
    conn.commit()

    if not created:
        # An existing non-admin account holds the address or username. Promoting
        # it automatically would silently grant administrative authority to an
        # account someone else controls, so say what happened instead.
        logger.error(
            "Could not create the bootstrap administrator: an account already "
            "uses this address or username, and no account holds ADMIN",
            extra={
                "email": ADMIN_EMAIL,
                "user_name": ADMIN_USERNAME,
                "action_required": (
                    "grant ADMIN to an existing account, or remove the "
                    "conflicting account and re-run the migrator"
                ),
            },
        )
        return

    # The only time this value is ever available. It is not recoverable.
    logger.warning(
        "Created bootstrap administrator; record this password now",
        extra={
            "email": ADMIN_EMAIL,
            "password": password,
            "action_required": "sign in and change this password",
        },
    )


def provision_twin_reader(
    conn: psycopg2.extensions.connection, database: DatabaseSettings
) -> None:
    """Create a read-only login for the internal twin-core service.

    Standalone migration runs without twin settings keep working. Compose sets
    both values, so its dependent twin-core always receives a provisioned role.
    """
    user = os.getenv("TWIN_DB_USER", "")
    password = os.getenv("TWIN_DB_PASSWORD", "")
    if not user and not password:
        return
    if not user or not password:
        raise ValueError("TWIN_DB_USER and TWIN_DB_PASSWORD must be set together")
    if user == database.user:
        raise ValueError("Twin reader must use a separate database role")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT oid, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                   rolreplication
            FROM pg_roles WHERE rolname = %s
            """,
            (user,),
        )
        existing = cursor.fetchone()
        if existing is None:
            cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} WITH LOGIN NOINHERIT NOSUPERUSER "
                    "NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %s"
                ).format(sql.Identifier(user)),
                (password,),
            )
        else:
            role_id, can_login, superuser, create_db, create_role, replication = (
                existing
            )
            if not can_login or any((superuser, create_db, create_role, replication)):
                raise ValueError("Twin database role has unexpected privileges")
            cursor.execute(
                "SELECT 1 FROM pg_auth_members WHERE member = %s LIMIT 1",
                (role_id,),
            )
            if cursor.fetchone() is not None:
                raise ValueError("Twin database role must not inherit another role")
            cursor.execute(
                sql.SQL("ALTER ROLE {} PASSWORD %s").format(sql.Identifier(user)),
                (password,),
            )

        # PostgreSQL 14 grants CREATE on public and TEMPORARY on the database
        # to PUBLIC by default. Table SELECT grants alone do not make this
        # account read-only while those broad grants remain in force.
        cursor.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        cursor.execute(
            sql.SQL("REVOKE TEMPORARY ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(database.name)
            )
        )
        cursor.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(
                sql.Identifier(user)
            )
        )
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database.name), sql.Identifier(user)
            )
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT ON TABLE wellhead, deviceParameterMapping, "
                "parameterType, parameterReading TO {}"
            ).format(sql.Identifier(user))
        )
    conn.commit()
    logger.info("Twin-core read-only database role is ready", extra={"role": user})


def main() -> int:
    """Apply schema migrations, then seed fixtures."""
    settings = DatabaseSettings.from_env()

    migrations = discover(MIGRATIONS_DIR)
    if not migrations:
        logger.error("No migrations found", extra={"directory": str(MIGRATIONS_DIR)})
        return 1

    try:
        conn = connect_db(settings)
    except psycopg2.OperationalError:
        logger.exception("Database connection failed")
        return 1

    try:
        applied = run(conn, migrations)
        seeded = run(conn, discover(SEEDS_DIR))
    except (psycopg2.Error, RuntimeError):
        logger.exception("Migration failed")
        return 1

    try:
        bootstrap_admin(conn)
    except psycopg2.Error:
        # The schema is applied and correct; only the convenience account
        # failed. Blocking every dependent service on that would be wrong.
        logger.exception("Bootstrap administrator could not be created")
        conn.rollback()

    try:
        provision_twin_reader(conn, settings)
    except (psycopg2.Error, ValueError):
        logger.error("Twin-core database role could not be provisioned")
        conn.rollback()
        return 1
    finally:
        if not conn.closed:
            conn.close()

    logger.info(
        "Database is up to date",
        extra={
            "migrations_applied": applied,
            "seeds_applied": seeded,
            "migrations_total": len(migrations),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
