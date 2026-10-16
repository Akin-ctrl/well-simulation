"""Install, check, or adopt the canonical historian schema and provision services."""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from pathlib import Path

import bcrypt
import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from schema_install import (
    SchemaContract,
    adopt,
    apply_seeds,
    build_reference,
    check_installed,
    has_table,
    install_fresh,
    installer_lock,
)
from schema_sql import SqlFile, discover
from telemetry_common import DatabaseSettings, configure_logging, connect_db

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
SCHEMA_PATH = SQL_DIR / "schema.sql"
CONTRACT_PATH = SQL_DIR / "schema-contract.json"
SEEDS_DIR = SQL_DIR / "seeds"
ADMIN_EMAIL = "admin@example.com"
ADMIN_USERNAME = "bootstrap-admin"
ADMIN_PASSWORD_BYTES = 18
BCRYPT_ROUNDS = 11
logger = configure_logging("schema", stream=sys.stdout)


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


def _provision_service_login(
    conn: psycopg2.extensions.connection,
    database: DatabaseSettings,
    user: str,
    password: str,
) -> None:
    """Reset a separate service login to explicit grants, rejecting powerful roles."""
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
                raise ValueError("Service database role has unexpected privileges")
            cursor.execute(
                "SELECT 1 FROM pg_auth_members WHERE member = %s LIMIT 1",
                (role_id,),
            )
            if cursor.fetchone() is not None:
                raise ValueError("Service database role must not inherit another role")
            cursor.execute(
                sql.SQL("ALTER ROLE {} PASSWORD %s").format(sql.Identifier(user)),
                (password,),
            )

        # PostgreSQL 14 grants CREATE on public and TEMPORARY on the database
        # to PUBLIC by default. Table SELECT grants alone do not make this
        # account least-privilege while those broad grants remain in force.
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
            sql.SQL("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {}").format(
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


def provision_twin_reader(
    conn: psycopg2.extensions.connection, database: DatabaseSettings
) -> None:
    """Create a least-privilege login for the internal twin-core service.

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

    _provision_service_login(conn, database, user, password)
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "GRANT SELECT ON TABLE wellhead, deviceParameterMapping, "
                "parameterType, parameterReading, alarmRule TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT ON TABLE twin_state_snapshot, "
                "wellhead_model_parameter TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT INSERT ON TABLE twin_state_snapshot, twin_recovery_event, "
                "prediction_run, prediction_point, scenario_run, scenario_point TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT USAGE ON SEQUENCE twin_state_snapshot_snapshot_id_seq, "
                "twin_recovery_event_event_id_seq TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT ON TABLE control_command, wellhead_control_state TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT UPDATE (status, result_at, result_revision, result_reason) "
                "ON control_command TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL("GRANT INSERT, UPDATE ON wellhead_control_state TO {}").format(
                sql.Identifier(user)
            )
        )
        cursor.execute(
            sql.SQL("GRANT INSERT ON control_command_event TO {}").format(
                sql.Identifier(user)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT USAGE ON SEQUENCE control_command_event_event_id_seq TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT EXECUTE ON FUNCTION control_actor_authority(INT) TO {}"
            ).format(sql.Identifier(user))
        )
    conn.commit()
    logger.info("Twin-core database role is ready", extra={"role": user})


def provision_api_role(
    conn: psycopg2.extensions.connection, database: DatabaseSettings
) -> None:
    """Grant API writes without schema or active-control ownership."""
    user = os.getenv("API_DB_USER", "")
    password = os.getenv("API_DB_PASSWORD", "")
    if not user and not password:
        return
    if not user or not password:
        raise ValueError("API_DB_USER and API_DB_PASSWORD must be set together")
    if user in (database.user, os.getenv("TWIN_DB_USER", "")):
        raise ValueError("API must use a separate database role")
    _provision_service_login(conn, database, user, password)
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "GRANT SELECT ON users, wellhead, field, location, device, "
                "parameterType, deviceParameterMapping, parameterReading, alarmRule, "
                "alarmEvent, v_wellhead_parameter_readings, v_active_alarms, "
                "mv_2min_pressure_trends, mv_2min_temp_flow_trends, "
                "mv_2min_water_cut_gor_trends, mv_5min_alarm_counts, "
                "prediction_run, prediction_point, scenario_run, scenario_point, "
                "control_command, control_command_event, wellhead_control_state, "
                "model_parameter_change, wellhead_model_parameter TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT INSERT ON users, auditEvent, control_command, "
                "control_command_event, model_parameter_change, "
                "wellhead_model_parameter TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL("GRANT UPDATE (session_version, updated_at) ON users TO {}").format(
                sql.Identifier(user)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT UPDATE (status, expires_at, queued_at, "
                "activated_by, activated_role, "
                "activated_session_version, result_at, result_reason) "
                "ON control_command TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT UPDATE (status, result_at, published_override_id) "
                "ON model_parameter_change TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL(
                "GRANT USAGE ON SEQUENCE users_id_seq, auditevent_audit_event_id_seq, "
                "control_command_event_event_id_seq, "
                "wellhead_model_parameter_parameter_override_id_seq TO {}"
            ).format(sql.Identifier(user))
        )
        cursor.execute(
            sql.SQL("GRANT EXECUTE ON FUNCTION lock_control_state(INT) TO {}").format(
                sql.Identifier(user)
            )
        )
    conn.commit()
    logger.info("API database role is ready", extra={"role": user})


def command_parser() -> argparse.ArgumentParser:
    """Describe explicit schema operations; init also checks an adopted database."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        default="init",
        choices=("init", "check", "adopt", "build-reference"),
    )
    parser.add_argument(
        "--without-seeds",
        action="store_true",
        help="Install structure without demo records or accounts",
    )
    parser.add_argument(
        "--contract-output",
        type=Path,
        help="Write a generated reference from a new disposable database",
    )
    return parser


def provision(conn: psycopg2.extensions.connection, settings: DatabaseSettings) -> None:
    """Apply configured service grants after structural verification."""
    provision_twin_reader(conn, settings)
    provision_api_role(conn, settings)


def execute_mode(
    conn: psycopg2.extensions.connection,
    settings: DatabaseSettings,
    args: argparse.Namespace,
) -> None:
    """Dispatch the explicit database action while holding the installer lock."""
    source = SqlFile.load(SCHEMA_PATH)
    seeds = discover(SEEDS_DIR)
    if args.mode == "build-reference":
        if args.contract_output is None:
            raise ValueError("build-reference requires --contract-output")
        reference = build_reference(conn, source, CONTRACT_PATH)
        args.contract_output.write_text(reference.document(), encoding="utf-8")
        logger.info("Generated schema reference from a fresh database")
        return
    if args.contract_output is not None:
        raise ValueError("--contract-output is only valid with build-reference")
    contract = SchemaContract.load(CONTRACT_PATH, source)
    if args.mode == "adopt":
        adopt(conn, source, contract, seeds)
    elif args.mode == "check" or has_table(conn, "schema_installation"):
        check_installed(conn, contract, seeds)
    else:
        install_fresh(conn, source, contract)
    if args.mode == "check":
        logger.info("Schema identity and structure verified")
        return
    # Role revocations and grants must commit together. Schema statements use
    # autocommit for TimescaleDB, but account provisioning uses transactions.
    conn.autocommit = False
    if not args.without_seeds:
        apply_seeds(conn, seeds)
        bootstrap_admin(conn)
    provision(conn, settings)
    logger.info("Database schema and configured roles are ready")


def main() -> int:
    """Run the installer and close its connection on every outcome."""
    args = command_parser().parse_args()
    conn = None
    try:
        settings = DatabaseSettings.from_env()
        conn = connect_db(settings)
        with installer_lock(conn):
            execute_mode(conn, settings, args)
    except psycopg2.Error as exc:
        # Database errors can contain SQL values. Report the error class and
        # SQLSTATE, keeping credentials and application data out of logs.
        logger.error(
            "Schema database operation failed",
            extra={
                "error_type": type(exc).__name__,
                "sqlstate": exc.pgcode,
            },
        )
        return 1
    except (RuntimeError, ValueError, OSError) as exc:
        logger.error("Schema operation refused", extra={"reason": str(exc)})
        return 1
    finally:
        if conn is not None:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
