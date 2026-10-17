"""Accept a separately verified engine upgrade without applying application DDL."""

from __future__ import annotations

import psycopg2.extensions
from psycopg2.extras import Json

from schema_install import SchemaContract, check_installed, verify_structure
from schema_sql import SqlFile


def accept_engine(
    conn: psycopg2.extensions.connection,
    previous: SchemaContract,
    target: SchemaContract,
    seeds: list[SqlFile],
) -> None:
    """Verify the new engine and atomically record its identity and audit event.

    The caller must hold the installer lock. The previous generated reference
    must match the installed identity. Both references must describe the same
    SQL source and legacy history. Engine upgrade and backup verification happen
    separately; this operation cannot substitute for those release checks.
    """
    if (
        previous.source_checksum != target.source_checksum
        or previous.legacy_checksums != target.legacy_checksums
    ):
        raise RuntimeError("Engine acceptance cannot change SQL source or history")
    if previous.versions == target.versions:
        raise RuntimeError("Engine acceptance requires a changed engine version")
    # Check the target's actual structure before accepting any changed identity.
    verify_structure(conn, target)
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT source_checksum, contract_checksum, status "
            "FROM public.schema_installation"
        )
        rows = cursor.fetchall()
    if rows == [(target.source_checksum, target.checksum, "ready")]:
        check_installed(conn, target, seeds)
        return
    if rows != [(previous.source_checksum, previous.checksum, "ready")]:
        raise RuntimeError("Previous reference does not match the installed baseline")
    # Only the engine versions and their generated catalog descriptions may
    # differ. The regular checker still enforces the complete legacy history.
    detail = {
        "previous_versions": previous.versions,
        "target_versions": target.versions,
        "previous_contract": previous.checksum,
        "target_contract": target.checksum,
        "source_checksum": target.source_checksum,
    }
    with conn, conn.cursor() as cursor:
        cursor.execute(
            "UPDATE public.schema_installation SET contract_checksum=%s "
            "WHERE singleton AND contract_checksum=%s AND status='ready'",
            (target.checksum, previous.checksum),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Installed baseline changed during engine acceptance")
        check_installed(conn, target, seeds)
        cursor.execute(
            "INSERT INTO public.auditevent "
            "(actor_identifier, action, subject_type, subject_id, outcome, detail) "
            "VALUES (current_user, 'schema.engine.accepted', 'database', "
            "current_database(), 'success', %s)",
            (Json(detail),),
        )
