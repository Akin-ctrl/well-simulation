"""Command tests against fresh schemas with the real service permissions."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import pytest
from test_schema_database import pytestmark
from twin_core.config import ServiceSettings
from twin_core.control_store import (
    CommandRef,
    ControlCommitUnknownError,
    ControlContext,
    ControlStore,
)
from twin_core.database import DatabaseUnavailableError
from twin_core.model import ControlInputs

__all__ = ["pytestmark"]


@pytest.fixture
def admin(
    installed: psycopg2.extensions.connection, model_settings: ServiceSettings
) -> Iterator[psycopg2.extensions.connection]:
    assert installed.info.dbname == model_settings.database_name
    with installed.cursor() as cur:
        cur.execute(
            "INSERT INTO "
            "users(email,user_name,encrypted_password,role,status,deleted) VALUES "
            "('control-test@example.com','control-test','unused','OPERATIONS','ACTIVE',false)"
        )
    installed.commit()
    yield installed


@pytest.fixture
def store(model_settings: ServiceSettings) -> ControlStore:
    return ControlStore(model_settings)


def queue(
    admin: psycopg2.extensions.connection, *, expired: bool = False
) -> CommandRef:
    command_id = str(uuid4())
    with admin.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE email='control-test@example.com'")
        actor = cur.fetchone()
        assert actor is not None
        cur.execute(
            "INSERT INTO "
            "control_command(command_id,wellhead_id,created_by,idempotency_key,reason,expected_revision,model_version,parameter_fingerprint,pump_on)"
            " VALUES(%s,1,%s,%s,'test',0,'0.3.0',%s,false)",
            (command_id, actor[0], str(uuid4()), "a" * 64),
        )
        expiry = datetime.now(timezone.utc) + timedelta(seconds=-1 if expired else 30)
        cur.execute(
            "UPDATE control_command SET "
            "status='queued',activated_by=created_by,activated_role='OPERATIONS',activated_session_version=0,queued_at=clock_timestamp(),expires_at=%s"
            " WHERE command_id=%s",
            (expiry, command_id),
        )
    admin.commit()
    return CommandRef(command_id, 1, actor[0])


def context() -> ControlContext:
    return ControlContext(1, "0.3.0", "a" * 64, "running", ControlInputs(), 0)


def test_apply_once_and_restore_active_over_old_snapshot(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    ref = queue(admin)
    assert store.scan()[1] == 1
    result = store.apply(ref, context())
    assert result.status == "applied" and result.active is not None
    assert result.active.revision == 1 and not result.active.controls.pump_on
    assert store.apply(ref, context()).status == "unchanged"
    restored = store.restore(context())
    assert restored.revision == 1 and not restored.controls.pump_on
    with admin.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM control_command_event WHERE event_type='applied'"
        )
        assert cur.fetchone() == (1,)


@pytest.mark.parametrize(
    "change", ["session_version=1", "role='USER'", "status='DISABLED'", "deleted=true"]
)
def test_revocation_after_queue_prevents_apply(
    admin: psycopg2.extensions.connection, store: ControlStore, change: str
) -> None:
    store.restore(context())
    ref = queue(admin)
    with admin.cursor() as cur:
        # Values above are test-owned SQL assignments, never request input.
        cur.execute("UPDATE users SET " + change + " WHERE id=%s", (ref.actor_id,))  # noqa: S608
    admin.commit()
    result = store.apply(ref, context())
    assert result.status == "rejected" and result.reason == "authority_revoked"
    assert store.read(1).revision == 0


def test_expired_command_never_replays(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    result = store.apply(queue(admin, expired=True), context())
    assert result.status == "expired"
    assert store.read(1).controls.pump_on


def test_inbox_expires_old_commands_without_applying_controls(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    ref = queue(admin, expired=True)
    assert store.expire_queued() == 1
    assert store.expire_queued() == 0
    assert store.scan()[1] == 0
    assert store.read(1).revision == 0
    assert store.read(1).controls.pump_on
    with admin.cursor() as cur:
        cur.execute(
            "SELECT event_type, reason_code FROM control_command_event "
            "WHERE command_id=%s",
            (ref.command_id,),
        )
        assert cur.fetchall() == [("expired", "expired")]


def test_changed_model_identity_rejects_queued_command(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    ref = queue(admin)
    changed = replace(context(), parameter_fingerprint="b" * 64)
    store.restore(changed)
    assert store.apply(ref, changed).reason == "parameters_changed"


def test_service_roles_cannot_edit_users_or_active_controls(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    with admin.cursor() as cur:
        cur.execute(
            "SELECT has_table_privilege(%s,'users','UPDATE'), "
            "has_table_privilege(%s,'wellhead_control_state','UPDATE'), "
            "has_table_privilege(%s,'control_command','DELETE'), "
            "has_table_privilege(%s,'control_command_event','DELETE')",
            (
                os.environ["TWIN_DB_USER"],
                os.environ["API_DB_USER"],
                os.environ["API_DB_USER"],
                os.environ["TWIN_DB_USER"],
            ),
        )
        assert cur.fetchone() == (False, False, False, False)


def test_terminal_command_and_proposal_are_immutable(
    admin: psycopg2.extensions.connection, store: ControlStore
) -> None:
    store.restore(context())
    ref = queue(admin)
    store.apply(ref, context())
    with pytest.raises(psycopg2.Error), admin.cursor() as cur:
        cur.execute(
            "UPDATE control_command SET status='queued',result_at=NULL WHERE "
            "command_id=%s",
            (ref.command_id,),
        )
    admin.rollback()
    with pytest.raises(psycopg2.Error), admin.cursor() as cur:
        cur.execute(
            "UPDATE control_command SET reason='changed' WHERE command_id=%s",
            (ref.command_id,),
        )
    admin.rollback()


def test_event_failure_rolls_back_active_controls(
    admin: psycopg2.extensions.connection,
    store: ControlStore,
) -> None:
    from psycopg2 import sql

    store.restore(context())
    ref = queue(admin)
    role = sql.Identifier(os.environ["TWIN_DB_USER"])
    with admin.cursor() as cur:
        cur.execute(
            sql.SQL("REVOKE INSERT ON control_command_event FROM {}").format(role)
        )
    admin.commit()
    try:
        with pytest.raises(DatabaseUnavailableError):
            store.apply(ref, context())
        assert store.read(1).revision == 0
        assert store.scan()[1] == 1
    finally:
        with admin.cursor() as cur:
            cur.execute(
                sql.SQL("GRANT INSERT ON control_command_event TO {}").format(role)
            )
        admin.commit()


def test_lost_commit_reply_is_reconciled_from_the_database(
    admin: psycopg2.extensions.connection,
    store: ControlStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store.restore(context())
    ref = queue(admin)

    class LostReply(psycopg2.extensions.connection):
        def commit(self) -> None:
            super().commit()
            raise psycopg2.OperationalError("test lost reply after commit")

    def connect() -> psycopg2.extensions.connection:
        settings = store.settings
        return psycopg2.connect(
            host=settings.database_host,
            port=settings.database_port,
            dbname=settings.database_name,
            user=settings.database_user,
            password=settings.database_password,
            connection_factory=LostReply,
        )

    monkeypatch.setattr(store, "_connect", connect)
    with pytest.raises(ControlCommitUnknownError):
        store.apply(ref, context())
    active = store.read(1)
    assert active.revision == 1 and not active.controls.pump_on
