"""Contract tests binding the Drizzle read layer to the canonical SQL schema.

ADR 0006 makes the SQL migrations canonical and the Drizzle schema a typed
mirror of them. Nothing enforced that, so the two drifted: enum values and
table definitions were maintained independently in both places.

These tests are the cheap half of that enforcement. They compare the names
declared on each side. A full introspection diff, which would also compare
column types, belongs in CI against a scratch database.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "data" / "sql" / "migrations"
DRIZZLE_SCHEMAS = REPO_ROOT / "monorepo" / "packages" / "db" / "schemas"


def migration_sql() -> str:
    """Return every migration concatenated, with original casing preserved.

    Identifiers are compared case-insensitively via the patterns below; the
    text itself must not be lowercased or enum *values* would be corrupted
    along with the keywords.
    """
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(MIGRATIONS_DIR.glob("*.sql"))
    )


def sql_tables() -> set[str]:
    """Return every table name created by the migrations."""
    return {
        name.lower()
        for name in re.findall(
            r"create table if not exists\s+([a-zA-Z_]+)",
            migration_sql(),
            re.IGNORECASE,
        )
    }


def sql_enums() -> dict[str, list[str]]:
    """Return every enum type created by the migrations, with its values."""
    enums: dict[str, list[str]] = {}
    for name, body in re.findall(
        r"create type\s+\"?([a-zA-Z_]+)\"?\s+as enum\s*\(([^)]*)\)",
        migration_sql(),
        re.IGNORECASE,
    ):
        enums[name.lower()] = re.findall(r"'([^']+)'", body)
    return enums


def drizzle_source() -> str:
    """Return every Drizzle schema module concatenated."""
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(DRIZZLE_SCHEMAS.glob("*.ts"))
    )


def drizzle_code() -> str:
    """Return the Drizzle source with comments stripped.

    Checks for forbidden identifiers must not fire on prose explaining why the
    identifier is forbidden.
    """
    without_blocks = re.sub(r"/\*.*?\*/", "", drizzle_source(), flags=re.DOTALL)
    return re.sub(r"//[^\n]*", "", without_blocks)


def drizzle_tables() -> set[str]:
    """Return every table name declared via pgTable."""
    return set(re.findall(r"pgTable\(\s*['\"]([a-z_]+)['\"]", drizzle_source()))


def drizzle_views() -> set[str]:
    """Return every view name declared via pgView."""
    return set(re.findall(r"pgView\(\s*['\"]([a-z_0-9]+)['\"]", drizzle_source()))


def drizzle_enums() -> dict[str, list[str]]:
    """Return every enum declared via pgEnum, with its values."""
    enums: dict[str, list[str]] = {}
    for name, body in re.findall(
        r"pgEnum\(\s*['\"]([a-z_]+)['\"]\s*,\s*\[([^\]]*)\]", drizzle_source()
    ):
        enums[name] = re.findall(r"['\"]([^'\"]+)['\"]", body)
    return enums


class TestEnums:
    """Enum values are defined in both places and must agree exactly."""

    def test_drizzle_declares_no_unknown_enum(self) -> None:
        assert set(drizzle_enums()) <= set(sql_enums())

    @pytest.mark.parametrize("enum_name", sorted(drizzle_enums()))
    def test_values_match_sql(self, enum_name: str) -> None:
        assert drizzle_enums()[enum_name] == sql_enums()[enum_name], (
            f"'{enum_name}' differs between the SQL migrations and "
            f"packages/db/schemas/enums.ts"
        )


class TestSharedRoleContract:
    """The role list now exists in SQL, Drizzle, and the shared DTO package.

    ADR 0033 gates authorization on it, so a value present in one place and
    missing from another is an authorization bug, not a typo.
    """

    def dto_roles(self) -> list[str]:
        source = (
            REPO_ROOT / "monorepo" / "packages" / "dto" / "auth" / "roles.ts"
        ).read_text(encoding="utf-8")
        match = re.search(r"export const ROLES = \[([^\]]*)\]", source)
        assert match, "ROLES not found in packages/dto/auth/roles.ts"
        return re.findall(r"'([^']+)'", match.group(1))

    def test_dto_roles_match_sql_enum(self) -> None:
        assert sorted(self.dto_roles()) == sorted(sql_enums()["role"])

    def test_dto_roles_match_drizzle_enum(self) -> None:
        assert sorted(self.dto_roles()) == sorted(drizzle_enums()["role"])


class TestTables:
    def test_every_drizzle_table_exists_in_sql(self) -> None:
        missing = drizzle_tables() - sql_tables()
        assert not missing, (
            f"Drizzle declares tables the SQL schema does not create: {sorted(missing)}"
        )


class TestViews:
    def test_every_drizzle_view_exists_in_sql(self) -> None:
        declared = {
            name.lower()
            for name in re.findall(
                r"create (?:or replace )?(?:materialized )?view\s+([a-zA-Z_0-9]+)",
                migration_sql(),
                re.IGNORECASE,
            )
        }
        missing = drizzle_views() - declared
        assert not missing, (
            f"Drizzle declares views the SQL schema does not create: {sorted(missing)}"
        )

    def test_no_reference_to_timescale_internals(self) -> None:
        """Hypertable numbering is not stable across a rebuild."""
        assert "_timescaledb_internal" not in drizzle_code()

    def test_view_names_state_their_real_bucket_width(self) -> None:
        """They were named hourly and daily while bucketing at 2 and 5 minutes."""
        names = drizzle_views()
        assert not any("hourly" in n or "daily" in n for n in names), names
