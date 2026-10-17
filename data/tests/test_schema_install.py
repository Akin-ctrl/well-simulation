"""Reference integrity and parser failures that do not require a database."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from schema import CONTRACT_PATH, SCHEMA_PATH
from schema_install import SchemaContract
from schema_sql import SqlFile, split_statements


def test_reference_is_bound_to_current_source() -> None:
    contract = SchemaContract.load(CONTRACT_PATH, SqlFile.load(SCHEMA_PATH))
    assert len(contract.legacy_checksums) == 13
    assert len(contract.catalog["hypertables"]) == 7
    assert len(contract.catalog["aggregates"]) == 4


def test_stale_reference_is_rejected(tmp_path: Path) -> None:
    changed = tmp_path / "schema.sql"
    changed.write_text(SCHEMA_PATH.read_text() + "\n-- changed\n")
    with pytest.raises(RuntimeError, match="rebuild and verify"):
        SchemaContract.load(CONTRACT_PATH, SqlFile.load(changed))


def test_missing_comparison_section_is_rejected(tmp_path: Path) -> None:
    raw = json.loads(CONTRACT_PATH.read_text())
    del raw["catalog"]["jobs"]
    changed = tmp_path / "contract.json"
    changed.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="comparison sections"):
        SchemaContract.load(changed, SqlFile.load(SCHEMA_PATH))


@pytest.mark.parametrize(
    "body",
    [
        """SELECT "semi;colon"; SELECT 2;""",
        "SELECT 'can''t; split'; SELECT 2;",
        "SELECT E'can\\'t; split'; SELECT 2;",
        "/* outer /* inner ; */ still ; comment */ SELECT 1; SELECT 2;",
    ],
)
def test_quoted_separators_and_nested_comments(body: str) -> None:
    statements = split_statements(body)
    assert len(statements) == 2
    assert statements[-1] == "SELECT 2"


@pytest.mark.parametrize(
    "body",
    ["SELECT 'unfinished", 'SELECT "unfinished', "DO $$ unfinished", "/* unfinished"],
)
def test_incomplete_source_is_rejected_before_install(body: str) -> None:
    with pytest.raises(ValueError, match="Unterminated SQL"):
        split_statements(body)
