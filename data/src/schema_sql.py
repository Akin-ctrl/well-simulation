"""Load SQL files and split top-level statements without splitting function bodies."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SqlFile:
    """One SQL file and how it must be applied."""

    path: Path
    sql: str
    checksum: str

    @property
    def name(self) -> str:
        """Return the filename used as the applied-SQL file key."""
        return self.path.name

    @classmethod
    def load(cls, path: Path) -> SqlFile:
        """Read a SQL file and compute its checksum."""
        sql = path.read_text(encoding="utf-8")
        return cls(
            path=path,
            sql=sql,
            checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        )


DOLLAR_TAG_PATTERN = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def _quoted_end(sql: str, start: int) -> int:
    """Skip SQL strings or identifiers, including doubled delimiters."""
    quote = sql[start]
    escapes = start > 0 and sql[start - 1] in "eE" and quote == "'"
    index = start + 1
    while index < len(sql):
        if (escapes and sql[index] == "\\") or sql[index : index + 2] == quote * 2:
            index += 2
        elif sql[index] == quote:
            return index + 1
        else:
            index += 1
    raise ValueError("Unterminated SQL quoted value")


def _comment_end(sql: str, start: int) -> int:
    """Skip a block comment, including nested comments allowed by PostgreSQL."""
    depth = 1
    index = start + 2
    while index < len(sql):
        pair = sql[index : index + 2]
        if pair == "/*":
            depth += 1
            index += 2
        elif pair == "*/":
            depth -= 1
            index += 2
            if depth == 0:
                return index
        else:
            index += 1
    raise ValueError("Unterminated SQL block comment")


def skip_region(sql: str, index: int) -> int | None:
    """If a non-splittable region starts at `index`, return the index past it.

    Comments, string literals, and dollar-quoted bodies may all contain
    semicolons that are not statement separators.
    """
    rest = sql[index:]

    if rest.startswith("--"):
        end = sql.find("\n", index)
        return len(sql) if end == -1 else end + 1
    if rest.startswith("/*"):
        return _comment_end(sql, index)
    if rest.startswith(("'", '"')):
        return _quoted_end(sql, index)

    tag = DOLLAR_TAG_PATTERN.match(rest)
    if tag:
        end = sql.find(tag.group(0), index + len(tag.group(0)))
        if end == -1:
            raise ValueError("Unterminated SQL function body")
        return end + len(tag.group(0))

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
        skipped = skip_region(sql, index)
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


def discover(directory: Path) -> list[SqlFile]:
    """Return every .sql file in a directory, in filename order."""
    if not directory.is_dir():
        return []
    return [SqlFile.load(path) for path in sorted(directory.glob("*.sql"))]
