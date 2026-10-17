"""Tests for canonical SQL loading and statement splitting.

The statement splitter exists because TimescaleDB refuses to create a
continuous aggregate inside a transaction block, and Postgres wraps any
multi-statement simple query in one. Splitting wrongly would either merge
statements back into an implicit transaction or cut a function body in half.
"""

from __future__ import annotations

from pathlib import Path

from schema_sql import SqlFile, split_statements


class TestSplitStatements:
    def test_splits_on_top_level_semicolons(self) -> None:
        assert split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]

    def test_ignores_trailing_whitespace_and_empty_statements(self) -> None:
        assert split_statements("SELECT 1;;\n\n  ;") == ["SELECT 1"]

    def test_statement_without_trailing_semicolon_is_kept(self) -> None:
        assert split_statements("SELECT 1") == ["SELECT 1"]

    def test_semicolon_inside_string_literal_is_not_a_separator(self) -> None:
        sql = "SELECT ';' AS a; SELECT 2;"
        assert split_statements(sql) == ["SELECT ';' AS a", "SELECT 2"]

    def test_semicolon_inside_line_comment_is_not_a_separator(self) -> None:
        sql = "SELECT 1 -- trailing ; comment\n; SELECT 2;"
        statements = split_statements(sql)
        assert len(statements) == 2
        assert statements[1] == "SELECT 2"

    def test_semicolon_inside_block_comment_is_not_a_separator(self) -> None:
        sql = "/* a ; b */ SELECT 1; SELECT 2;"
        assert split_statements(sql) == ["/* a ; b */ SELECT 1", "SELECT 2"]

    def test_dollar_quoted_body_is_kept_whole(self) -> None:
        """A plpgsql body is full of semicolons and must survive intact."""
        sql = (
            "CREATE FUNCTION f() RETURNS void AS $$\n"
            "BEGIN\n  PERFORM 1;\n  PERFORM 2;\nEND;\n"
            "$$ LANGUAGE plpgsql;\n"
            "SELECT 1;"
        )
        statements = split_statements(sql)
        assert len(statements) == 2
        assert "PERFORM 1;" in statements[0]
        assert "PERFORM 2;" in statements[0]
        assert statements[1] == "SELECT 1"

    def test_tagged_dollar_quote_is_kept_whole(self) -> None:
        sql = "DO $tag$ BEGIN PERFORM 1; END $tag$; SELECT 2;"
        statements = split_statements(sql)
        assert len(statements) == 2
        assert statements[1] == "SELECT 2"

    def test_canonical_aggregates_split_cleanly(self) -> None:
        """The file this splitter exists for must produce runnable statements."""
        path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
        statements = split_statements(path.read_text(encoding="utf-8"))

        assert len(statements) > 1
        assert not any(s.endswith(";") for s in statements)
        creates = [s for s in statements if "CREATE MATERIALIZED VIEW" in s]
        assert len(creates) == 4
        # Each aggregate must arrive as one statement, not be split mid-body.
        for create in creates:
            assert "GROUP BY" in create


class TestSqlFile:
    def _write(self, tmp_path: Path, name: str, body: str) -> SqlFile:
        path = tmp_path / name
        path.write_text(body, encoding="utf-8")
        return SqlFile.load(path)

    def test_checksum_changes_with_content(self, tmp_path: Path) -> None:
        a = self._write(tmp_path, "a.sql", "SELECT 1;")
        b = self._write(tmp_path, "b.sql", "SELECT 2;")
        assert a.checksum != b.checksum

    def test_checksum_is_stable(self, tmp_path: Path) -> None:
        body = "SELECT 1;"
        assert (
            self._write(tmp_path, "a.sql", body).checksum
            == self._write(tmp_path, "b.sql", body).checksum
        )
