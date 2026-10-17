"""Limit engine comparison equivalence to the verified literal-array casts."""

from __future__ import annotations

import json

import pytest

from schema_catalog import differences
from schema_normalize import normalize_text_arrays


@pytest.mark.parametrize("literal", ["'success'", "'can''t'", "'::character varying'"])
def test_whole_array_and_element_casts_compare_equal(literal: str) -> None:
    whole = f"ANY ((ARRAY[{literal}::character varying])::text[])"
    elements = f"ANY (ARRAY[({literal}::character varying)::text])"
    assert normalize_text_arrays(whole) == normalize_text_arrays(elements)
    assert normalize_text_arrays(whole) == f"ANY (ARRAY[{literal}::text])"


@pytest.mark.parametrize(
    "expression",
    [
        "ARRAY[('a'::character varying(2))::text]",
        "ARRAY[('a'::character)::text]",
        "ARRAY[(some_column::character varying)::text]",
        "ARRAY[('a'::character varying)::integer]",
        "ARRAY[(NULL::character varying)::text]",
        "'ARRAY[(''a''::character varying)::text]'",
        "\"ARRAY[('a'::character varying)::text]\"",
        "$$ARRAY[('a'::character varying)::text]$$",
        "/* ARRAY[('a'::character varying)::text] */ SELECT 1",
        "-- ARRAY[('a'::character varying)::text]\nSELECT 1",
    ],
)
def test_other_casts_and_quoted_sql_remain_unchanged(expression: str) -> None:
    assert normalize_text_arrays(expression) == expression


def test_changed_values_still_compare_differently() -> None:
    original = "ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[])"
    for changed in ("'x'", "'b'"):
        candidate = original.replace("'a'", changed)
        assert normalize_text_arrays(original) != normalize_text_arrays(candidate)


def test_duplicate_catalog_entries_are_not_hidden_by_comparison() -> None:
    row = json.dumps(["parameterreading", "policy_compression"])
    assert differences({"jobs": [row]}, {"jobs": [row, row]})


@pytest.mark.parametrize("column", ["interval_seconds", "horizon_seconds"])
def test_bounded_minutes_restore_preserves_all_comparisons(column: str) -> None:
    from schema_normalize import normalize_bounded_minutes

    fresh = (
        f"CHECK (((({column} >= 300) AND ({column} <= 1800)) "
        f"AND (({column} % 60) = 0)))"
    )
    restored = (
        f"CHECK ((({column} >= 300) AND ({column} <= 1800) AND (({column} % 60) = 0)))"
    )
    assert normalize_bounded_minutes(fresh) == restored
    assert normalize_bounded_minutes(restored) == restored

    def row(definition: str, valid: bool = True) -> str:
        return json.dumps(["forecast_schedule", "bounds", definition, valid])

    expected = {"constraints": [row(fresh)]}
    assert not differences(expected, {"constraints": [row(restored)]})
    assert differences(expected, {"constraints": [row(restored, False)]})
    for before, after in [
        ("300", "301"),
        ("1800", "1860"),
        ("60", "30"),
        ("= 0", "= 1"),
        (">=", ">"),
        ("AND", "OR"),
        (column, "another_column"),
    ]:
        assert differences(
            expected, {"constraints": [row(restored.replace(before, after))]}
        )
    for other in [
        "'" + fresh + "'",
        "/* " + fresh + " */",
        fresh + " NOT VALID",
        fresh.replace("AND", "OR"),
        fresh.replace("300", "some_function()"),
    ]:
        assert normalize_bounded_minutes(other) == other
