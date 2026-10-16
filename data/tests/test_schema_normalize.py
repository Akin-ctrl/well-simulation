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
