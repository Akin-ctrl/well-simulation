"""Compare equivalent PostgreSQL renderings without ignoring SQL value changes."""

from __future__ import annotations

import json
import re

from schema_sql import skip_region

# PostgreSQL 14 casts each unbounded varchar literal to text. PostgreSQL 16 can
# cast the entire array instead. Only these two exact forms are interchangeable;
# bounded varchar casts, other types, nulls, and expressions remain untouched.
LITERAL = r"'(?:[^']|'')*'"
VARCHAR = LITERAL + r"::character varying"
ARRAY_CASTS = (
    re.compile(r"\(ARRAY\[(" + VARCHAR + r"(?:, " + VARCHAR + r")*)\]\)::text\[\]"),
    re.compile(
        r"ARRAY\[(\(" + VARCHAR + r"\)::text(?:, \(" + VARCHAR + r"\)::text)*)\]"
    ),
)
VARCHAR_VALUE = re.compile("(" + LITERAL + r")::character varying")


def _array_match(definition: str, index: int) -> re.Match[str] | None:
    """Recognize either supported array cast at the current SQL position."""
    for pattern in ARRAY_CASTS:
        match = pattern.match(definition, index)
        if match is not None:
            return match
    return None


def normalize_text_arrays(definition: str) -> str:
    """Canonicalize only literal arrays, preserving quoted SQL and comments."""
    result: list[str] = []
    index = 0
    while index < len(definition):
        end = skip_region(definition, index)
        if end is not None:
            result.append(definition[index:end])
            index = end
            continue
        match = _array_match(definition, index)
        if match is None:
            result.append(definition[index])
            index += 1
            continue
        values = [m.group(1) + "::text" for m in VARCHAR_VALUE.finditer(match.group(1))]
        result.append("ARRAY[" + ", ".join(values) + "]")
        index = match.end()
    return "".join(result)


def comparison_rows(section: str, rows: list[str]) -> list[str]:
    """Normalize known deparser differences only in constraint and view SQL."""
    position = {"constraints": 2, "aggregates": 5}.get(section)
    if position is None:
        return sorted(rows)
    result: list[str] = []
    for row in rows:
        values = json.loads(row)
        values[position] = normalize_text_arrays(values[position])
        result.append(json.dumps(values, sort_keys=True))
    return sorted(result)
