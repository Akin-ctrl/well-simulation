"""Validate committed OpenAPI specifications for basic contract hygiene."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, NoReturn

import yaml

OPENAPI_DIR = Path(__file__).resolve().parents[1] / "docs" / "openapi"

HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "options", "head"})

SUCCESS_STATUS_CODES = frozenset({"200", "201", "202"})

# Mapping parsed from YAML. Values stay Any because a specification document is
# arbitrarily nested; every read below narrows with isinstance before use.
Document = dict[str, Any]


def fail(message: str) -> NoReturn:
    """Raise a validation error with the supplied message."""
    raise ValueError(message)


def as_mapping(value: object, label: str) -> Document:
    """Return the value as a mapping, or fail with a labelled error."""
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def collect_refs(value: object) -> set[str]:
    """Collect every `$ref` string anywhere within a parsed document."""
    refs: set[str] = set()
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str):
            refs.add(ref)
        for child in value.values():
            refs.update(collect_refs(child))
    elif isinstance(value, list):
        for child in value:
            refs.update(collect_refs(child))
    return refs


def resolve_ref(document: Document, ref: str) -> bool:
    """Report whether an internal `#/`-style reference resolves within the document."""
    if not ref.startswith("#/"):
        return False

    current: object = document
    for part in ref.removeprefix("#/").split("/"):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]

    return True


def validate_operation(path: str, method: str, operation: Document) -> list[str]:
    """Check that one operation declares an id and a success response."""
    errors: list[str] = []
    label = f"{method.upper()} {path}"

    if not operation.get("operationId"):
        errors.append(f"{label} is missing operationId")

    responses = operation.get("responses")
    if not isinstance(responses, dict) or not responses:
        errors.append(f"{label} is missing responses")
        return errors

    if not SUCCESS_STATUS_CODES & set(responses):
        errors.append(f"{label} has no success response")

    return errors


def validate_metadata(document: Document) -> list[str]:
    """Check the top-level `openapi` version and `info` block."""
    errors: list[str] = []

    version = document.get("openapi")
    if not isinstance(version, str) or not version.startswith("3."):
        errors.append("openapi must be a 3.x version string")

    info = as_mapping(document.get("info"), "info")
    if not info.get("title"):
        errors.append("info.title is required")
    if not info.get("version"):
        errors.append("info.version is required")

    return errors


def validate_paths(paths: Document) -> list[str]:
    """Check every operation under `paths` and enforce unique operation ids."""
    errors: list[str] = []
    operation_ids: set[str] = set()

    for route, path_item in paths.items():
        path_item_map = as_mapping(path_item, f"path {route}")

        for method, operation in path_item_map.items():
            if method.lower() not in HTTP_METHODS:
                continue

            operation_map = as_mapping(operation, f"{method.upper()} {route}")

            operation_id = operation_map.get("operationId")
            if isinstance(operation_id, str):
                if operation_id in operation_ids:
                    errors.append(f"duplicate operationId: {operation_id}")
                operation_ids.add(operation_id)

            errors.extend(validate_operation(route, method, operation_map))

    return errors


def validate_refs(document: Document) -> list[str]:
    """Check that every `$ref` in the document resolves internally."""
    return [
        f"unresolved or external $ref: {ref}"
        for ref in collect_refs(document)
        if not resolve_ref(document, ref)
    ]


def validate_document(path: Path) -> list[str]:
    """Validate one specification file and return every problem found."""
    document = as_mapping(yaml.safe_load(path.read_text(encoding="utf-8")), str(path))

    errors = validate_metadata(document)

    paths = as_mapping(document.get("paths"), "paths")
    if not paths:
        errors.append("paths must not be empty")
    errors.extend(validate_paths(paths))

    errors.extend(validate_refs(document))
    return errors


def main() -> int:
    """Validate every committed specification and report the outcome."""
    spec_paths = sorted(OPENAPI_DIR.glob("*.yaml"))
    if not spec_paths:
        print(f"No OpenAPI specs found in {OPENAPI_DIR}", file=sys.stderr)
        return 1

    failures: list[str] = []
    for spec_path in spec_paths:
        try:
            errors = validate_document(spec_path)
        except (ValueError, yaml.YAMLError, OSError) as exc:
            errors = [str(exc)]

        if errors:
            failures.extend(f"{spec_path}: {error}" for error in errors)
        else:
            print(f"validated {spec_path.relative_to(OPENAPI_DIR.parents[1])}")

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
