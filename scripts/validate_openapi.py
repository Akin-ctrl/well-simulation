"""Validate committed OpenAPI specifications for basic contract hygiene."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml


OPENAPI_DIR = Path(__file__).resolve().parents[1] / "docs" / "openapi"


def fail(message: str) -> None:
    raise ValueError(message)


def as_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def collect_refs(value: Any) -> set[str]:
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


def resolve_ref(document: dict[str, Any], ref: str) -> bool:
    if not ref.startswith("#/"):
        return False

    current: Any = document
    for part in ref.removeprefix("#/").split("/"):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]

    return True


def validate_operation(path: str, method: str, operation: Any) -> list[str]:
    errors: list[str] = []
    operation_map = as_mapping(operation, f"{method.upper()} {path}")

    if not operation_map.get("operationId"):
        errors.append(f"{method.upper()} {path} is missing operationId")

    responses = operation_map.get("responses")
    if not isinstance(responses, dict) or not responses:
        errors.append(f"{method.upper()} {path} is missing responses")
        return errors

    if "200" not in responses and "201" not in responses and "202" not in responses:
        errors.append(f"{method.upper()} {path} has no success response")

    return errors


def validate_document(path: Path) -> list[str]:
    errors: list[str] = []
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document_map = as_mapping(document, str(path))

    openapi_version = document_map.get("openapi")
    if not isinstance(openapi_version, str) or not openapi_version.startswith("3."):
        errors.append("openapi must be a 3.x version string")

    info = as_mapping(document_map.get("info"), "info")
    if not info.get("title"):
        errors.append("info.title is required")
    if not info.get("version"):
        errors.append("info.version is required")

    paths = as_mapping(document_map.get("paths"), "paths")
    if not paths:
        errors.append("paths must not be empty")

    operation_ids: set[str] = set()
    for route, path_item in paths.items():
        path_item_map = as_mapping(path_item, f"path {route}")
        for method, operation in path_item_map.items():
            if method.lower() not in {
                "get",
                "post",
                "put",
                "patch",
                "delete",
                "options",
                "head",
            }:
                continue

            operation_map = as_mapping(operation, f"{method.upper()} {route}")
            operation_id = operation_map.get("operationId")
            if isinstance(operation_id, str):
                if operation_id in operation_ids:
                    errors.append(f"duplicate operationId: {operation_id}")
                operation_ids.add(operation_id)

            errors.extend(validate_operation(route, method, operation_map))

    for ref in collect_refs(document_map):
        if not resolve_ref(document_map, ref):
            errors.append(f"unresolved or external $ref: {ref}")

    return errors


def main() -> int:
    spec_paths = sorted(OPENAPI_DIR.glob("*.yaml"))
    if not spec_paths:
        print(f"No OpenAPI specs found in {OPENAPI_DIR}", file=sys.stderr)
        return 1

    failures: list[str] = []
    for spec_path in spec_paths:
        try:
            errors = validate_document(spec_path)
        except Exception as exc:
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
