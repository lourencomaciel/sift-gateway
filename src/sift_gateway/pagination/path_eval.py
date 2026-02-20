"""Shared JSONPath first-match helper used by pagination modules."""

from __future__ import annotations

from typing import Any

from sift_gateway.query.jsonpath import JsonPathError, evaluate_jsonpath


def evaluate_path(data: Any, path: str) -> Any | None:
    """Evaluate a JSONPath and return the first match."""
    if not path:
        return None
    try:
        matches = evaluate_jsonpath(data, path)
    except JsonPathError:
        return None
    if not matches:
        return None
    return matches[0]
