"""Resolve JSON-encoded strings embedded within JSON values.

Upstream MCP tools commonly return ``structuredContent`` with
double-encoded JSON (e.g. ``{"result": "{\\\"data\\\": [...]}"``).
This module provides ``resolve_json_strings`` to recursively
parse such strings so that the mapper and retrieval pipelines
see the actual nested structure.
"""

from __future__ import annotations

import json
from typing import Any


def resolve_json_strings(
    value: Any,
    *,
    max_depth: int = 3,
    _current_depth: int = 0,
) -> Any:
    """Recursively parse JSON-encoded strings within a JSON value.

    Walk the value tree and replace string leaves that decode to
    ``dict`` or ``list`` with their parsed equivalents.  Scalar
    JSON strings (numbers, booleans, null encoded as strings) are
    left unchanged.

    Args:
        value: Any JSON-compatible Python value.
        max_depth: Maximum recursion depth to prevent runaway
            parsing of pathologically nested JSON strings.
        _current_depth: Internal counter (callers should not set).

    Returns:
        A new value tree with JSON-encoded strings resolved.
        The original value is not mutated.
    """
    if _current_depth >= max_depth:
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
        if isinstance(parsed, (dict, list)):
            return resolve_json_strings(
                parsed,
                max_depth=max_depth,
                _current_depth=_current_depth + 1,
            )
        return value

    if isinstance(value, dict):
        return {
            k: resolve_json_strings(
                v,
                max_depth=max_depth,
                _current_depth=_current_depth,
            )
            for k, v in value.items()
        }

    if isinstance(value, list):
        return [
            resolve_json_strings(
                item,
                max_depth=max_depth,
                _current_depth=_current_depth,
            )
            for item in value
        ]

    return value
