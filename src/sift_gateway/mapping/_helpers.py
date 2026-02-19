"""Shared helpers for the mapping subsystem.

Provides JSON type classification, canonical JSONPath segment
formatting, and deterministic type sort keys used by the full,
partial, and schema mapping modules.
"""

from __future__ import annotations

import re
from typing import Any

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def json_type_name(value: Any) -> str:
    """Return a JSON-style type name for a Python value.

    Uses ``"number"`` for both ``int`` and ``float``, matching the
    JSON spec which has no separate integer type.

    Args:
        value: Any Python value to classify.

    Returns:
        One of ``"null"``, ``"boolean"``, ``"number"``, ``"string"``,
        ``"array"``, ``"object"``, or the Python type name.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def normalize_path_segment(key: str) -> str:
    """Normalize a key to canonical JSONPath segment form.

    Simple identifiers use dot notation (e.g. ``.foo``).
    Others use bracket-quote notation (e.g. ``['my key']``).

    Args:
        key: Object key string.

    Returns:
        A JSONPath segment string.
    """
    if _IDENT_RE.match(key):
        return f".{key}"
    escaped = (
        key.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f"['{escaped}']"


def type_sort_key(type_name: str) -> tuple[int, str]:
    """Sort JSON types in a stable, human-readable order.

    Args:
        type_name: A JSON type label (e.g. ``"string"``).

    Returns:
        A ``(priority, name)`` tuple for deterministic sorting.
    """
    preferred_order = {
        "null": 0,
        "boolean": 1,
        "number": 2,
        "string": 3,
        "array": 4,
        "object": 5,
    }
    return preferred_order.get(type_name, 99), type_name
