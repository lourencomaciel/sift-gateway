"""Full mapping: parse fully, discover roots, build inventory."""
from __future__ import annotations

import re
from typing import Any

from mcp_artifact_gateway.mapping.runner import RootInventory

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Maximum number of elements to sample for field type discovery
_FIELD_SAMPLE_LIMIT = 50


def _normalize_path_segment(key: str) -> str:
    """Normalize a key to canonical JSONPath segment form."""
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


def _json_type_name(value: Any) -> str:
    """Return a JSON-style type name for a Python value.

    Uses "number" for both int and float, matching the JSON spec
    (which has no separate integer type).
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


def _build_fields_top(elements: list[Any], sample_limit: int = _FIELD_SAMPLE_LIMIT) -> dict[str, Any]:
    """Build fields_top from sampled elements: field -> {type -> count}."""
    field_types: dict[str, dict[str, int]] = {}
    sampled = elements[:sample_limit]

    for elem in sampled:
        if not isinstance(elem, dict):
            continue
        for key, val in elem.items():
            type_name = _json_type_name(val)
            if key not in field_types:
                field_types[key] = {}
            field_types[key][type_name] = field_types[key].get(type_name, 0) + 1

    return field_types


def _score_root(value: Any) -> float:
    """Score a root by size/relevance. Larger collections score higher."""
    if isinstance(value, list):
        return float(len(value))
    if isinstance(value, dict):
        return float(len(value))
    return 0.0


def _build_root_inventory(
    root_key: str,
    root_path: str,
    value: Any,
) -> RootInventory:
    """Build a RootInventory for a discovered root."""
    if isinstance(value, list):
        count = len(value)
        shape = "array"
        fields_top = _build_fields_top(value) if count > 0 else None
        score = float(count)
        coverage = 1.0
        summary: dict[str, Any] = {"element_count": count}
    elif isinstance(value, dict):
        count = len(value)
        shape = "object"
        # For objects, treat values as the "elements"
        vals = list(value.values())
        fields_top = _build_fields_top(vals) if count > 0 else None
        score = float(count)
        coverage = 1.0
        summary = {"key_count": count}
    else:
        count = None
        shape = None
        fields_top = None
        score = 0.0
        coverage = None
        summary = {}

    return RootInventory(
        root_key=root_key,
        root_path=root_path,
        count_estimate=count,
        root_shape=shape,
        fields_top=fields_top,
        root_summary=summary,
        inventory_coverage=coverage,
        root_score=score,
        sample_indices=None,
        prefix_coverage=False,
        stop_reason=None,
        sampled_prefix_len=None,
    )


def run_full_mapping(
    json_value: Any,
    *,
    max_roots: int = 3,
) -> list[RootInventory]:
    """Parse JSON value fully and discover up to K roots.

    Root discovery rules:
    - If value is an array: root at "$" with shape "array"
    - If value is an object: check top-level keys for arrays/objects
    - Discover up to max_roots roots, scored by size/relevance
    - Each root gets full inventory: count, fields_top, etc.
    """
    if isinstance(json_value, list):
        # Root-level array: single root at "$"
        root = _build_root_inventory(
            root_key="$",
            root_path="$",
            value=json_value,
        )
        return [root]

    if isinstance(json_value, dict):
        # Object at root: examine top-level keys for arrays/objects
        candidates: list[tuple[float, str, str, Any]] = []

        for key, val in json_value.items():
            if isinstance(val, (list, dict)):
                path_segment = _normalize_path_segment(key)
                root_path = f"${path_segment}"
                score = _score_root(val)
                candidates.append((score, key, root_path, val))

        # Sort by score descending, then by key ascending for determinism
        candidates.sort(key=lambda c: (-c[0], c[1]))

        roots: list[RootInventory] = []
        for score, key, root_path, val in candidates[:max_roots]:
            root = _build_root_inventory(
                root_key=key,
                root_path=root_path,
                value=val,
            )
            roots.append(root)

        # If no nested arrays/objects found, create a root for the object itself
        if not roots:
            root = _build_root_inventory(
                root_key="$",
                root_path="$",
                value=json_value,
            )
            roots.append(root)

        return roots

    # Scalar value at root: no meaningful roots
    return [
        RootInventory(
            root_key="$",
            root_path="$",
            count_estimate=None,
            root_shape=None,
            fields_top=None,
            root_summary={},
            inventory_coverage=None,
            root_score=0.0,
            sample_indices=None,
            prefix_coverage=False,
            stop_reason=None,
            sampled_prefix_len=None,
        )
    ]
