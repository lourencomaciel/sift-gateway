"""Perform full in-memory mapping of parsed JSON values.

Walk the complete JSON structure to discover collection roots
(arrays and top-level object keys), score them by size, and
build ``RootInventory`` objects with exact counts and field
type distributions.  Key export is ``run_full_mapping``.
"""

from __future__ import annotations

from typing import Any

from sift_gateway.mapping._helpers import json_type_name
from sift_gateway.mapping.runner import RootInventory

# Maximum number of elements to sample for field type discovery
_FIELD_SAMPLE_LIMIT = 50


def _build_fields_top(
    elements: list[Any],
    sample_limit: int = _FIELD_SAMPLE_LIMIT,
) -> dict[str, Any]:
    """Build field type distributions from sampled elements.

    Args:
        elements: List of elements (dicts are inspected).
        sample_limit: Maximum elements to sample.

    Returns:
        A dict mapping field names to {type_name: count} dicts.
    """
    field_types: dict[str, dict[str, int]] = {}
    sampled = elements[:sample_limit]

    for elem in sampled:
        if not isinstance(elem, dict):
            continue
        for key, val in elem.items():
            type_name = json_type_name(val)
            if key not in field_types:
                field_types[key] = {}
            field_types[key][type_name] = field_types[key].get(type_name, 0) + 1

    return field_types


def _build_root_inventory(
    root_key: str,
    root_path: str,
    value: Any,
) -> RootInventory:
    """Build a RootInventory for a discovered root.

    Args:
        root_key: Key identifying this root.
        root_path: Canonical JSONPath to the root.
        value: The JSON value at this root.

    Returns:
        A RootInventory with exact counts and field types.
    """
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
        # Treat the object itself as a single record so fields_top
        # shows the object's own keys (matching what select can project).
        fields_top = _build_fields_top([value]) if count > 0 else None
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
    """Map the canonical root ``$`` for a parsed JSON value.

    Root mapping is intentionally anchored at ``$`` so code-query
    execution always receives the complete resolved payload tree.
    JSON-encoded strings are recursively resolved before root
    inventory is built.

    Args:
        json_value: Fully parsed JSON value to analyze.
        max_roots: Unused; retained for backward compatibility.

    Returns:
        A single-element list containing the ``$`` root inventory.
    """
    from sift_gateway.mapping.json_strings import resolve_json_strings

    _ = max_roots
    json_value = resolve_json_strings(json_value)
    root = _build_root_inventory(
        root_key="$",
        root_path="$",
        value=json_value,
    )
    return [root]
