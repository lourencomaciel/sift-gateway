"""Perform full in-memory mapping of parsed JSON values.

Walk the complete JSON structure to discover collection roots
(arrays and top-level object keys), score them by size, and
build ``RootInventory`` objects with exact counts and field
type distributions.  Key export is ``run_full_mapping``.
"""

from __future__ import annotations

from typing import Any

from sift_mcp.mapping._helpers import json_type_name, normalize_path_segment
from sift_mcp.mapping.runner import RootInventory

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


def _score_root(value: Any) -> float:
    """Score a root by collection size.

    Larger collections score higher.  Non-collection values
    score zero.

    Args:
        value: Any JSON value to score.

    Returns:
        A float score (element count for collections, 0.0
        for scalars).
    """
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
    """Walk a parsed JSON value and discover collection roots.

    Discovery rules:
    - Array at root: single root at ``$`` with shape "array".
    - Object at root: examine top-level keys and one level of
      nesting for arrays/objects, scored by size descending.
      JSON-encoded string values are resolved before inspection.
    - Scalar at root: return a single zero-score placeholder.

    Args:
        json_value: Fully parsed JSON value to analyze.
        max_roots: Maximum number of roots to return.

    Returns:
        A list of up to max_roots RootInventory objects, each
        with exact counts and field type distributions.
    """
    from sift_mcp.mapping.json_strings import resolve_json_strings

    json_value = resolve_json_strings(json_value)

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
                path_segment = normalize_path_segment(key)
                root_path = f"${path_segment}"
                score = _score_root(val)
                candidates.append((score, key, root_path, val))

                # One level deeper: discover arrays inside dict values
                if isinstance(val, dict):
                    for sub_key, sub_val in val.items():
                        if isinstance(sub_val, (list, dict)):
                            sub_segment = normalize_path_segment(sub_key)
                            sub_path = f"{root_path}{sub_segment}"
                            sub_score = _score_root(sub_val)
                            sub_sort_key = f"{key}.{sub_key}"
                            candidates.append(
                                (sub_score, sub_sort_key, sub_path, sub_val)
                            )

        # Sort by score descending, then by key ascending for determinism
        candidates.sort(key=lambda c: (-c[0], c[1]))

        roots: list[RootInventory] = []
        for _score, key, root_path, val in candidates[:max_roots]:
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
