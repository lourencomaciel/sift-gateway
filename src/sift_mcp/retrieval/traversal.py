"""Provide deterministic JSON value traversal iterators.

Implement the traversal_v1 order contract: arrays in
ascending index order, objects in lexicographic key order.
Provide ``traverse_deterministic`` for full recursive
traversal and ``traverse_sampled`` for iterating only over
pre-selected sample indices in ascending order.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import re
from typing import Any

_ASCII_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _child_path(parent: str, key: str) -> str:
    """Build a child JSONPath segment from a parent and key.

    Use dot notation for simple ASCII identifiers and
    bracket-quoted notation for keys with special chars.

    Args:
        parent: Parent JSONPath string.
        key: Child key name.

    Returns:
        Extended JSONPath string for the child.
    """
    if _ASCII_IDENT_RE.fullmatch(key):
        return f"{parent}.{key}"
    escaped = key.replace("\\", "\\\\").replace("'", "\\'")
    return f"{parent}['{escaped}']"


def traverse_deterministic(
    value: Any, path: str = "$"
) -> Iterator[tuple[str, Any]]:
    """Yield ``(path, value)`` pairs in deterministic order.

    Implement the traversal_v1 contract: arrays in ascending
    index order, objects in lexicographic key order.  Recurse
    depth-first through the entire value tree.

    Args:
        value: JSON-compatible Python value to traverse.
        path: JSONPath prefix for the root node.

    Yields:
        Tuples of ``(jsonpath_string, node_value)`` for
        every node in the tree including the root.
    """
    yield path, value

    if isinstance(value, list):
        for index, item in enumerate(value):
            child = f"{path}[{index}]"
            yield from traverse_deterministic(item, child)
        return

    if isinstance(value, dict):
        for key in sorted(value):
            child = _child_path(path, key)
            yield from traverse_deterministic(value[key], child)


def traverse_sampled(
    records: Sequence[Any],
    sample_indices: Sequence[int],
    *,
    path: str = "$",
) -> Iterator[tuple[str, int, Any]]:
    """Yield ``(path, index, record)`` for sampled indices.

    Enumerate *sample_indices* in ascending order regardless
    of input order.  Indices outside *records* are silently
    skipped.  Yielded records are not recursively expanded;
    callers pass them to ``traverse_deterministic`` or
    ``project_select_paths`` for deeper projection.

    Args:
        records: Full sequence of records to sample from.
        sample_indices: Indices to include in output.
        path: JSONPath prefix for the root array.

    Yields:
        Tuples of ``(child_path, sample_index, record)``
        for each valid sampled index.
    """
    for idx in sorted(sample_indices):
        if 0 <= idx < len(records):
            child = f"{path}[{idx}]"
            yield child, idx, records[idx]
