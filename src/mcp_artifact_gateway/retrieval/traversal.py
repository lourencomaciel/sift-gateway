"""Deterministic traversal helpers.

Order contract (traversal_v1):
- Arrays: ascending index (0, 1, 2, ...)
- Objects: lexicographic key order (sorted)
- Wildcard [*] expansions obey the same ordering
- Sampled mode: enumerates only the supplied sampled indices, ascending
"""

from __future__ import annotations

import re
from typing import Any, Iterator, Sequence

_ASCII_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _child_path(parent: str, key: str) -> str:
    """Build a child JSONPath segment from a parent path and a key."""
    if _ASCII_IDENT_RE.fullmatch(key):
        return f"{parent}.{key}"
    escaped = key.replace("\\", "\\\\").replace("'", "\\'")
    return f"{parent}['{escaped}']"


def traverse_deterministic(value: Any, path: str = "$") -> Iterator[tuple[str, Any]]:
    """Yield ``(path, value)`` pairs in deterministic traversal order.

    Order contract:
    - arrays in ascending index
    - objects in lexicographic key order
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
    """Yield ``(path, sample_index, record)`` for sampled indices only.

    The *sample_indices* are always enumerated in ascending order regardless
    of the order they are supplied in.  Indices that fall outside *records*
    are silently skipped (the caller already validated them during mapping).

    Each yielded record is **not** recursively expanded -- callers typically
    pass individual records to ``traverse_deterministic`` or
    ``project_select_paths`` for deeper projection.
    """
    for idx in sorted(sample_indices):
        if 0 <= idx < len(records):
            child = f"{path}[{idx}]"
            yield child, idx, records[idx]
