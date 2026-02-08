"""Deterministic traversal helpers per traversal contract (§12.4)."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any


def iter_children(value: Any) -> Iterator[tuple[str | int, Any]]:
    """Yield child members in deterministic order.

    - dict: keys sorted lexicographically (Unicode code point order)
    - list: indices ascending
    - other: yields nothing
    """
    if isinstance(value, dict):
        for key in sorted(value.keys()):
            yield key, value[key]
        return

    if isinstance(value, list):
        for idx, item in enumerate(value):
            yield idx, item
        return

    return


def iter_wildcard(value: Any) -> Iterator[tuple[str | int, Any]]:
    """Alias for wildcard expansion using traversal contract ordering."""
    return iter_children(value)


def iter_sample_indices(sample_indices: Iterable[int]) -> Iterator[int]:
    """Yield sampled indices in ascending order (partial mode contract)."""
    return iter(sorted(sample_indices))
