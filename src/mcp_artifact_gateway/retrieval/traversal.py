"""Deterministic traversal helpers."""

from __future__ import annotations

import re
from typing import Any, Iterator

_ASCII_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def traverse_deterministic(value: Any, path: str = "$") -> Iterator[tuple[str, Any]]:
    """Yield values in deterministic traversal order.

    Order contract:
    - arrays in ascending index
    - objects in lexicographic key order
    """
    yield path, value

    if isinstance(value, list):
        for index, item in enumerate(value):
            child_path = f"{path}[{index}]"
            yield from traverse_deterministic(item, child_path)
        return

    if isinstance(value, dict):
        for key in sorted(value):
            if _ASCII_IDENT_RE.fullmatch(key):
                child_path = f"{path}.{key}"
            else:
                escaped = key.replace("\\", "\\\\").replace("'", "\\'")
                child_path = f"{path}['{escaped}']"
            yield from traverse_deterministic(value[key], child_path)
