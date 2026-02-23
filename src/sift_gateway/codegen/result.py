"""Unwrap code-query execution responses."""

from __future__ import annotations

from typing import Any


def unwrap_code_result(response: dict[str, Any]) -> Any:
    """Extract the primary value from a code-query response.

    Handles three response shapes:

    - ``items`` list: unwraps single-element lists to the bare value,
      returns multi-element lists as-is.
    - ``payload`` fallback: returns the payload directly.
    - Error responses: passed through unchanged.

    Args:
        response: The dict returned by code-query execution.

    Returns:
        The unwrapped result value, or the full response dict for
        error / unrecognised shapes.
    """
    if "error" in response:
        return response

    items = response.get("items")
    if isinstance(items, list):
        if len(items) == 1:
            return items[0]
        return items

    payload = response.get("payload")
    if payload is not None:
        return payload

    return response
