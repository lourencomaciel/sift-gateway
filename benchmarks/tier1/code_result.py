"""Unwrap code-query execution responses for LLM answer extraction."""

from __future__ import annotations

from typing import Any


def unwrap_code_result(response: dict[str, Any]) -> Any:
    """Extract the primary value from a code-query response.

    Handles three response shapes:

    - ``items`` list: unwraps single-element lists to the bare value,
      returns multi-element lists as-is.  Empty lists are returned
      as ``[]`` (not the full response dict).
    - ``payload`` fallback: returns the payload directly, including
      explicit ``None`` values.
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

    if "payload" in response:
        return response["payload"]

    return response
