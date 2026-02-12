"""Consolidated ``artifact`` tool handler.

Routes ``action`` parameter to the appropriate existing handler.
Replaces the 7 separate artifact tools with a single ``artifact``
tool that accepts ``action`` as a required parameter.

Actions:
    - ``describe``: Inspect artifact structure and mapping roots.
    - ``get``: Retrieve raw envelope or mapped metadata.
    - ``select``: Project fields from a mapped root array.
    - ``search``: Find artifacts visible to the current session.
    - ``next_page``: Fetch next upstream page for paginated results.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sidepouch_mcp.envelope.responses import gateway_error

if TYPE_CHECKING:
    from sidepouch_mcp.mcp.server import GatewayServer

_VALID_ACTIONS = frozenset(
    {
        "describe",
        "get",
        "select",
        "search",
        "next_page",
    }
)


async def handle_artifact(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch to the appropriate artifact handler based on action.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments including required ``action``
            parameter and action-specific parameters.

    Returns:
        Handler result dict, or a gateway error for invalid
        actions or missing required parameters.
    """
    action = arguments.get("action")
    if not isinstance(action, str) or action not in _VALID_ACTIONS:
        return gateway_error(
            "INVALID_ARGUMENT",
            f"action must be one of: {', '.join(sorted(_VALID_ACTIONS))}",
        )

    if action == "describe":
        return await _handle_describe(ctx, arguments)
    if action == "get":
        return await _handle_get(ctx, arguments)
    if action == "select":
        return await _handle_select(ctx, arguments)
    if action == "search":
        return await _handle_search(ctx, arguments)
    # action == "next_page"
    return await _handle_next_page(ctx, arguments)


async def _handle_describe(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the describe handler.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with ``artifact_id``.

    Returns:
        Describe response or gateway error.
    """
    if not arguments.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for action=describe",
        )
    from sidepouch_mcp.mcp.handlers.artifact_describe import (
        handle_artifact_describe,
    )

    return await handle_artifact_describe(ctx, arguments)


async def _handle_get(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the get handler.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with ``artifact_id`` and
            optional ``target``, ``jsonpath``.

    Returns:
        Envelope or mapped data, or gateway error.
    """
    if not arguments.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for action=get",
        )
    from sidepouch_mcp.mcp.handlers.artifact_get import (
        handle_artifact_get,
    )

    return await handle_artifact_get(ctx, arguments)


async def _handle_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the select handler.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with ``artifact_id``,
            ``root_path``, ``select_paths``, and optional
            ``where`` filter.

    Returns:
        Projected field data, or gateway error.
    """
    artifact_id = arguments.get("artifact_id")
    if not artifact_id:
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for action=select",
        )
    root_path = arguments.get("root_path")
    if not root_path:
        return gateway_error(
            "INVALID_ARGUMENT",
            "root_path is required for action=select",
        )
    select_paths = arguments.get("select_paths")
    if not isinstance(select_paths, list) or not select_paths:
        return gateway_error(
            "INVALID_ARGUMENT",
            "select_paths is required for action=select",
        )
    from sidepouch_mcp.mcp.handlers.artifact_select import (
        handle_artifact_select,
    )

    return await handle_artifact_select(ctx, arguments)


async def _handle_search(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the search handler.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with optional ``filters``,
            ``order_by``, ``limit``, and ``cursor``.

    Returns:
        Search results or gateway error.
    """
    from sidepouch_mcp.mcp.handlers.artifact_search import (
        handle_artifact_search,
    )

    return await handle_artifact_search(ctx, arguments)


async def _handle_next_page(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the next_page handler.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with ``artifact_id``.

    Returns:
        Next-page upstream result or gateway error.
    """
    if not arguments.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for action=next_page",
        )
    from sidepouch_mcp.mcp.handlers.artifact_next_page import (
        handle_artifact_next_page,
    )

    return await handle_artifact_next_page(ctx, arguments)
