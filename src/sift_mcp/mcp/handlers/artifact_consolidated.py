"""Consolidated ``artifact`` tool handler.

Public contract:

- ``action="query"`` for artifact retrieval/search operations.
- ``action="next_page"`` for upstream pagination continuation.

The query action dispatches to legacy describe/get/select/search
handlers based on query parameters and cursor context.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
    verify_cursor_token,
)
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.envelope.responses import gateway_error

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

_PUBLIC_ACTIONS = frozenset({"query", "next_page"})
_VALID_ACTIONS = _PUBLIC_ACTIONS
_GET_SIGNAL_FIELDS = frozenset({"target", "jsonpath"})
_SEARCH_SIGNAL_FIELDS = frozenset({"filters", "order_by"})


def _has_search_signals(arguments: dict[str, Any]) -> bool:
    """Return True when query args explicitly indicate a search query."""
    return any(
        key in arguments and arguments.get(key) is not None
        for key in _SEARCH_SIGNAL_FIELDS
    )


def _has_get_signals(arguments: dict[str, Any]) -> bool:
    """Return True when query args explicitly indicate an artifact.get."""
    return any(
        key in arguments and arguments.get(key) is not None
        for key in _GET_SIGNAL_FIELDS
    )


def _has_select_signals(arguments: dict[str, Any]) -> bool:
    """Return True when query args explicitly indicate artifact.select."""
    if arguments.get("count_only") is True or arguments.get("distinct") is True:
        return True
    return any(
        key in arguments and arguments.get(key) is not None
        for key in ("root_path", "select_paths", "where")
    )


def _infer_cursor_query_mode(
    ctx: GatewayServer,
    token: str,
) -> tuple[str, dict[str, Any]] | dict[str, Any]:
    """Infer query mode from a signed cursor token.

    Args:
        ctx: Gateway server instance.
        token: Cursor token string.

    Returns:
        ``(mode, payload)`` where mode is ``search|get|select`` when
        inference succeeds; otherwise a gateway error dict.
    """
    try:
        payload = verify_cursor_token(token, ctx._get_cursor_secrets())
    except (CursorTokenError, CursorExpiredError) as exc:
        return ctx._cursor_error(exc)

    if payload.get("tool") != "artifact":
        return ctx._cursor_error(CursorStaleError("cursor tool mismatch"))

    if any(
        key in payload
        for key in (
            "root_path",
            "select_paths",
            "select_paths_hash",
            "where_hash",
        )
    ):
        return "select", payload

    if any(key in payload for key in ("target", "normalized_jsonpath")):
        return "get", payload

    cursor_artifact = payload.get("artifact_id")
    if isinstance(cursor_artifact, str) and cursor_artifact.startswith(
        "session:"
    ):
        return "search", payload

    return gateway_error("INVALID_ARGUMENT", "invalid cursor")


async def _dispatch_query_mode(
    ctx: GatewayServer,
    *,
    mode: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch a resolved query mode to the legacy handler."""
    if mode == "describe":
        return await _handle_describe(ctx, arguments)
    if mode == "get":
        return await _handle_get(ctx, arguments)
    if mode == "select":
        return await _handle_select(ctx, arguments)
    # mode == "search"
    return await _handle_search(ctx, arguments)


async def _handle_query(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route ``action=query`` to describe/get/select/search handlers.

    Args:
        ctx: Gateway server instance.
        arguments: Raw query arguments.

    Returns:
        Handler result dict, or a gateway error.
    """
    query_args = dict(arguments)
    has_artifact_id = bool(query_args.get("artifact_id"))
    has_cursor = isinstance(query_args.get("cursor"), str) and bool(
        query_args.get("cursor")
    )
    has_search_signals = _has_search_signals(query_args)
    has_get_signals = _has_get_signals(query_args)
    has_select_signals = _has_select_signals(query_args)

    if has_get_signals and has_select_signals:
        return gateway_error(
            "INVALID_ARGUMENT",
            "query cannot combine get and select parameters",
        )

    mode_from_args: str | None
    if has_artifact_id:
        if has_search_signals:
            return gateway_error(
                "INVALID_ARGUMENT",
                "filters/order_by are only valid for session search "
                "queries (query without artifact_id)",
            )
        if has_select_signals:
            mode_from_args = "select"
        elif has_get_signals:
            mode_from_args = "get"
        elif has_cursor:
            mode_from_args = None
        else:
            mode_from_args = "describe"
    else:
        if has_get_signals or has_select_signals:
            return gateway_error(
                "INVALID_ARGUMENT",
                "artifact_id is required for get/select query parameters",
            )
        if has_cursor and not has_search_signals:
            mode_from_args = None
        else:
            mode_from_args = "search"

    cursor_payload: dict[str, Any] | None = None
    mode: str
    if mode_from_args is None:
        cursor_token = query_args.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            inferred = _infer_cursor_query_mode(ctx, cursor_token)
            if isinstance(inferred, dict):
                return inferred
            mode, cursor_payload = inferred
        else:
            mode = "describe" if has_artifact_id else "search"
    else:
        mode = mode_from_args

    if (
        mode in {"get", "select"}
        and not query_args.get("artifact_id")
        and isinstance(cursor_payload, dict)
    ):
        bound_artifact = cursor_payload.get("artifact_id")
        if isinstance(bound_artifact, str) and bound_artifact:
            query_args["artifact_id"] = bound_artifact

    if mode == "search" and query_args.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "search query does not accept artifact_id",
        )

    return await _dispatch_query_mode(ctx, mode=mode, arguments=query_args)


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
            f"action must be one of: {', '.join(sorted(_PUBLIC_ACTIONS))}",
        )

    if action == "next_page":
        return await _handle_next_page(ctx, arguments)
    return await _handle_query(ctx, arguments)


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
            "artifact_id is required for query describe-mode",
        )
    from sift_mcp.mcp.handlers.artifact_describe import (
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
            "artifact_id is required for query get-mode",
        )
    if arguments.get("where") is not None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "The 'where' parameter is only supported with "
            "query select-mode. Use "
            "artifact(action='query', where=..., "
            "select_paths=[...]) for filtered queries.",
        )
    from sift_mcp.mcp.handlers.artifact_get import (
        handle_artifact_get,
    )

    return await handle_artifact_get(ctx, arguments)


async def _handle_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the select handler.

    When a ``cursor`` is present, ``root_path`` and
    ``select_paths`` are optional — the handler extracts
    them from the signed cursor payload.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments with ``artifact_id``,
            ``root_path``, ``select_paths``, and optional
            ``where`` filter and ``cursor``.

    Returns:
        Projected field data, or gateway error.
    """
    if not arguments.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for query select-mode",
        )
    # Defer root_path / select_paths validation to the handler
    # when a cursor is present — embedded values will be extracted.
    from sift_mcp.mcp.handlers.artifact_select import (
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
    from sift_mcp.mcp.handlers.artifact_search import (
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
    from sift_mcp.mcp.handlers.artifact_next_page import (
        handle_artifact_next_page,
    )

    return await handle_artifact_next_page(ctx, arguments)
