"""Consolidated ``artifact`` tool handler.

Public contract:

- ``action="query"`` for artifact retrieval/search operations.
- ``action="next_page"`` for upstream pagination continuation.

Query behavior is explicit:

- ``query_kind`` is required and must be one of
  ``describe|get|select|search|code``.
- ``scope`` is supported for ``describe|get|select`` and defaults
  to ``all_related``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.envelope.responses import gateway_error

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

_PUBLIC_ACTIONS = frozenset({"query", "next_page"})
_VALID_ACTIONS = _PUBLIC_ACTIONS
_QUERY_KINDS = frozenset({"describe", "get", "select", "search", "code"})
_QUERY_SCOPES = frozenset({"all_related", "single"})
_CODE_DISALLOWED_PARAMS = (
    "target",
    "jsonpath",
    "select_paths",
    "where",
    "order_by",
    "distinct",
    "count_only",
    "filters",
)


async def _dispatch_query_kind(
    ctx: GatewayServer,
    *,
    query_kind: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch a query to the selected query kind handler."""
    if query_kind == "describe":
        return await _handle_describe(ctx, arguments)
    if query_kind == "get":
        return await _handle_get(ctx, arguments)
    if query_kind == "select":
        return await _handle_select(ctx, arguments)
    if query_kind == "code":
        return await _handle_code(ctx, arguments)
    return await _handle_search(ctx, arguments)


def _resolve_query_kind(
    query_args: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    """Return validated query_kind."""
    raw_kind = query_args.get("query_kind")
    if not isinstance(raw_kind, str) or raw_kind not in _QUERY_KINDS:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "query_kind is required for action=query and must be one of: "
            "describe, get, select, search, code",
        )
    return raw_kind, None


def _validate_search_query_args(
    query_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate query_kind=search-specific constraints."""
    if query_args.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "query_kind=search does not accept artifact_id",
        )
    if query_args.get("scope") is not None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "query_kind=search does not accept scope",
        )
    return None


def _validate_query_artifact_scope(
    *,
    query_kind: str,
    query_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate artifact identity and scope inputs for non-search queries."""
    if query_kind == "code":
        has_artifact_id = bool(query_args.get("artifact_id"))
        raw_artifact_ids = query_args.get("artifact_ids")
        has_artifact_ids = isinstance(raw_artifact_ids, list) and bool(
            raw_artifact_ids
        )
        if not has_artifact_id and not has_artifact_ids:
            return gateway_error(
                "INVALID_ARGUMENT",
                "artifact_id or artifact_ids is required for query_kind=code",
            )
        # Preserve backward compatibility: ignore scope for code queries.
        query_args.pop("scope", None)
        return None

    if not query_args.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            f"artifact_id is required for query_kind={query_kind}",
        )

    raw_scope = query_args.get("scope")
    if raw_scope is None:
        cursor_token = query_args.get("cursor")
        has_cursor = isinstance(cursor_token, str) and bool(cursor_token)
        # Let handlers recover scope from cursor when omitted.
        if not has_cursor:
            query_args["scope"] = "all_related"
        return None
    if not isinstance(raw_scope, str) or raw_scope not in _QUERY_SCOPES:
        return gateway_error(
            "INVALID_ARGUMENT",
            "scope must be one of: all_related, single",
        )
    query_args["scope"] = raw_scope
    return None


def _validate_query_kind_arguments(
    *,
    query_kind: str,
    query_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate query_kind-specific argument compatibility."""
    if query_kind == "get":
        if query_args.get("where") is not None:
            return gateway_error(
                "INVALID_ARGUMENT",
                "The 'where' parameter is only supported with "
                "query_kind=select.",
            )
        return None
    if query_kind == "code":
        disallowed = [
            param
            for param in _CODE_DISALLOWED_PARAMS
            if query_args.get(param) is not None
        ]
        if disallowed:
            return gateway_error(
                "INVALID_ARGUMENT",
                "query_kind=code does not accept: " + ", ".join(disallowed),
            )
        return None
    if query_kind == "select" and (
        query_args.get("target") is not None
        or query_args.get("jsonpath") is not None
    ):
        return gateway_error(
            "INVALID_ARGUMENT",
            "target/jsonpath are only supported with query_kind=get",
        )
    return None


async def _handle_query(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route ``action=query`` using explicit query_kind and scope."""
    query_args = dict(arguments)
    query_kind, kind_err = _resolve_query_kind(query_args)
    if kind_err is not None:
        return kind_err
    if query_kind is None:
        return gateway_error("INTERNAL", "query_kind resolution failed")

    if query_kind == "search":
        search_err = _validate_search_query_args(query_args)
        if search_err is not None:
            return search_err
        return await _dispatch_query_kind(
            ctx, query_kind=query_kind, arguments=query_args
        )

    artifact_scope_err = _validate_query_artifact_scope(
        query_kind=query_kind,
        query_args=query_args,
    )
    if artifact_scope_err is not None:
        return artifact_scope_err

    args_err = _validate_query_kind_arguments(
        query_kind=query_kind,
        query_args=query_args,
    )
    if args_err is not None:
        return args_err

    return await _dispatch_query_kind(
        ctx, query_kind=query_kind, arguments=query_args
    )


async def handle_artifact(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch to the appropriate artifact handler based on action."""
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
    """Route to the describe handler."""
    from sift_mcp.mcp.handlers.artifact_describe import (
        handle_artifact_describe,
    )

    return await handle_artifact_describe(ctx, arguments)


async def _handle_get(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the get handler."""
    from sift_mcp.mcp.handlers.artifact_get import (
        handle_artifact_get,
    )

    return await handle_artifact_get(ctx, arguments)


async def _handle_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the select handler."""
    from sift_mcp.mcp.handlers.artifact_select import (
        handle_artifact_select,
    )

    return await handle_artifact_select(ctx, arguments)


async def _handle_search(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the search handler."""
    from sift_mcp.mcp.handlers.artifact_search import (
        handle_artifact_search,
    )

    return await handle_artifact_search(ctx, arguments)


async def _handle_code(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the code-query handler."""
    from sift_mcp.mcp.handlers.artifact_code import (
        handle_artifact_code,
    )

    return await handle_artifact_code(ctx, arguments)


async def _handle_next_page(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to the next_page handler."""
    if not arguments.get("artifact_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id is required for action=next_page",
        )
    from sift_mcp.mcp.handlers.artifact_next_page import (
        handle_artifact_next_page,
    )

    return await handle_artifact_next_page(ctx, arguments)
