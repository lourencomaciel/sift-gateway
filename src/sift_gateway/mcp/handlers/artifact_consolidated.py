"""Consolidated ``artifact`` tool handler for contract-v1.

Public contract:

- ``action="query"`` with ``query_kind="code"`` only.
- ``action="next_page"`` for upstream pagination continuation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_gateway.envelope.responses import gateway_error

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer

_PUBLIC_ACTIONS = frozenset({"query", "next_page"})
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


def _validate_code_query_arguments(
    query_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate ``action=query`` arguments for ``query_kind=code``."""
    raw_kind = query_args.get("query_kind")
    if raw_kind != "code":
        return gateway_error(
            "INVALID_ARGUMENT",
            "query_kind is required for action=query and must be: code",
        )

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

    raw_scope = query_args.get("scope")
    if raw_scope is None:
        query_args["scope"] = "all_related"
    elif not isinstance(raw_scope, str) or raw_scope not in _QUERY_SCOPES:
        return gateway_error(
            "INVALID_ARGUMENT",
            "scope must be one of: all_related, single",
        )

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


async def _handle_query(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route ``action=query`` with ``query_kind=code`` only."""
    query_args = dict(arguments)
    validation_error = _validate_code_query_arguments(query_args)
    if validation_error is not None:
        return validation_error
    from sift_gateway.mcp.handlers.artifact_code import (
        handle_artifact_code,
    )

    return await handle_artifact_code(ctx, query_args)


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
    from sift_gateway.mcp.handlers.artifact_next_page import (
        handle_artifact_next_page,
    )

    return await handle_artifact_next_page(ctx, arguments)


async def handle_artifact(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch to the appropriate artifact handler based on action."""
    action = arguments.get("action")
    if not isinstance(action, str) or action not in _PUBLIC_ACTIONS:
        return gateway_error(
            "INVALID_ARGUMENT",
            f"action must be one of: {', '.join(sorted(_PUBLIC_ACTIONS))}",
        )
    if action == "next_page":
        return await _handle_next_page(ctx, arguments)
    return await _handle_query(ctx, arguments)

