"""Consolidated ``artifact`` tool handler for contract-v1.

Public contract:

- ``action="query"`` with ``query_kind="code"`` only.
- ``action="next_page"`` for upstream pagination continuation.
- ``action="blob_list"`` to list referenced blobs without inline bytes.
- ``action="blob_materialize"`` to stage one blob as a local file path.
- ``action="blob_cleanup"`` to clean staged local blob files.
- ``action="blob_manifest"`` to export blob metadata as CSV/JSON.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_gateway.envelope.responses import gateway_error

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer

_PUBLIC_ACTIONS = frozenset(
    {
        "query",
        "next_page",
        "blob_list",
        "blob_materialize",
        "blob_cleanup",
        "blob_manifest",
    }
)
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
        query_args["scope"] = "single"
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


async def _handle_blob_list(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to blob_list handler."""
    has_artifact_id = bool(arguments.get("artifact_id"))
    raw_artifact_ids = arguments.get("artifact_ids")
    has_artifact_ids = isinstance(raw_artifact_ids, list) and bool(
        raw_artifact_ids
    )
    if not has_artifact_id and not has_artifact_ids:
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id or artifact_ids is required for action=blob_list",
        )
    from sift_gateway.mcp.handlers.artifact_blob import (
        handle_artifact_blob_list,
    )

    return await handle_artifact_blob_list(ctx, arguments)


async def _handle_blob_materialize(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to blob_materialize handler."""
    has_blob_id = bool(arguments.get("blob_id"))
    has_binary_hash = bool(arguments.get("binary_hash"))
    if not has_blob_id and not has_binary_hash:
        return gateway_error(
            "INVALID_ARGUMENT",
            (
                "blob_id or binary_hash is required for "
                "action=blob_materialize"
            ),
        )
    from sift_gateway.mcp.handlers.artifact_blob import (
        handle_artifact_blob_materialize,
    )

    return await handle_artifact_blob_materialize(ctx, arguments)


async def _handle_blob_cleanup(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to blob_cleanup handler."""
    from sift_gateway.mcp.handlers.artifact_blob import (
        handle_artifact_blob_cleanup,
    )

    return await handle_artifact_blob_cleanup(ctx, arguments)


async def _handle_blob_manifest(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Route to blob_manifest handler."""
    has_artifact_id = bool(arguments.get("artifact_id"))
    raw_artifact_ids = arguments.get("artifact_ids")
    has_artifact_ids = isinstance(raw_artifact_ids, list) and bool(
        raw_artifact_ids
    )
    if not has_artifact_id and not has_artifact_ids:
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id or artifact_ids is required for action=blob_manifest",
        )
    from sift_gateway.mcp.handlers.artifact_blob import (
        handle_artifact_blob_manifest,
    )

    return await handle_artifact_blob_manifest(ctx, arguments)


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
    if action == "blob_list":
        return await _handle_blob_list(ctx, arguments)
    if action == "blob_materialize":
        return await _handle_blob_materialize(ctx, arguments)
    if action == "blob_cleanup":
        return await _handle_blob_cleanup(ctx, arguments)
    if action == "blob_manifest":
        return await _handle_blob_manifest(ctx, arguments)
    return await _handle_query(ctx, arguments)
