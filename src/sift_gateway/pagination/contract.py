"""Build canonical pagination metadata for gateway responses.

Defines layer-explicit pagination fields used by mirrored upstream
responses and retrieval tool responses.
"""

from __future__ import annotations

from typing import Any, Literal

from sift_gateway.tools.usage_hint import with_pagination_completeness_rule

NEXT_PAGE_TOOL_NAME: Literal["artifact"] = "artifact"

PAGINATION_LAYER_UPSTREAM: Literal["upstream"] = "upstream"
PAGINATION_LAYER_ARTIFACT_RETRIEVAL: Literal["artifact_retrieval"] = (
    "artifact_retrieval"
)

RETRIEVAL_STATUS_PARTIAL: Literal["PARTIAL"] = "PARTIAL"
RETRIEVAL_STATUS_COMPLETE: Literal["COMPLETE"] = "COMPLETE"

UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE: Literal[
    "MORE_PAGES_AVAILABLE"
] = "MORE_PAGES_AVAILABLE"
UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE: Literal["SIGNAL_INCONCLUSIVE"] = (
    "SIGNAL_INCONCLUSIVE"
)
UPSTREAM_PARTIAL_REASON_CONFIG_MISSING: Literal["CONFIG_MISSING"] = (
    "CONFIG_MISSING"
)
UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING: Literal["NEXT_TOKEN_MISSING"] = (
    "NEXT_TOKEN_MISSING"
)

RETRIEVAL_PARTIAL_REASON_CURSOR_AVAILABLE: Literal["CURSOR_AVAILABLE"] = (
    "CURSOR_AVAILABLE"
)

PAGINATION_WARNING_INCOMPLETE_RESULT_SET: Literal["INCOMPLETE_RESULT_SET"] = (
    "INCOMPLETE_RESULT_SET"
)

RetrievalStatus = Literal["PARTIAL", "COMPLETE"]
UpstreamPartialReason = Literal[
    "MORE_PAGES_AVAILABLE",
    "SIGNAL_INCONCLUSIVE",
    "CONFIG_MISSING",
    "NEXT_TOKEN_MISSING",
]
RetrievalPartialReason = Literal["CURSOR_AVAILABLE"]
UpstreamNextKind = Literal["tool_call", "command", "params_only"]


def _extract_cursor_info(next_params: dict[str, Any]) -> tuple[str | None, Any]:
    """Return a single cursor-like key/value from next params."""
    if len(next_params) != 1:
        return None, None
    only_item = next(iter(next_params.items()))
    if not isinstance(only_item[0], str) or not only_item[0]:
        return None, None
    return only_item[0], only_item[1]


def _build_tool_call_next(artifact_id: str) -> dict[str, Any]:
    """Build canonical MCP tool-call continuation payload."""
    return {
        "kind": "tool_call",
        "artifact_id": artifact_id,
        "tool": NEXT_PAGE_TOOL_NAME,
        "arguments": {
            "action": "next_page",
            "artifact_id": artifact_id,
        },
    }


def _build_command_next(artifact_id: str) -> dict[str, Any]:
    """Build canonical CLI command continuation payload."""
    command_line = (
        f"sift-gateway run --continue-from {artifact_id} -- <next-command>"
    )
    return {
        "kind": "command",
        "artifact_id": artifact_id,
        "command": "run",
        "continue_from_artifact_id": artifact_id,
        "command_line": command_line,
    }


def _build_params_only_next(artifact_id: str) -> dict[str, Any]:
    """Build canonical manual continuation payload."""
    return {
        "kind": "params_only",
        "artifact_id": artifact_id,
    }


def _build_upstream_next(
    *,
    artifact_id: str,
    next_kind: UpstreamNextKind | None,
    next_params: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Build canonical upstream continuation payload."""
    if next_kind is None:
        return None
    if next_kind == "tool_call":
        next_payload = _build_tool_call_next(artifact_id)
    elif next_kind == "command":
        next_payload = _build_command_next(artifact_id)
    else:
        next_payload = _build_params_only_next(artifact_id)
    next_payload["params"] = (
        dict(next_params) if isinstance(next_params, dict) else {}
    )
    return next_payload


def _maybe_limit_hint(original_args: dict[str, Any] | None) -> str | None:
    """Return limit hint when request included positive integer limit."""
    limit_value = (
        original_args.get("limit") if isinstance(original_args, dict) else None
    )
    if isinstance(limit_value, int) and limit_value > 0:
        return (
            f"request used limit={limit_value}, so this page may be truncated"
        )
    return None


def _next_hint_phrase(
    *,
    next_payload: dict[str, Any],
    cursor_param: str | None,
    cursor_value: Any,
) -> str:
    """Build continuation phrase for the current next payload."""
    kind = next_payload.get("kind")
    if kind == "tool_call":
        artifact_id = next_payload.get("artifact_id")
        if isinstance(artifact_id, str) and artifact_id:
            if cursor_param is not None:
                return (
                    "continue with "
                    f'artifact(action="next_page", artifact_id="{artifact_id}") '
                    "or re-call the mirrored tool with that cursor"
                )
            return (
                "call "
                f'artifact(action="next_page", artifact_id="{artifact_id}") '
                "to fetch the next page"
            )
    if kind == "command":
        command_line = next_payload.get("command_line")
        if isinstance(command_line, str) and command_line:
            if cursor_param is not None:
                return (
                    f'continue with "{command_line}" and use '
                    '"pagination.next.params" as continuation values'
                )
            return f'continue with "{command_line}"'
    return "use pagination.next.params as continuation values"


def _build_upstream_hint(
    *,
    retrieval_status: RetrievalStatus,
    next_payload: dict[str, Any] | None,
    original_args: dict[str, Any] | None,
) -> str:
    """Build a user-facing hint consistent with pagination state."""
    if next_payload is not None:
        next_params_raw = next_payload.get("params")
        next_params = (
            next_params_raw if isinstance(next_params_raw, dict) else {}
        )
        cursor_param, cursor_value = _extract_cursor_info(next_params)
        hint_parts: list[str] = ["More results are available"]
        limit_hint = _maybe_limit_hint(original_args)
        if limit_hint is not None:
            hint_parts.append(limit_hint)
        if cursor_param is not None:
            hint_parts.append(f'next cursor is {cursor_param}="{cursor_value}"')
        hint_parts.append(
            _next_hint_phrase(
                next_payload=next_payload,
                cursor_param=cursor_param,
                cursor_value=cursor_value,
            )
        )
        return with_pagination_completeness_rule(". ".join(hint_parts) + ".")

    if retrieval_status == RETRIEVAL_STATUS_PARTIAL:
        return with_pagination_completeness_rule(
            "Result set may be incomplete. More pages might exist, "
            "but a continuation action could not be generated."
        )
    return with_pagination_completeness_rule(
        "No additional pages are available."
    )


def _warning_items(
    *,
    warning: str | None,
    extra_warnings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Build warnings list preserving primary warning code."""
    warning_items: list[dict[str, Any]] = []
    if isinstance(warning, str) and warning:
        warning_items.append({"code": warning})
    if isinstance(extra_warnings, list):
        warning_items.extend(
            warning_item
            for warning_item in extra_warnings
            if isinstance(warning_item, dict)
        )
    return warning_items


def build_upstream_pagination_meta(
    *,
    artifact_id: str,
    page_number: int,
    retrieval_status: RetrievalStatus,
    has_more: bool,
    partial_reason: UpstreamPartialReason | None,
    warning: str | None,
    next_kind: UpstreamNextKind | None = None,
    next_params: dict[str, Any] | None = None,
    original_args: dict[str, Any] | None = None,
    extra_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build canonical upstream pagination metadata."""
    next_payload = (
        _build_upstream_next(
            artifact_id=artifact_id,
            next_kind=next_kind,
            next_params=next_params,
        )
        if has_more
        else None
    )
    hint = _build_upstream_hint(
        retrieval_status=retrieval_status,
        next_payload=next_payload,
        original_args=original_args,
    )

    meta: dict[str, Any] = {
        "layer": PAGINATION_LAYER_UPSTREAM,
        "retrieval_status": retrieval_status,
        "partial_reason": partial_reason,
        "has_more": has_more,
        "page_number": page_number,
        "next": next_payload,
        "warning": warning,
        "hint": hint,
    }
    warnings_list = _warning_items(
        warning=warning,
        extra_warnings=extra_warnings,
    )
    if warnings_list:
        meta["warnings"] = warnings_list
    return meta


def build_retrieval_pagination_meta(
    *,
    truncated: bool,
    cursor: str | None,
) -> dict[str, Any]:
    """Build canonical retrieval-layer pagination metadata.

    Args:
        truncated: Whether result truncation occurred.
        cursor: Opaque cursor for next page, if any.

    Returns:
        Pagination metadata dict for retrieval tools.
    """
    has_more = bool(truncated and cursor)
    if has_more:
        hint = (
            "More results available. Resume with the cursor "
            "returned in this response. "
            "Do not claim completeness until "
            "pagination.retrieval_status == COMPLETE."
        )
    elif truncated:
        hint = (
            "Result set truncated but no cursor available. "
            "Narrow your query with where or smaller limit."
        )
    else:
        hint = "All matching records returned (retrieval_status=COMPLETE)."
    return {
        "layer": PAGINATION_LAYER_ARTIFACT_RETRIEVAL,
        "retrieval_status": (
            RETRIEVAL_STATUS_PARTIAL if truncated else RETRIEVAL_STATUS_COMPLETE
        ),
        "partial_reason": (
            RETRIEVAL_PARTIAL_REASON_CURSOR_AVAILABLE if has_more else None
        ),
        "has_more": has_more,
        "next_cursor": cursor if has_more else None,
        "hint": hint,
    }
