"""Build canonical pagination metadata for gateway responses.

Defines layer-explicit pagination fields used by mirrored upstream
responses and retrieval tool responses.  Exports helper builders
that preserve backward-compatible pagination fields while adding
canonical completion semantics.
"""

from __future__ import annotations

from typing import Any, Literal

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


def _extract_cursor_info(
    next_params: dict[str, Any] | None,
) -> tuple[str | None, Any]:
    """Return a single cursor-like key/value from next params."""
    if not isinstance(next_params, dict) or len(next_params) != 1:
        return None, None
    only_item = next(iter(next_params.items()))
    if not isinstance(only_item[0], str) or not only_item[0]:
        return None, None
    return only_item[0], only_item[1]


def _build_next_action(artifact_id: str) -> dict[str, Any]:
    """Build canonical artifact next_page tool action."""
    return {
        "tool": NEXT_PAGE_TOOL_NAME,
        "arguments": {
            "action": "next_page",
            "artifact_id": artifact_id,
        },
    }


def _maybe_limit_hint(original_args: dict[str, Any] | None) -> str | None:
    """Return limit hint when request included positive integer limit."""
    limit_value = (
        original_args.get("limit")
        if isinstance(original_args, dict)
        else None
    )
    if isinstance(limit_value, int) and limit_value > 0:
        return f"request used limit={limit_value}, so this page may be truncated"
    return None


def _build_upstream_hint(
    *,
    artifact_id: str,
    retrieval_status: RetrievalStatus,
    has_next_page: bool,
    cursor_param: str | None,
    cursor_value: Any,
    original_args: dict[str, Any] | None,
) -> str:
    """Build a user-facing hint consistent with pagination state."""
    if has_next_page:
        hint_parts: list[str] = ["More results are available"]
        limit_hint = _maybe_limit_hint(original_args)
        if limit_hint is not None:
            hint_parts.append(limit_hint)
        if cursor_param is not None:
            hint_parts.append(f'next cursor is {cursor_param}="{cursor_value}"')
            hint_parts.append(
                "continue with "
                f'artifact(action="next_page", artifact_id="{artifact_id}") '
                "or re-call the mirrored tool with that cursor"
            )
        else:
            hint_parts.append(
                "call "
                f'artifact(action="next_page", artifact_id="{artifact_id}") '
                "to fetch the next page"
            )
        return ". ".join(hint_parts) + "."

    if retrieval_status == RETRIEVAL_STATUS_PARTIAL:
        return (
            "Result set may be incomplete. More pages might exist, "
            "but a next-page action could not be generated."
        )
    return "No additional pages are available."


def _warning_items(
    *,
    warning: str | None,
    extra_warnings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Build warnings list preserving backward-compatible primary warning."""
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
    has_next_page: bool,
    next_params: dict[str, Any] | None = None,
    original_args: dict[str, Any] | None = None,
    extra_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build canonical upstream pagination metadata.

    Args:
        artifact_id: Artifact identifier for the current page.
        page_number: Zero-based page number for this artifact.
        retrieval_status: Completion status (``PARTIAL`` or
            ``COMPLETE``).
        has_more: Whether more upstream results are known to
            exist from this response.
        partial_reason: Optional reason when status is PARTIAL.
        warning: Optional warning code for incomplete results.
        has_next_page: Whether ``artifact.next_page`` can fetch
            the next page from this artifact.
        next_params: Optional next-call params extracted from
            upstream pagination state.
        original_args: Original mirrored-tool args used for this
            page. Used only for hint enrichment.
        extra_warnings: Additional structured warnings to expose
            alongside the legacy single warning code.

    Returns:
        Pagination metadata dict with canonical and compatibility
        fields.
    """
    cursor_param, cursor_value = _extract_cursor_info(next_params)
    next_action = _build_next_action(artifact_id) if has_next_page else None
    hint = _build_upstream_hint(
        artifact_id=artifact_id,
        retrieval_status=retrieval_status,
        has_next_page=has_next_page,
        cursor_param=cursor_param,
        cursor_value=cursor_value,
        original_args=original_args,
    )

    meta: dict[str, Any] = {
        "layer": PAGINATION_LAYER_UPSTREAM,
        "retrieval_status": retrieval_status,
        "partial_reason": partial_reason,
        "has_more": has_more,
        "page_number": page_number,
        "next_action": next_action,
        "warning": warning,
        # Backward-compatible fields.
        "has_next_page": has_next_page,
        "hint": hint,
    }
    if has_next_page and isinstance(next_params, dict):
        meta["next_params"] = next_params
        if cursor_param is not None:
            meta["next_cursor_param"] = cursor_param
            meta["next_cursor"] = cursor_value

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
