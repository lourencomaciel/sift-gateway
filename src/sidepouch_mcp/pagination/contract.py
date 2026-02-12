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


def build_upstream_pagination_meta(
    *,
    artifact_id: str,
    page_number: int,
    retrieval_status: RetrievalStatus,
    has_more: bool,
    partial_reason: UpstreamPartialReason | None,
    warning: str | None,
    has_next_page: bool,
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

    Returns:
        Pagination metadata dict with canonical and compatibility
        fields.
    """
    next_action: dict[str, Any] | None = None
    hint: str
    if has_next_page:
        next_action = {
            "tool": NEXT_PAGE_TOOL_NAME,
            "arguments": {
                "action": "next_page",
                "artifact_id": artifact_id,
            },
        }
        hint = (
            "More results are available. Call "
            'artifact(action="next_page", '
            f'artifact_id="{artifact_id}") '
            "to fetch the next page."
        )
    elif retrieval_status == RETRIEVAL_STATUS_PARTIAL:
        hint = (
            "Result set may be incomplete. More pages might exist, "
            "but a next-page action could not be generated."
        )
    else:
        hint = "No additional pages are available."

    return {
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
    }
