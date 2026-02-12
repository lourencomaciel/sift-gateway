"""Extract and assess upstream pagination signals.

Detect pagination cursors, offsets, or page numbers in upstream
tool responses using per-upstream JSONPath configuration.  Exports
``assess_pagination`` (canonical assessment) and
``extract_pagination_state`` (compatibility wrapper).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sidepouch_mcp.config.settings import PaginationConfig
from sidepouch_mcp.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_COMPLETE,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
    UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING,
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    RetrievalStatus,
    UpstreamPartialReason,
)
from sidepouch_mcp.query.jsonpath import JsonPathError, evaluate_jsonpath


@dataclass(frozen=True)
class PaginationState:
    """Immutable pagination state stored alongside an artifact.

    Contains everything needed to construct the next upstream
    call when the LLM invokes ``artifact.next_page``.

    Attributes:
        upstream_prefix: Namespace prefix of the upstream.
        tool_name: Original upstream tool name (unqualified).
        original_args: Arguments from the original tool call
            (without gateway-reserved keys).
        next_params: Parameter overrides for the next call
            (e.g. ``{"after": "QVFIU..."}``).
        page_number: Zero-based page number of this artifact.
    """

    upstream_prefix: str
    tool_name: str
    original_args: dict[str, Any]
    next_params: dict[str, Any]
    page_number: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict for DB storage.

        Returns:
            Dict representation of the pagination state.
        """
        return {
            "upstream_prefix": self.upstream_prefix,
            "tool_name": self.tool_name,
            "original_args": self.original_args,
            "next_params": self.next_params,
            "page_number": self.page_number,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PaginationState:
        """Deserialize from a stored dict.

        Args:
            data: Dict as produced by ``to_dict``.

        Returns:
            Reconstructed ``PaginationState``.
        """
        return cls(
            upstream_prefix=str(data["upstream_prefix"]),
            tool_name=str(data["tool_name"]),
            original_args=dict(data.get("original_args") or {}),
            next_params=dict(data.get("next_params") or {}),
            page_number=int(data.get("page_number", 0)),
        )


@dataclass(frozen=True)
class PaginationAssessment:
    """Canonical pagination assessment for one upstream response.

    Attributes:
        state: Next-page state when a follow-up call can be issued.
        has_more: Whether additional records are known to exist.
        retrieval_status: Completion status (``PARTIAL`` or
            ``COMPLETE``).
        partial_reason: Optional reason when status is PARTIAL.
        warning: Optional warning code (e.g.
            ``INCOMPLETE_RESULT_SET``).
        page_number: Zero-based page number for this response.
    """

    state: PaginationState | None
    has_more: bool
    retrieval_status: RetrievalStatus
    partial_reason: UpstreamPartialReason | None
    warning: str | None
    page_number: int


def _evaluate_path(
    data: Any,
    path: str,
) -> Any | None:
    """Evaluate a JSONPath and return the first match.

    Delegates to the project's ``evaluate_jsonpath`` which
    supports dotted fields, bracket-quoted fields, integer
    array indices, and the ``[*]`` wildcard.  Returns the
    first matched value, or ``None`` when the path yields
    no results or is syntactically invalid.

    Args:
        data: JSON-compatible value to traverse.
        path: JSONPath expression starting with ``$``.

    Returns:
        The first matched value, or ``None`` if not found.
    """
    if not path:
        return None
    try:
        matches = evaluate_jsonpath(data, path)
    except JsonPathError:
        return None
    if not matches:
        return None
    return matches[0]


def _has_more(
    json_value: Any,
    has_more_path: str | None,
) -> bool:
    """Check whether more pages exist.

    When ``has_more_path`` is configured, evaluate it and
    check for a truthy, non-empty value.  When not configured,
    assume more pages exist (caller decides).

    Args:
        json_value: The JSON content value from the response.
        has_more_path: Optional JSONPath for the has-more signal.

    Returns:
        True if more pages likely exist.
    """
    if not has_more_path:
        return True
    val = _evaluate_path(json_value, has_more_path)
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, list) and len(val) == 0:
        return False
    return True


def _has_more_signal(
    json_value: Any,
    has_more_path: str | None,
) -> bool | None:
    """Return has-more signal or ``None`` when signal is unknown.

    Args:
        json_value: The JSON content value from the response.
        has_more_path: Optional JSONPath for has-more signal.

    Returns:
        ``True`` or ``False`` when a signal path is configured,
        otherwise ``None``.
    """
    if not has_more_path:
        return None
    return _has_more(json_value, has_more_path)


def _build_next_params(
    *,
    strategy: str,
    json_value: Any,
    pagination_config: PaginationConfig,
    original_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Build next-page argument overrides for a pagination strategy.

    Args:
        strategy: Configured pagination strategy.
        json_value: JSON content value from the response.
        pagination_config: Pagination config for this upstream.
        original_args: Original forwarded arguments.

    Returns:
        Dict of next-page arg overrides, or ``None`` when no
        follow-up request can be built.
    """
    next_params: dict[str, Any] = {}

    if strategy == "cursor":
        cursor_value = _evaluate_path(
            json_value,
            pagination_config.cursor_response_path or "",
        )
        if cursor_value is None:
            return None
        if isinstance(cursor_value, str) and not cursor_value.strip():
            return None
        next_params[pagination_config.cursor_param_name or "after"] = (
            cursor_value
        )
        return next_params

    if strategy == "offset":
        param = pagination_config.offset_param_name or "offset"
        size_param = pagination_config.page_size_param_name or "limit"
        current_offset = original_args.get(param, 0)
        if not isinstance(current_offset, int):
            current_offset = 0
        page_size = original_args.get(size_param, 0)
        if not isinstance(page_size, int) or page_size <= 0:
            return None
        next_params[param] = current_offset + page_size
        return next_params

    if strategy == "page_number":
        param = pagination_config.page_param_name or "page"
        current_page = original_args.get(param, 1)
        if not isinstance(current_page, int):
            current_page = 1
        next_params[param] = current_page + 1
        return next_params

    return None


def assess_pagination(
    *,
    json_value: Any,
    pagination_config: PaginationConfig,
    original_args: dict[str, Any],
    upstream_prefix: str,
    tool_name: str,
    page_number: int = 0,
) -> PaginationAssessment:
    """Assess pagination completion and next-page availability.

    Args:
        json_value: The JSON content value from the upstream
            response envelope.
        pagination_config: Per-upstream pagination settings.
        original_args: Original tool call arguments (reserved
            keys already stripped).
        upstream_prefix: Upstream namespace prefix.
        tool_name: Unqualified upstream tool name.
        page_number: Zero-based page number of the current
            artifact (0 for the initial call).

    Returns:
        A canonical ``PaginationAssessment``.
    """
    has_more_signal = _has_more_signal(
        json_value,
        pagination_config.has_more_response_path,
    )
    next_params = _build_next_params(
        strategy=pagination_config.strategy,
        json_value=json_value,
        pagination_config=pagination_config,
        original_args=original_args,
    )

    # Explicit terminal signal from upstream.
    if has_more_signal is False:
        return PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_COMPLETE,
            partial_reason=None,
            warning=None,
            page_number=page_number,
        )

    if next_params is not None:
        state = PaginationState(
            upstream_prefix=upstream_prefix,
            tool_name=tool_name,
            original_args=original_args,
            next_params=next_params,
            page_number=page_number,
        )
        return PaginationAssessment(
            state=state,
            has_more=True,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )

    partial_reason = UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE
    if has_more_signal is True:
        partial_reason = UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING

    return PaginationAssessment(
        state=None,
        has_more=False,
        retrieval_status=RETRIEVAL_STATUS_PARTIAL,
        partial_reason=partial_reason,
        warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
        page_number=page_number,
    )


def extract_pagination_state(
    *,
    json_value: Any,
    pagination_config: PaginationConfig,
    original_args: dict[str, Any],
    upstream_prefix: str,
    tool_name: str,
    page_number: int = 0,
) -> PaginationState | None:
    """Extract next-page state from an upstream response.

    Inspects the JSON content value of an upstream response
    using the configured pagination strategy and JSONPath
    mappings.  This compatibility wrapper returns only the
    next-page state and omits canonical status fields.

    Args:
        json_value: The JSON content value from the upstream
            response envelope.
        pagination_config: Per-upstream pagination settings.
        original_args: Original tool call arguments (reserved
            keys already stripped).
        upstream_prefix: Upstream namespace prefix.
        tool_name: Unqualified upstream tool name.
        page_number: Zero-based page number of the current
            artifact (0 for the initial call).

    Returns:
        A ``PaginationState`` with ``next_params`` populated,
        or ``None`` when a follow-up call cannot be issued.
    """
    return assess_pagination(
        json_value=json_value,
        pagination_config=pagination_config,
        original_args=original_args,
        upstream_prefix=upstream_prefix,
        tool_name=tool_name,
        page_number=page_number,
    ).state
