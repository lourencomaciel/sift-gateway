"""Upstream pagination detection, state, and response contracts."""

from sift_gateway.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_COMPLETE,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
    UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING,
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    build_retrieval_pagination_meta,
    build_upstream_pagination_meta,
)
from sift_gateway.pagination.discovery import (
    PaginationDiscovery,
    discover_pagination,
)
from sift_gateway.pagination.extract import (
    PaginationAssessment,
    PaginationState,
    assess_pagination,
    extract_pagination_state,
)

__all__ = [
    "PAGINATION_WARNING_INCOMPLETE_RESULT_SET",
    "RETRIEVAL_STATUS_COMPLETE",
    "RETRIEVAL_STATUS_PARTIAL",
    "UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE",
    "UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING",
    "UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE",
    "PaginationAssessment",
    "PaginationDiscovery",
    "PaginationState",
    "assess_pagination",
    "build_retrieval_pagination_meta",
    "build_upstream_pagination_meta",
    "discover_pagination",
    "extract_pagination_state",
]
