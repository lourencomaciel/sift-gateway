"""Shared lineage root-candidate resolution helpers for query handlers."""

from sift_gateway.core.lineage_roots import (
    AllRelatedRootCandidates,
    CandidateRow,
    SingleRootCandidate,
    resolve_all_related_root_candidates,
    resolve_single_root_candidate,
)

__all__ = [
    "AllRelatedRootCandidates",
    "CandidateRow",
    "SingleRootCandidate",
    "resolve_all_related_root_candidates",
    "resolve_single_root_candidate",
]
