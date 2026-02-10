"""Canonicalize select-path lists and project records.

Deduplicate and sort JSONPath select-path lists into a
canonical form, compute stable hashes over them, and project
selected fields from JSON records.  Key exports are
``canonicalize_select_paths``, ``select_paths_hash``, and
``project_select_paths``.
"""

from __future__ import annotations

from typing import Any, Sequence

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.jsonpath import (
    canonicalize_jsonpath,
    evaluate_jsonpath,
)
from mcp_artifact_gateway.util.hashing import sha256_hex


def canonicalize_select_paths(
    paths: Sequence[str],
    *,
    max_jsonpath_length: int | None = None,
    max_path_segments: int | None = None,
) -> list[str]:
    """Deduplicate and sort select-paths into canonical form.

    Each path is individually canonicalized, then the list is
    deduplicated and lexicographically sorted.

    Args:
        paths: JSONPath strings to canonicalize.
        max_jsonpath_length: Optional cap per path string.
        max_path_segments: Optional cap on segments per path.

    Returns:
        Sorted, deduplicated list of canonical path strings.

    Raises:
        JsonPathError: If any path is syntactically invalid
            or exceeds the configured limits.
    """
    return sorted(
        {
            canonicalize_jsonpath(
                path,
                max_length=max_jsonpath_length,
                max_segments=max_path_segments,
            )
            for path in paths
        }
    )


def select_paths_hash(
    paths: Sequence[str],
    *,
    max_jsonpath_length: int | None = None,
    max_path_segments: int | None = None,
) -> str:
    """Compute a stable SHA-256 hash over canonical select-paths.

    Canonicalize the path list, serialize it to RFC 8785
    canonical JSON bytes, and return the hex digest.

    Args:
        paths: JSONPath strings to hash.
        max_jsonpath_length: Optional cap per path string.
        max_path_segments: Optional cap on segments per path.

    Returns:
        Hex-encoded SHA-256 digest of the canonical paths.
    """
    canonical = canonicalize_select_paths(
        paths,
        max_jsonpath_length=max_jsonpath_length,
        max_path_segments=max_path_segments,
    )
    return sha256_hex(canonical_bytes(canonical))


def project_select_paths(
    record: Any,
    paths: Sequence[str],
    *,
    missing_as_null: bool = False,
    max_jsonpath_length: int | None = None,
    max_path_segments: int | None = None,
    max_wildcard_expansion_total: int | None = None,
) -> dict[str, Any]:
    """Project selected fields from a JSON record.

    Evaluate each canonical select-path against the record
    and collect matched values into a dict keyed by path.
    Single matches are returned as scalars; multiple matches
    (from wildcards) are returned as lists.

    Args:
        record: JSON-compatible value to project from.
        paths: JSONPath strings selecting desired fields.
        missing_as_null: If True, emit None for paths that
            match nothing instead of omitting the key.
        max_jsonpath_length: Optional cap per path string.
        max_path_segments: Optional cap on segments per path.
        max_wildcard_expansion_total: Optional cumulative cap
            on wildcard expansion across all evaluations.

    Returns:
        Dict mapping canonical path strings to their
        projected values.
    """
    projected: dict[str, Any] = {}
    for path in canonicalize_select_paths(
        paths,
        max_jsonpath_length=max_jsonpath_length,
        max_path_segments=max_path_segments,
    ):
        values = evaluate_jsonpath(
            record,
            path,
            max_length=max_jsonpath_length,
            max_segments=max_path_segments,
            max_wildcard_expansion_total=max_wildcard_expansion_total,
        )
        if not values:
            if missing_as_null:
                projected[path] = None
            continue
        projected[path] = values[0] if len(values) == 1 else values
    return projected
