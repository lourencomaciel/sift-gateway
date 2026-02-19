"""Canonicalize select-path lists and compute stable hashes.

Deduplicate and sort JSONPath select-path lists into a
canonical form and compute stable hashes over them.  Key
exports are ``canonicalize_select_paths`` and
``select_paths_hash``.
"""

from __future__ import annotations

from collections.abc import Sequence

from sift_gateway.canon.rfc8785 import canonical_bytes
from sift_gateway.query.jsonpath import canonicalize_jsonpath
from sift_gateway.util.hashing import sha256_hex


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
