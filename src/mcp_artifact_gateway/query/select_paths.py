"""Select path canonicalization and projection helpers."""

from __future__ import annotations

from typing import Any, Sequence

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.jsonpath import canonicalize_jsonpath, evaluate_jsonpath
from mcp_artifact_gateway.util.hashing import sha256_hex


def canonicalize_select_paths(
    paths: Sequence[str],
    *,
    max_jsonpath_length: int | None = None,
    max_path_segments: int | None = None,
) -> list[str]:
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
