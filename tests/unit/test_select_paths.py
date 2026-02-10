from __future__ import annotations

from sidepouch_mcp.query.select_paths import (
    canonicalize_select_paths,
    project_select_paths,
    select_paths_hash,
)


def test_select_paths_canonicalize_and_dedupe() -> None:
    paths = canonicalize_select_paths(["$['b']", "$.a", "$.a"])
    assert paths == ["$.a", "$.b"]


def test_select_projection_missing_as_null() -> None:
    projected = project_select_paths(
        {"a": 1}, ["$.a", "$.missing"], missing_as_null=True
    )
    assert projected["$.a"] == 1
    assert projected["$.missing"] is None


def test_select_paths_hash_stable() -> None:
    h1 = select_paths_hash(["$.a", "$.b"])
    h2 = select_paths_hash(["$.b", "$.a"])
    assert h1 == h2
