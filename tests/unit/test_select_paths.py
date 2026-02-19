from __future__ import annotations

from sift_gateway.query.select_paths import (
    canonicalize_select_paths,
    select_paths_hash,
)


def test_select_paths_canonicalize_and_dedupe() -> None:
    paths = canonicalize_select_paths(["$['b']", "$.a", "$.a"])
    assert paths == ["$.a", "$.b"]


def test_select_paths_hash_stable() -> None:
    h1 = select_paths_hash(["$.a", "$.b"])
    h2 = select_paths_hash(["$.b", "$.a"])
    assert h1 == h2
