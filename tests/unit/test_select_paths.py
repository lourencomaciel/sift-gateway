import pytest

from mcp_artifact_gateway.query.select_paths import canonicalize_select_paths, select_paths_hash


def test_canonicalize_select_paths() -> None:
    paths = [".b", " .a ", ".a", "['z']"]
    canonical = canonicalize_select_paths(paths)
    assert canonical == [".a", ".b", ".z"]


def test_select_paths_rejects_absolute() -> None:
    with pytest.raises(ValueError):
        canonicalize_select_paths(["$.a"])


def test_select_paths_hash_stable() -> None:
    canonical = [".a", ".b"]
    h1 = select_paths_hash(canonical)
    h2 = select_paths_hash(canonical)
    assert h1 == h2
    assert len(h1) == 64
