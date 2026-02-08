import pytest

from mcp_artifact_gateway.query.where_hash import where_hash


def test_where_hash_raw_string() -> None:
    h1 = where_hash("a = 1", mode="raw_string")
    h2 = where_hash("a = 1", mode="raw_string")
    assert h1 == h2
    assert len(h1) == 64


def test_where_hash_canonical_ast_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        where_hash("a = 1", mode="canonical_ast")


def test_where_hash_invalid_mode() -> None:
    with pytest.raises(ValueError):
        where_hash("a = 1", mode="bogus")
