from __future__ import annotations

from mcp_artifact_gateway.query.jsonpath import evaluate_jsonpath
from mcp_artifact_gateway.retrieval.traversal import traverse_deterministic


def test_traversal_object_keys_are_lexicographic() -> None:
    data = {"b": 2, "a": 1}
    paths = [path for path, _ in traverse_deterministic(data)]
    assert paths[:3] == ["$", "$.a", "$.b"]


def test_traversal_arrays_are_ascending() -> None:
    data = ["x", "y"]
    paths = [path for path, _ in traverse_deterministic(data)]
    assert paths == ["$", "$[0]", "$[1]"]


def test_traversal_non_ascii_object_key_uses_bracket_notation() -> None:
    data = {"\u00e9": 1}
    paths = [path for path, _ in traverse_deterministic(data)]
    assert paths == ["$", "$['\u00e9']"]
    assert evaluate_jsonpath(data, paths[1]) == [1]
