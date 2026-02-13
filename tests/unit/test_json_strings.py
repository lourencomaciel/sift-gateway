"""Tests for resolve_json_strings: recursive JSON string resolution."""

from __future__ import annotations

import json

from sift_mcp.mapping.json_strings import resolve_json_strings


def test_dict_with_json_object_string_is_parsed() -> None:
    """A string value containing a JSON object is replaced with the parsed dict."""
    value = {"result": json.dumps({"data": [1, 2, 3]})}
    resolved = resolve_json_strings(value)

    assert isinstance(resolved["result"], dict)
    assert resolved["result"] == {"data": [1, 2, 3]}


def test_dict_with_json_array_string_is_parsed() -> None:
    """A string value containing a JSON array is replaced with the parsed list."""
    value = {"items": json.dumps([{"id": 1}, {"id": 2}])}
    resolved = resolve_json_strings(value)

    assert isinstance(resolved["items"], list)
    assert resolved["items"] == [{"id": 1}, {"id": 2}]


def test_nested_json_strings_resolved_recursively() -> None:
    """JSON strings within parsed JSON strings are also resolved."""
    inner = json.dumps({"key": "val"})
    outer = json.dumps({"nested": inner})
    value = {"wrapper": outer}
    resolved = resolve_json_strings(value)

    assert resolved == {"wrapper": {"nested": {"key": "val"}}}


def test_non_json_strings_left_alone() -> None:
    """Plain strings that aren't valid JSON remain unchanged."""
    value = {"name": "alice", "note": "not json at all"}
    resolved = resolve_json_strings(value)

    assert resolved == value


def test_scalar_json_strings_left_as_strings() -> None:
    """Strings that decode to JSON scalars (numbers, bools) stay as strings."""
    value = {"num": "42", "flag": "true", "nil": "null"}
    resolved = resolve_json_strings(value)

    assert resolved["num"] == "42"
    assert resolved["flag"] == "true"
    assert resolved["nil"] == "null"


def test_none_passthrough() -> None:
    """None is returned unchanged."""
    assert resolve_json_strings(None) is None


def test_int_passthrough() -> None:
    """Integers are returned unchanged."""
    assert resolve_json_strings(42) == 42


def test_bool_passthrough() -> None:
    """Booleans are returned unchanged."""
    assert resolve_json_strings(True) is True


def test_list_with_json_strings() -> None:
    """JSON strings inside a list are resolved."""
    value = [json.dumps({"a": 1}), "plain", 99]
    resolved = resolve_json_strings(value)

    assert resolved[0] == {"a": 1}
    assert resolved[1] == "plain"
    assert resolved[2] == 99


def test_depth_limit_prevents_infinite_nesting() -> None:
    """Parsing stops after max_depth levels of JSON-in-JSON."""
    # Build 5 levels of nesting
    inner = json.dumps({"deep": "value"})
    for _ in range(4):
        inner = json.dumps({"wrap": inner})

    resolved = resolve_json_strings({"data": inner}, max_depth=2)

    # After 2 levels of string parsing, the remaining should still be a string
    level1 = resolved["data"]
    assert isinstance(level1, dict)
    level2 = level1["wrap"]
    assert isinstance(level2, dict)
    # Level 3 should still be a string (depth exhausted)
    level3 = level2["wrap"]
    assert isinstance(level3, str)


def test_empty_dict_passthrough() -> None:
    """Empty dict is returned as-is."""
    assert resolve_json_strings({}) == {}


def test_empty_list_passthrough() -> None:
    """Empty list is returned as-is."""
    assert resolve_json_strings([]) == []


def test_original_value_not_mutated() -> None:
    """The input dict is not modified in place."""
    original = {"result": json.dumps({"data": [1]})}
    original_copy = {"result": original["result"]}
    resolve_json_strings(original)

    assert original == original_copy


def test_meta_ads_style_structured_content() -> None:
    """Realistic meta-ads-mcp response with double-encoded JSON."""
    campaigns = [
        {"id": "123", "name": "Campaign A", "status": "ACTIVE"},
        {"id": "456", "name": "Campaign B", "status": "PAUSED"},
    ]
    paging = {"cursors": {"after": "abc"}}
    inner_json = json.dumps({"data": campaigns, "paging": paging})

    value = {"result": inner_json}
    resolved = resolve_json_strings(value)

    assert isinstance(resolved["result"], dict)
    assert isinstance(resolved["result"]["data"], list)
    assert len(resolved["result"]["data"]) == 2
    assert resolved["result"]["data"][0]["name"] == "Campaign A"
    assert resolved["result"]["paging"]["cursors"]["after"] == "abc"
