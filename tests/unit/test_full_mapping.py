"""Tests for full mapping: root discovery, fields_top, root_path normalization."""

from __future__ import annotations

from sidepouch_mcp.mapping.full import run_full_mapping


def test_simple_array_produces_correct_root_with_count() -> None:
    """Mapping a simple array produces a root at $ with correct count."""
    data = [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 1
    root = roots[0]
    assert root.root_key == "$"
    assert root.root_path == "$"
    assert root.root_shape == "array"
    assert root.count_estimate == 3
    assert root.inventory_coverage == 1.0
    assert root.root_score == 3.0


def test_object_with_nested_arrays_discovers_up_to_k_roots() -> None:
    """Mapping an object with nested arrays discovers up to K=3 roots."""
    data = {
        "users": [{"id": 1}, {"id": 2}],
        "orders": [{"oid": 1}, {"oid": 2}, {"oid": 3}],
        "products": [{"pid": 1}],
        "metadata": {"version": "1.0"},
        "extra_list": [1, 2, 3, 4, 5],
    }
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 3
    # Sorted by score descending: extra_list(5), orders(3), users(2)
    assert roots[0].root_key == "extra_list"
    assert roots[0].count_estimate == 5
    assert roots[1].root_key == "orders"
    assert roots[1].count_estimate == 3
    assert roots[2].root_key == "users"
    assert roots[2].count_estimate == 2


def test_fields_top_correctly_reports_field_types() -> None:
    """fields_top should report type distributions for each field."""
    data = [
        {"id": 1, "name": "alice", "active": True},
        {"id": 2, "name": "bob", "active": False},
        {"id": 3, "name": None, "active": True},
    ]
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 1
    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert "id" in fields_top
    assert "number" in fields_top["id"]
    assert fields_top["id"]["number"] == 3
    assert "name" in fields_top
    assert fields_top["name"]["string"] == 2
    assert fields_top["name"]["null"] == 1
    assert "active" in fields_top
    assert fields_top["active"]["boolean"] == 3


def test_root_path_normalization_valid_identifier() -> None:
    """Root paths use .name notation for valid identifiers."""
    data = {"users": [{"id": 1}]}
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 1
    assert roots[0].root_path == "$.users"


def test_root_path_normalization_bracket_notation() -> None:
    """Root paths use bracket notation for non-identifier keys."""
    data = {"my-items": [{"id": 1}], "normal": [{"id": 2}]}
    roots = run_full_mapping(data, max_roots=3)

    paths = {r.root_key: r.root_path for r in roots}
    assert paths["my-items"] == "$['my-items']"
    assert paths["normal"] == "$.normal"


def test_empty_array_root() -> None:
    """Empty array produces root with count 0 and no fields_top."""
    data: list[object] = []
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 1
    root = roots[0]
    assert root.count_estimate == 0
    assert root.root_shape == "array"
    assert root.fields_top is None


def test_scalar_root_produces_minimal_root() -> None:
    """Scalar value at root produces a root with no count or shape."""
    roots = run_full_mapping("hello", max_roots=3)

    assert len(roots) == 1
    root = roots[0]
    assert root.root_key == "$"
    assert root.count_estimate is None
    assert root.root_shape is None


def test_object_no_nested_collections_falls_back_to_root() -> None:
    """Object with no nested arrays/objects creates root at $."""
    data = {"a": 1, "b": "hello", "c": True}
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) == 1
    assert roots[0].root_key == "$"
    assert roots[0].root_path == "$"
    assert roots[0].root_shape == "object"
    assert roots[0].count_estimate == 3


def test_max_roots_limits_discovery() -> None:
    """Setting max_roots=1 returns only the highest-scoring root."""
    data = {
        "big": list(range(100)),
        "small": [1, 2],
    }
    roots = run_full_mapping(data, max_roots=1)

    assert len(roots) == 1
    assert roots[0].root_key == "big"


def test_full_mapping_inventory_coverage_is_1() -> None:
    """Full mapping always sets inventory_coverage=1.0 for complete analysis."""
    data = {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}
    roots = run_full_mapping(data, max_roots=3)
    assert len(roots) == 1
    assert roots[0].inventory_coverage == 1.0
    assert roots[0].prefix_coverage is False
    assert roots[0].stop_reason is None
    assert roots[0].sampled_prefix_len is None


def test_full_mapping_fields_top_mixed_types_single_field() -> None:
    """fields_top captures multiple types for the same field."""
    data = [
        {"value": 1},
        {"value": "text"},
        {"value": None},
        {"value": True},
    ]
    roots = run_full_mapping(data, max_roots=3)
    ft = roots[0].fields_top
    assert ft is not None
    assert ft["value"]["number"] == 1
    assert ft["value"]["string"] == 1
    assert ft["value"]["null"] == 1
    assert ft["value"]["boolean"] == 1


def test_full_mapping_nested_object_root() -> None:
    """Object at root with nested objects discovers them as roots."""
    data = {
        "config": {"key1": "val1", "key2": "val2"},
        "data": [{"id": 1}],
    }
    roots = run_full_mapping(data, max_roots=3)
    keys = {r.root_key for r in roots}
    assert "config" in keys
    assert "data" in keys


def test_full_mapping_deterministic_tiebreak() -> None:
    """Roots with equal scores are sorted by key name ascending."""
    data = {
        "beta": [1, 2],
        "alpha": [3, 4],
    }
    roots = run_full_mapping(data, max_roots=3)
    assert len(roots) == 2
    # Both have score 2.0, so alphabetical tiebreak: alpha first
    assert roots[0].root_key == "alpha"
    assert roots[1].root_key == "beta"


def test_full_mapping_root_summary_contains_element_count() -> None:
    """Root summary for array root contains element_count."""
    data = [{"x": 1}, {"x": 2}]
    roots = run_full_mapping(data, max_roots=3)
    assert roots[0].root_summary is not None
    assert roots[0].root_summary.get("element_count") == 2


def test_full_mapping_object_root_summary_contains_key_count() -> None:
    """Root summary for object root contains key_count."""
    data = {"a": 1, "b": 2, "c": 3}
    roots = run_full_mapping(data, max_roots=3)
    assert roots[0].root_summary is not None
    assert roots[0].root_summary.get("key_count") == 3


# -----------------------------------------------------------------------
# JSON-within-JSON (double-encoded string) tests
# -----------------------------------------------------------------------


def test_json_string_value_is_parsed_and_array_discovered() -> None:
    """A JSON-encoded string containing an array is resolved and discovered."""
    import json

    data = {"items": json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])}
    roots = run_full_mapping(data, max_roots=3)

    assert len(roots) >= 1
    root = roots[0]
    assert root.root_path == "$.items"
    assert root.root_shape == "array"
    assert root.count_estimate == 3


def test_nested_json_string_with_dict_discovers_inner_array() -> None:
    """Double-encoded JSON like meta-ads discovers $.result.data array."""
    import json

    campaigns = [
        {"id": "1", "name": "A", "status": "ACTIVE"},
        {"id": "2", "name": "B", "status": "PAUSED"},
        {"id": "3", "name": "C", "status": "ACTIVE"},
    ]
    inner = json.dumps({"data": campaigns, "paging": {"after": "x"}})
    data = {"result": inner}

    roots = run_full_mapping(data, max_roots=3)

    # $.result.data (array, 3 items) should outscore $.result (dict, 2 keys)
    # and $.result.paging (dict, 1 key)
    paths = {r.root_path for r in roots}
    assert "$.result.data" in paths

    data_root = next(r for r in roots if r.root_path == "$.result.data")
    assert data_root.root_shape == "array"
    assert data_root.count_estimate == 3
    assert data_root.fields_top is not None
    assert "name" in data_root.fields_top


def test_non_json_string_values_produce_no_extra_roots() -> None:
    """Plain string values don't create spurious roots."""
    data = {
        "status": "ok",
        "message": "not json",
        "items": [{"id": 1}],
    }
    roots = run_full_mapping(data, max_roots=3)

    # Only $.items should be a root
    assert len(roots) == 1
    assert roots[0].root_path == "$.items"


def test_object_root_fields_top_shows_own_keys() -> None:
    """Object root fields_top reports its own keys, not sub-object keys."""
    data = {
        "data": [{"id": 1}, {"id": 2}],
        "paging": {
            "cursors": {"after": "abc", "before": "xyz"},
            "next": "https://example.com",
        },
    }
    roots = run_full_mapping(data, max_roots=5)
    # $.paging is an object root
    paging_root = next(r for r in roots if r.root_path == "$.paging")
    assert paging_root.root_shape == "object"
    ft = paging_root.fields_top
    assert ft is not None
    # Should show paging's own keys: cursors and next.
    assert "cursors" in ft
    assert ft["cursors"] == {"object": 1}
    assert "next" in ft
    assert ft["next"] == {"string": 1}
    # Should NOT show sub-object keys like "after" or "before".
    assert "after" not in ft
    assert "before" not in ft


def test_scalar_only_object_root_fields_top() -> None:
    """Object root with scalar values shows keys with correct types."""
    data = {"a": 1, "b": "hello", "c": True}
    roots = run_full_mapping(data, max_roots=3)
    assert len(roots) == 1
    ft = roots[0].fields_top
    assert ft is not None
    assert ft["a"] == {"number": 1}
    assert ft["b"] == {"string": 1}
    assert ft["c"] == {"boolean": 1}


def test_depth_exploration_finds_nested_dict_arrays() -> None:
    """Depth exploration discovers arrays nested inside dict values."""
    data = {
        "response": {
            "records": [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}],
            "meta": {"count": 4},
        },
    }
    roots = run_full_mapping(data, max_roots=3)

    paths = {r.root_path for r in roots}
    assert "$.response.records" in paths

    records_root = next(r for r in roots if r.root_path == "$.response.records")
    assert records_root.root_shape == "array"
    assert records_root.count_estimate == 4
