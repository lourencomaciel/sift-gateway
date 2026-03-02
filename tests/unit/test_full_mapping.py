"""Tests for full mapping with canonical root ``$``."""

from __future__ import annotations

import json

from sift_gateway.mapping.full import run_full_mapping


def test_simple_array_maps_canonical_root_with_count() -> None:
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
    assert root.root_summary == {"element_count": 3}


def test_object_maps_canonical_root_with_key_count() -> None:
    data = {
        "users": [{"id": 1}, {"id": 2}],
        "orders": [{"oid": 1}, {"oid": 2}, {"oid": 3}],
        "metadata": {"version": "1.0"},
    }
    roots = run_full_mapping(data, max_roots=1)

    assert len(roots) == 1
    root = roots[0]
    assert root.root_key == "$"
    assert root.root_path == "$"
    assert root.root_shape == "object"
    assert root.count_estimate == 3
    assert root.root_summary == {"key_count": 3}


def test_fields_top_for_array_root_reports_type_distribution() -> None:
    data = [
        {"id": 1, "name": "alice", "active": True},
        {"id": 2, "name": "bob", "active": False},
        {"id": 3, "name": None, "active": True},
    ]
    roots = run_full_mapping(data, max_roots=3)

    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert fields_top["id"] == {"number": 3}
    assert fields_top["name"] == {"string": 2, "null": 1}
    assert fields_top["active"] == {"boolean": 3}


def test_fields_top_for_object_root_shows_only_top_level_keys() -> None:
    data = {
        "data": [{"id": 1}, {"id": 2}],
        "paging": {"cursors": {"after": "abc", "before": "xyz"}},
    }
    roots = run_full_mapping(data, max_roots=3)

    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert fields_top["data"] == {"array": 1}
    assert fields_top["paging"] == {"object": 1}
    assert "after" not in fields_top
    assert "before" not in fields_top


def test_empty_array_root() -> None:
    roots = run_full_mapping([], max_roots=3)
    root = roots[0]
    assert root.root_key == "$"
    assert root.count_estimate == 0
    assert root.root_shape == "array"
    assert root.fields_top is None


def test_scalar_root() -> None:
    roots = run_full_mapping("hello", max_roots=3)
    root = roots[0]
    assert root.root_key == "$"
    assert root.root_path == "$"
    assert root.count_estimate is None
    assert root.root_shape is None
    assert root.fields_top is None


def test_max_roots_parameter_is_ignored_in_canonical_mode() -> None:
    data = {"big": list(range(100)), "small": [1, 2]}

    roots_one = run_full_mapping(data, max_roots=1)
    roots_three = run_full_mapping(data, max_roots=3)

    assert len(roots_one) == 1
    assert len(roots_three) == 1
    assert roots_one[0].root_path == "$"
    assert roots_three[0].root_path == "$"


def test_json_string_value_is_resolved_inside_root_payload() -> None:
    data = {"items": json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])}
    roots = run_full_mapping(data, max_roots=3)

    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert fields_top["items"] == {"array": 1}


def test_nested_json_string_is_resolved_inside_root_payload() -> None:
    campaigns = [
        {"id": "1", "name": "A", "status": "ACTIVE"},
        {"id": "2", "name": "B", "status": "PAUSED"},
    ]
    data = {
        "result": json.dumps({"data": campaigns, "paging": {"after": "x"}})
    }
    roots = run_full_mapping(data, max_roots=3)

    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert fields_top["result"] == {"object": 1}


def test_non_json_string_remains_string() -> None:
    data = {"status": "ok", "message": "not json"}
    roots = run_full_mapping(data, max_roots=3)

    fields_top = roots[0].fields_top
    assert fields_top is not None
    assert fields_top["status"] == {"string": 1}
    assert fields_top["message"] == {"string": 1}

