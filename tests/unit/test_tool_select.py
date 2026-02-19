"""Tests for artifact.select tool implementation."""

from __future__ import annotations

from sift_gateway.core.artifact_select import _distinct_key
from sift_gateway.tools.artifact_select import (
    SelectOrderBy,
    _apply_select_sort,
    _sort_key_for_item,
    build_select_result,
    parse_select_order_by,
    sampled_indices_ascending,
    validate_select_args,
    validate_select_order_by,
)

# ---- validate_select_args ----


def test_validate_select_args_requires_session_id() -> None:
    result = validate_select_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_select_args_requires_artifact_id() -> None:
    result = validate_select_args(
        {"_gateway_context": {"session_id": "sess_1"}}
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "artifact_id" in result["message"]


def test_validate_select_args_requires_root_path() -> None:
    result = validate_select_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        }
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "root_path" in result["message"]


def test_validate_select_args_requires_select_paths_as_nonempty_list() -> None:
    base = {
        "_gateway_context": {"session_id": "sess_1"},
        "artifact_id": "art_1",
        "root_path": "$.data",
    }

    # Missing select_paths
    result = validate_select_args(base)
    assert result is not None
    assert "select_paths" in result["message"]

    # Empty list
    result = validate_select_args({**base, "select_paths": []})
    assert result is not None
    assert "select_paths" in result["message"]

    # Not a list
    result = validate_select_args({**base, "select_paths": "name"})
    assert result is not None
    assert "select_paths" in result["message"]


def test_validate_select_args_rejects_absolute_paths_in_select_paths() -> None:
    result = validate_select_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.data",
            "select_paths": ["$.name"],
        }
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "relative" in result["message"]


def test_validate_select_args_accepts_valid_arguments() -> None:
    result = validate_select_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.data",
            "select_paths": ["name", "id"],
        }
    )
    assert result is None


# ---- build_select_result ----


def test_build_select_result_basic() -> None:
    result = build_select_result(
        items=[{"name": "alice"}],
        truncated=False,
        cursor=None,
    )
    assert result["items"] == [{"name": "alice"}]
    assert result["truncated"] is False
    assert "cursor" not in result
    assert "sampled_only" not in result
    assert result["pagination"]["layer"] == "artifact_retrieval"
    assert result["pagination"]["retrieval_status"] == "COMPLETE"


def test_build_select_result_with_cursor() -> None:
    result = build_select_result(
        items=[{"name": "alice"}],
        truncated=True,
        cursor="cur_abc",
    )
    assert result["cursor"] == "cur_abc"
    assert result["truncated"] is True
    assert result["pagination"]["retrieval_status"] == "PARTIAL"
    assert result["pagination"]["partial_reason"] == "CURSOR_AVAILABLE"
    assert result["pagination"]["next_cursor"] == "cur_abc"


def test_build_select_result_with_sampled_only() -> None:
    result = build_select_result(
        items=[{"name": "alice"}],
        truncated=False,
        cursor=None,
        sampled_only=True,
        sample_indices_used=[0, 5, 10],
        sampled_prefix_len=100,
    )
    assert result["sampled_only"] is True
    assert result["sample_indices_used"] == [0, 5, 10]
    assert result["sampled_prefix_len"] == 100


def test_build_select_result_without_sampled_only_excludes_fields() -> None:
    result = build_select_result(
        items=[],
        truncated=False,
        cursor=None,
        sampled_only=False,
    )
    assert "sampled_only" not in result
    assert "sample_indices_used" not in result
    assert "sampled_prefix_len" not in result


def test_build_select_result_with_omitted_and_stats() -> None:
    result = build_select_result(
        items=[],
        truncated=False,
        cursor=None,
        omitted={"count": 5, "reason": "budget"},
        stats={"bytes_scanned": 1000},
    )
    assert result["omitted"] == {"count": 5, "reason": "budget"}
    assert result["stats"] == {"bytes_scanned": 1000}


# ---- sampled_indices_ascending ----


def test_sampled_indices_ascending_extracts_and_sorts() -> None:
    rows = [
        {"sample_index": 10, "record": {}},
        {"sample_index": 2, "record": {}},
        {"sample_index": 7, "record": {}},
    ]
    assert sampled_indices_ascending(rows) == [2, 7, 10]


def test_sampled_indices_ascending_already_sorted() -> None:
    rows = [
        {"sample_index": 0, "record": {}},
        {"sample_index": 5, "record": {}},
        {"sample_index": 99, "record": {}},
    ]
    assert sampled_indices_ascending(rows) == [0, 5, 99]


def test_sampled_indices_ascending_empty() -> None:
    assert sampled_indices_ascending([]) == []


def test_sampled_indices_ascending_skips_non_int() -> None:
    rows = [
        {"sample_index": 3, "record": {}},
        {"sample_index": "bad", "record": {}},
        {"sample_index": None, "record": {}},
        {"sample_index": 1, "record": {}},
    ]
    assert sampled_indices_ascending(rows) == [1, 3]


def test_sampled_indices_ascending_skips_missing_key() -> None:
    rows = [
        {"record": {}},
        {"sample_index": 5, "record": {}},
    ]
    assert sampled_indices_ascending(rows) == [5]


# ---- sampled_only in build_select_result ----


def test_build_select_result_sampled_only_indices_always_ascending() -> None:
    """sample_indices_used should be stored as-is (caller responsibility)."""
    result = build_select_result(
        items=[],
        truncated=False,
        cursor=None,
        sampled_only=True,
        sample_indices_used=[0, 3, 7],
    )
    assert result["sampled_only"] is True
    assert result["sample_indices_used"] == [0, 3, 7]


# ---- parse_select_order_by ----


def test_parse_simple_field_default_asc() -> None:
    result = parse_select_order_by("name")
    assert result is not None
    assert result.field == "name"
    assert result.direction == "ASC"
    assert result.cast is None


def test_parse_field_desc() -> None:
    result = parse_select_order_by("spend DESC")
    assert result is not None
    assert result.field == "spend"
    assert result.direction == "DESC"
    assert result.cast is None


def test_parse_field_asc_explicit() -> None:
    result = parse_select_order_by("name ASC")
    assert result is not None
    assert result.field == "name"
    assert result.direction == "ASC"


def test_parse_to_number_cast() -> None:
    result = parse_select_order_by("to_number(spend) DESC")
    assert result is not None
    assert result.field == "spend"
    assert result.direction == "DESC"
    assert result.cast == "to_number"


def test_parse_to_string_cast() -> None:
    result = parse_select_order_by("to_string(id) ASC")
    assert result is not None
    assert result.field == "id"
    assert result.cast == "to_string"


def test_parse_search_mode_returns_none() -> None:
    assert parse_select_order_by("created_seq_desc") is None
    assert parse_select_order_by("last_seen_desc") is None
    assert parse_select_order_by("chain_seq_asc") is None


def test_parse_empty_returns_none() -> None:
    assert parse_select_order_by("") is None
    assert parse_select_order_by("  ") is None


def test_parse_case_insensitive_direction() -> None:
    result = parse_select_order_by("spend desc")
    assert result is not None
    assert result.direction == "DESC"


# ---- validate_select_order_by ----


def test_validate_search_mode_returns_none() -> None:
    assert validate_select_order_by("created_seq_desc") is None


def test_validate_valid_select_order() -> None:
    assert validate_select_order_by("spend DESC") is None


def test_validate_empty_returns_none() -> None:
    assert validate_select_order_by("") is None


# ---- _sort_key_for_item ----


def test_sort_key_valid_value() -> None:
    item = {"projection": {"spend": "100"}}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (0, "100")


def test_sort_key_to_number_cast() -> None:
    item = {"projection": {"spend": "42.5"}}
    key = _sort_key_for_item(item, "spend", "to_number")
    assert key == (0, 42.5)


def test_sort_key_to_number_invalid() -> None:
    item = {"projection": {"spend": "not_a_number"}}
    key = _sort_key_for_item(item, "spend", "to_number")
    assert key == (1, "")


def test_sort_key_missing_field() -> None:
    item = {"projection": {"name": "alice"}}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (1, "")


def test_sort_key_none_value() -> None:
    item = {"projection": {"spend": None}}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (1, "")


def test_sort_key_no_projection() -> None:
    item = {"other": "data"}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (1, "")


def test_sort_key_to_string_cast() -> None:
    item = {"projection": {"id": 42}}
    key = _sort_key_for_item(item, "id", "to_string")
    assert key == (0, "42")


def test_sort_key_canonical_jsonpath_key() -> None:
    """Projection keys are canonical JSONPaths like $.spend."""
    item = {"projection": {"$.spend": "100"}}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (0, "100")


def test_sort_key_canonical_with_cast() -> None:
    item = {"projection": {"$.spend": "42.5"}}
    key = _sort_key_for_item(item, "spend", "to_number")
    assert key == (0, 42.5)


def test_sort_key_canonical_none_value() -> None:
    item = {"projection": {"$.spend": None}}
    key = _sort_key_for_item(item, "spend", None)
    assert key == (1, "")


# ---- _apply_select_sort ----


def test_apply_sort_canonical_keys() -> None:
    """Sort works with canonical projection keys ($.field)."""
    items = [
        {"projection": {"$.spend": "5"}},
        {"projection": {"$.spend": "100"}},
        {"projection": {"$.spend": "20"}},
    ]
    order = SelectOrderBy(field="spend", direction="DESC", cast="to_number")
    result = _apply_select_sort(items, order)
    values = [i["projection"]["$.spend"] for i in result]
    assert values == ["100", "20", "5"]


# ---- _apply_select_sort (bare keys) ----


def test_apply_sort_desc() -> None:
    items = [
        {"projection": {"spend": "10"}},
        {"projection": {"spend": "50"}},
        {"projection": {"spend": "20"}},
    ]
    order = SelectOrderBy(field="spend", direction="DESC", cast=None)
    result = _apply_select_sort(items, order)
    values = [i["projection"]["spend"] for i in result]
    assert values == ["50", "20", "10"]


def test_apply_sort_asc() -> None:
    items = [
        {"projection": {"name": "charlie"}},
        {"projection": {"name": "alice"}},
        {"projection": {"name": "bob"}},
    ]
    order = SelectOrderBy(field="name", direction="ASC", cast=None)
    result = _apply_select_sort(items, order)
    values = [i["projection"]["name"] for i in result]
    assert values == ["alice", "bob", "charlie"]


def test_apply_sort_with_to_number() -> None:
    items = [
        {"projection": {"spend": "5"}},
        {"projection": {"spend": "100"}},
        {"projection": {"spend": "20"}},
    ]
    order = SelectOrderBy(field="spend", direction="DESC", cast="to_number")
    result = _apply_select_sort(items, order)
    values = [i["projection"]["spend"] for i in result]
    assert values == ["100", "20", "5"]


def test_apply_sort_none_values_sort_last_asc() -> None:
    items = [
        {"projection": {"spend": "10"}},
        {"projection": {"spend": None}},
        {"projection": {"spend": "5"}},
    ]
    order = SelectOrderBy(field="spend", direction="ASC", cast=None)
    result = _apply_select_sort(items, order)
    # String ASC: "10" < "5" (lexicographic), None sorts last
    assert result[0]["projection"]["spend"] == "10"
    assert result[1]["projection"]["spend"] == "5"
    assert result[-1]["projection"]["spend"] is None


def test_apply_sort_none_values_sort_last_desc() -> None:
    items = [
        {"projection": {"spend": "10"}},
        {"projection": {"spend": None}},
        {"projection": {"spend": "5"}},
    ]
    order = SelectOrderBy(field="spend", direction="DESC", cast=None)
    result = _apply_select_sort(items, order)
    # Non-None values sorted DESC, None last
    assert result[-1]["projection"]["spend"] is None


def test_apply_sort_mixed_types_no_error() -> None:
    """Mixed JSON types should sort without raising TypeError."""
    items = [
        {"projection": {"val": 42}},
        {"projection": {"val": "hello"}},
        {"projection": {"val": 7}},
    ]
    order = SelectOrderBy(field="val", direction="ASC", cast=None)
    result = _apply_select_sort(items, order)
    # Should not raise; all 3 items present in output.
    assert len(result) == 3


def test_apply_sort_empty_list() -> None:
    order = SelectOrderBy(field="spend", direction="ASC", cast=None)
    result = _apply_select_sort([], order)
    assert result == []


# ---- validate_select_order_by ----


def test_validate_rejects_malformed_order_by() -> None:
    result = validate_select_order_by("name DOWN")
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"


def test_validate_accepts_bare_field() -> None:
    assert validate_select_order_by("name") is None


def test_validate_accepts_cast_no_direction() -> None:
    assert validate_select_order_by("to_number(spend)") is None


# ---- _distinct_key ----


def test_distinct_key_none() -> None:
    assert _distinct_key(None) == "null"


def test_distinct_key_json_object_string() -> None:
    """SQLite returns JSON objects as strings."""
    assert _distinct_key('{"b": 2, "a": 1}') == '{"a": 1, "b": 2}'


def test_distinct_key_json_array_string() -> None:
    assert _distinct_key("[1, 2, 3]") == "[1, 2, 3]"


def test_distinct_key_bare_scalar_string() -> None:
    """Bare strings from scalar record columns don't parse as JSON."""
    key = _distinct_key("alpha")
    assert key == '"alpha"'


def test_distinct_key_int() -> None:
    assert _distinct_key(42) == "42"


def test_distinct_key_float() -> None:
    assert _distinct_key(3.14) == "3.14"


def test_distinct_key_bool() -> None:
    assert _distinct_key(True) == "true"
