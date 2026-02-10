"""Tests for artifact.select tool implementation."""

from __future__ import annotations

from mcp_artifact_gateway.tools.artifact_select import (
    build_select_result,
    sampled_indices_ascending,
    validate_select_args,
)


# ---- validate_select_args ----


def test_validate_select_args_requires_session_id() -> None:
    result = validate_select_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_select_args_requires_artifact_id() -> None:
    result = validate_select_args({"_gateway_context": {"session_id": "sess_1"}})
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


def test_build_select_result_with_cursor() -> None:
    result = build_select_result(
        items=[{"name": "alice"}],
        truncated=True,
        cursor="cur_abc",
    )
    assert result["cursor"] == "cur_abc"
    assert result["truncated"] is True


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
