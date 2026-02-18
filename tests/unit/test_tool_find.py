"""Tests for artifact.find tool implementation."""

from __future__ import annotations

from sift_mcp.tools.artifact_find import (
    build_find_response,
    sampled_indices_from_rows,
    validate_find_args,
)

# ---- validate_find_args ----


def test_validate_find_args_requires_session_id() -> None:
    result = validate_find_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_find_args_requires_artifact_id() -> None:
    result = validate_find_args({"_gateway_context": {"session_id": "sess_1"}})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "artifact_id" in result["message"]


def test_validate_find_args_accepts_valid() -> None:
    result = validate_find_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        }
    )
    assert result is None


# ---- sampled_indices_from_rows ----


def test_sampled_indices_from_rows_sorts_ascending() -> None:
    rows = [
        {"sample_index": 20, "record": {}},
        {"sample_index": 5, "record": {}},
        {"sample_index": 12, "record": {}},
    ]
    assert sampled_indices_from_rows(rows) == [5, 12, 20]


def test_sampled_indices_from_rows_already_sorted() -> None:
    rows = [
        {"sample_index": 0, "record": {}},
        {"sample_index": 1, "record": {}},
        {"sample_index": 2, "record": {}},
    ]
    assert sampled_indices_from_rows(rows) == [0, 1, 2]


def test_sampled_indices_from_rows_empty() -> None:
    assert sampled_indices_from_rows([]) == []


def test_sampled_indices_from_rows_skips_non_int() -> None:
    rows = [
        {"sample_index": 7, "record": {}},
        {"sample_index": "invalid", "record": {}},
        {"sample_index": None, "record": {}},
        {"sample_index": 2, "record": {}},
    ]
    assert sampled_indices_from_rows(rows) == [2, 7]


def test_sampled_indices_from_rows_skips_missing_key() -> None:
    rows = [
        {"record": {}},
        {"sample_index": 3, "record": {}},
    ]
    assert sampled_indices_from_rows(rows) == [3]


# ---- build_find_response ----


def test_build_find_response_sampled_only_true_when_index_off() -> None:
    result = build_find_response(
        items=[{"root_path": "$.data", "index": 0, "record_hash": "abc"}],
        truncated=False,
        sampled_only=True,
        index_status="off",
    )
    assert result["sampled_only"] is True
    assert result["truncated"] is False
    assert result["items"] == [
        {"root_path": "$.data", "index": 0, "record_hash": "abc"}
    ]
    assert "cursor" not in result
    assert "hint" in result
    assert result["pagination"]["layer"] == "artifact_retrieval"
    assert result["pagination"]["retrieval_status"] == "COMPLETE"


def test_build_find_response_sampled_only_false_when_index_ready() -> None:
    result = build_find_response(
        items=[],
        truncated=False,
        sampled_only=True,
        index_status="ready",
    )
    assert result["sampled_only"] is False


def test_build_find_response_with_cursor() -> None:
    result = build_find_response(
        items=[],
        truncated=True,
        cursor="cursor_abc",
        sampled_only=True,
        index_status="off",
    )
    assert result["truncated"] is True
    assert result["cursor"] == "cursor_abc"
    assert result["pagination"]["retrieval_status"] == "PARTIAL"
    assert result["pagination"]["partial_reason"] == "CURSOR_AVAILABLE"
    assert result["pagination"]["next_cursor"] == "cursor_abc"


def test_build_find_response_no_cursor_when_not_truncated() -> None:
    result = build_find_response(
        items=[],
        truncated=False,
        cursor=None,
    )
    assert "cursor" not in result


def test_build_find_response_default_sampled_only() -> None:
    """Default sampled_only=True and index_status='off'."""
    result = build_find_response(
        items=[],
        truncated=False,
    )
    assert result["sampled_only"] is True


def test_build_find_response_includes_hint() -> None:
    result = build_find_response(items=[], truncated=False)
    assert "hint" in result
    assert "artifact_select" in result["hint"]


def test_build_find_response_includes_matched_count() -> None:
    result = build_find_response(
        items=[{"root_path": "$.data", "index": 0}],
        truncated=True,
        cursor="c1",
        matched_count=42,
    )
    assert result["matched_count"] == 42


def test_build_find_response_omits_matched_count_when_none() -> None:
    result = build_find_response(items=[], truncated=False)
    assert "matched_count" not in result
