"""Tests for artifact.search argument validation and SQL builder."""

from __future__ import annotations

from sift_gateway.tools.artifact_search import (
    build_search_query,
    validate_search_args,
)


def test_validate_search_args_defaults_to_local_session() -> None:
    result = validate_search_args({}, max_limit=200)
    assert "code" not in result
    assert result["session_id"] == "local"
    assert result["limit"] == 50
    assert result["order_by"] == "created_seq_desc"


def test_validate_search_args_accepts_context_session() -> None:
    result = validate_search_args(
        {"_gateway_context": {"session_id": "sess_1"}},
        max_limit=200,
    )
    assert result["session_id"] == "sess_1"


def test_validate_search_args_rejects_invalid_order_by() -> None:
    result = validate_search_args(
        {"order_by": "invalid"},
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "order_by" in result["message"]


def test_validate_search_args_caps_limit() -> None:
    result = validate_search_args({"limit": 500}, max_limit=200)
    assert result["limit"] == 200


def test_validate_search_args_rejects_bad_status_filter() -> None:
    result = validate_search_args(
        {"filters": {"status": "bad"}},
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "status" in result["message"]


def test_validate_search_args_rejects_bad_capture_kind_filter() -> None:
    result = validate_search_args(
        {"filters": {"capture_kind": "bad"}},
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "capture_kind" in result["message"]


def test_validate_search_args_rejects_unknown_filter_key() -> None:
    result = validate_search_args(
        {"filters": {"unknown": 1}},
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "unknown filter keys" in result["message"]


def test_build_search_query_base() -> None:
    sql, params = build_search_query({}, "created_seq_desc", 50)
    assert "FROM artifacts a" in sql
    assert "a.workspace_id = %s" in sql
    assert "artifact_refs" not in sql
    assert "a.deleted_at IS NULL" in sql
    assert "ORDER BY a.created_seq DESC" in sql
    assert params[0] == "local"
    assert params[-1] == 51


def test_build_search_query_include_deleted() -> None:
    sql, _ = build_search_query(
        {"include_deleted": True}, "created_seq_desc", 10
    )
    assert "a.deleted_at IS NULL" not in sql


def test_build_search_query_applies_filters() -> None:
    sql, params = build_search_query(
        {
            "source_tool": "github.search",
            "upstream_instance_id": "up_1",
            "request_key": "rk_1",
            "payload_hash_full": "ph_1",
            "parent_artifact_id": "art_parent",
            "status": "error",
        },
        "created_seq_desc",
        25,
    )
    assert "a.source_tool = %s" in sql
    assert "a.upstream_instance_id = %s" in sql
    assert "a.request_key = %s" in sql
    assert "a.payload_hash_full = %s" in sql
    assert "a.parent_artifact_id = %s" in sql
    assert "artifact_lineage_edges ale" in sql
    assert "a.error_summary IS NOT NULL" in sql
    assert "github.search" in params
    assert "up_1" in params
    assert "rk_1" in params
    assert "ph_1" in params
    assert "art_parent" in params


def test_build_search_query_applies_capture_filters() -> None:
    sql, params = build_search_query(
        {
            "capture_kind": "mcp_tool",
            "capture_key": "ck_1",
        },
        "created_seq_desc",
        25,
    )
    assert "COALESCE(a.capture_kind" in sql
    assert "COALESCE(a.capture_key, a.request_key) = %s" in sql
    assert "mcp_tool" in params
    assert "ck_1" in params


def test_build_search_query_order_by_last_seen_desc() -> None:
    sql, _ = build_search_query({}, "last_seen_desc", 50)
    assert "ORDER BY a.last_referenced_at DESC" in sql


def test_build_search_query_order_by_chain_seq() -> None:
    sql, _ = build_search_query({}, "chain_seq_asc", 50)
    assert "ORDER BY a.chain_seq ASC NULLS LAST, a.created_seq ASC" in sql


def test_build_search_query_offset() -> None:
    sql, params = build_search_query({}, "created_seq_desc", 5, offset=20)
    assert "OFFSET %s" in sql
    assert params[-1] == 20
