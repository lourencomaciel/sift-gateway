"""Tests for artifact.search tool implementation."""

from __future__ import annotations

from mcp_artifact_gateway.tools.artifact_search import (
    build_search_query,
    validate_search_args,
)

# ---- validate_search_args ----


def test_validate_search_args_requires_session_id() -> None:
    result = validate_search_args({}, max_limit=200)
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_search_args_requires_session_id_in_context() -> None:
    result = validate_search_args({"_gateway_context": {}}, max_limit=200)
    assert result["code"] == "INVALID_ARGUMENT"


def test_validate_search_args_rejects_invalid_order_by() -> None:
    result = validate_search_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "order_by": "invalid_order",
        },
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "invalid order_by" in result["message"]


def test_validate_search_args_accepts_valid_order_by() -> None:
    for order in ("created_seq_desc", "last_seen_desc"):
        result = validate_search_args(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "order_by": order,
            },
            max_limit=200,
        )
        assert "code" not in result
        assert result["order_by"] == order


def test_validate_search_args_caps_limit_at_max() -> None:
    result = validate_search_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "limit": 500,
        },
        max_limit=200,
    )
    assert result["limit"] == 200


def test_validate_search_args_uses_default_limit() -> None:
    result = validate_search_args(
        {"_gateway_context": {"session_id": "sess_1"}},
        max_limit=200,
    )
    assert result["limit"] == 50


def test_validate_search_args_returns_parsed_fields() -> None:
    result = validate_search_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "filters": {"source_tool": "github.search"},
            "cursor": "abc123",
        },
        max_limit=200,
    )
    assert result["session_id"] == "sess_1"
    assert result["filters"] == {"source_tool": "github.search"}
    assert result["cursor"] == "abc123"


def test_validate_search_args_rejects_invalid_status_filter() -> None:
    result = validate_search_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "filters": {"status": "unknown"},
        },
        max_limit=200,
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "status" in result["message"]


# ---- build_search_query ----


def test_build_search_query_base_has_workspace_and_session() -> None:
    sql, params = build_search_query("sess_1", {}, "created_seq_desc", 50)
    assert "ar.workspace_id = %s" in sql
    assert "ar.session_id = %s" in sql
    assert "a.status" not in sql
    assert "CASE WHEN a.error_summary IS NULL" in sql
    assert "END AS status" in sql
    assert params[0] == "local"
    assert params[1] == "sess_1"


def test_build_search_query_excludes_deleted_by_default() -> None:
    sql, params = build_search_query("sess_1", {}, "created_seq_desc", 50)
    assert "a.deleted_at IS NULL" in sql


def test_build_search_query_includes_deleted_when_requested() -> None:
    sql, params = build_search_query(
        "sess_1", {"include_deleted": True}, "created_seq_desc", 50
    )
    assert "a.deleted_at IS NULL" not in sql


def test_build_search_query_source_tool_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"source_tool": "github.search"}, "created_seq_desc", 50
    )
    assert "a.source_tool = %s" in sql
    assert "github.search" in params


def test_build_search_query_source_tool_prefix_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"source_tool_prefix": "github"}, "created_seq_desc", 50
    )
    assert "a.source_tool LIKE %s" in sql
    assert "github.%" in params


def test_build_search_query_upstream_instance_id_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"upstream_instance_id": "up_1"}, "created_seq_desc", 50
    )
    assert "a.upstream_instance_id = %s" in sql
    assert "up_1" in params


def test_build_search_query_request_key_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"request_key": "rk_abc"}, "created_seq_desc", 50
    )
    assert "a.request_key = %s" in sql
    assert "rk_abc" in params


def test_build_search_query_payload_hash_full_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"payload_hash_full": "sha256_abc"}, "created_seq_desc", 50
    )
    assert "a.payload_hash_full = %s" in sql
    assert "sha256_abc" in params


def test_build_search_query_parent_artifact_id_filter() -> None:
    sql, params = build_search_query(
        "sess_1", {"parent_artifact_id": "art_parent"}, "created_seq_desc", 50
    )
    assert "a.parent_artifact_id = %s" in sql
    assert "art_parent" in params


def test_build_search_query_created_seq_range_filters() -> None:
    sql, params = build_search_query(
        "sess_1",
        {"created_seq_min": 10, "created_seq_max": 100},
        "created_seq_desc",
        50,
    )
    assert "a.created_seq >= %s" in sql
    assert "a.created_seq <= %s" in sql
    assert 10 in params
    assert 100 in params


def test_build_search_query_created_at_range_filters() -> None:
    sql, params = build_search_query(
        "sess_1",
        {"created_at_after": "2024-01-01", "created_at_before": "2024-12-31"},
        "created_seq_desc",
        50,
    )
    assert "a.created_at >= %s" in sql
    assert "a.created_at <= %s" in sql
    assert "2024-01-01" in params
    assert "2024-12-31" in params


def test_build_search_query_order_by_created_seq_desc() -> None:
    sql, _ = build_search_query("sess_1", {}, "created_seq_desc", 50)
    assert "ORDER BY a.created_seq DESC" in sql


def test_build_search_query_order_by_last_seen_desc() -> None:
    sql, _ = build_search_query("sess_1", {}, "last_seen_desc", 50)
    assert "ORDER BY ar.last_seen_at DESC" in sql


def test_build_search_query_fetches_limit_plus_one() -> None:
    sql, params = build_search_query("sess_1", {}, "created_seq_desc", 50)
    assert "LIMIT %s" in sql
    assert params[-1] == 51


def test_build_search_query_status_error_filter() -> None:
    sql, _ = build_search_query(
        "sess_1", {"status": "error"}, "created_seq_desc", 50
    )
    assert "a.error_summary IS NOT NULL" in sql


def test_build_search_query_status_ok_filter() -> None:
    sql, _ = build_search_query(
        "sess_1", {"status": "ok"}, "created_seq_desc", 50
    )
    assert "a.error_summary IS NULL" in sql
