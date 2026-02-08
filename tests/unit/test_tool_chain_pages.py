"""Tests for artifact.chain_pages tool implementation."""

from __future__ import annotations

from datetime import datetime, timezone

from mcp_artifact_gateway.tools.artifact_chain_pages import (
    build_chain_pages_response,
    validate_chain_pages_args,
)


# ---- validate_chain_pages_args ----

def test_validate_chain_pages_args_requires_session_id() -> None:
    result = validate_chain_pages_args({})
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "session_id" in result["message"]


def test_validate_chain_pages_args_requires_parent_artifact_id() -> None:
    result = validate_chain_pages_args(
        {"_gateway_context": {"session_id": "sess_1"}}
    )
    assert result is not None
    assert result["code"] == "INVALID_ARGUMENT"
    assert "parent_artifact_id" in result["message"]


def test_validate_chain_pages_args_accepts_valid_arguments() -> None:
    result = validate_chain_pages_args(
        {
            "_gateway_context": {"session_id": "sess_1"},
            "parent_artifact_id": "art_parent",
        }
    )
    assert result is None


# ---- build_chain_pages_response ----

def test_build_chain_pages_response_formats_rows() -> None:
    ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "artifact_id": "art_page_1",
            "created_seq": 10,
            "created_at": ts,
            "chain_seq": 0,
            "source_tool": "github.search",
            "payload_total_bytes": 5000,
            "map_kind": "full",
            "map_status": "ready",
        },
        {
            "artifact_id": "art_page_2",
            "created_seq": 11,
            "created_at": ts,
            "chain_seq": 1,
            "source_tool": "github.search",
            "payload_total_bytes": 3000,
            "map_kind": "full",
            "map_status": "ready",
        },
    ]
    result = build_chain_pages_response(rows)

    assert len(result["items"]) == 2
    assert result["truncated"] is False
    assert result["cursor"] is None

    item0 = result["items"][0]
    assert item0["artifact_id"] == "art_page_1"
    assert item0["created_seq"] == 10
    assert item0["created_at"] == str(ts)
    assert item0["chain_seq"] == 0
    assert item0["source_tool"] == "github.search"
    assert item0["payload_total_bytes"] == 5000
    assert item0["map_kind"] == "full"
    assert item0["map_status"] == "ready"


def test_build_chain_pages_response_empty_rows() -> None:
    result = build_chain_pages_response([])
    assert result["items"] == []
    assert result["truncated"] is False


def test_build_chain_pages_response_with_truncated_and_cursor() -> None:
    ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "artifact_id": "art_1",
            "created_seq": 1,
            "created_at": ts,
        },
    ]
    result = build_chain_pages_response(rows, truncated=True, cursor="cur_next")
    assert result["truncated"] is True
    assert result["cursor"] == "cur_next"


def test_build_chain_pages_response_handles_missing_optional_fields() -> None:
    ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "artifact_id": "art_1",
            "created_seq": 1,
            "created_at": ts,
        },
    ]
    result = build_chain_pages_response(rows)
    item = result["items"][0]

    assert item["chain_seq"] is None
    assert item["source_tool"] is None
    assert item["payload_total_bytes"] is None
    assert item["map_kind"] is None
    assert item["map_status"] is None
