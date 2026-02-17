from __future__ import annotations

import datetime as dt

import pytest

from sift_mcp.cursor.payload import (
    CursorStaleError,
    assert_cursor_binding,
    build_cursor_payload,
)


def test_cursor_payload_has_required_fields() -> None:
    now = dt.datetime(2026, 2, 8, 12, 0, tzinfo=dt.UTC)
    payload = build_cursor_payload(
        tool="artifact.search",
        artifact_id="art_1",
        position_state={"offset": 10},
        ttl_minutes=30,
        now=now,
    )
    assert payload["cursor_version"] == "cursor_v1"
    assert payload["tool"] == "artifact.search"
    assert payload["artifact_id"] == "art_1"
    assert payload["issued_at"] == "2026-02-08T12:00:00Z"
    assert payload["expires_at"] == "2026-02-08T12:30:00Z"


def test_cursor_payload_rejects_extra_reserved_field_override() -> None:
    with pytest.raises(ValueError, match="reserved cursor fields") as exc_info:
        build_cursor_payload(
            tool="artifact.search",
            artifact_id="art_1",
            position_state={"offset": 10},
            ttl_minutes=30,
            extra={"expires_at": "2099-01-01T00:00:00Z"},
        )
    assert "expires_at" in str(exc_info.value)


def test_cursor_payload_binding_stale_on_mismatch() -> None:
    payload = build_cursor_payload(
        tool="artifact.search",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=5,
    )
    with pytest.raises(CursorStaleError, match="tool mismatch"):
        assert_cursor_binding(
            payload,
            expected_tool="artifact.get",
            expected_artifact_id="art_1",
        )


def test_cursor_payload_binding_stale_on_traversal_contract_mismatch() -> None:
    payload = build_cursor_payload(
        tool="artifact.search",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=5,
    )
    payload["traversal_contract_version"] = "traversal_v0"
    with pytest.raises(
        CursorStaleError, match="traversal_contract_version mismatch"
    ):
        assert_cursor_binding(
            payload,
            expected_tool="artifact.search",
            expected_artifact_id="art_1",
        )


def test_cursor_payload_binding_stale_on_mapper_version_mismatch() -> None:
    payload = build_cursor_payload(
        tool="artifact.search",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=5,
    )
    payload["mapper_version"] = "mapper_v0"
    with pytest.raises(CursorStaleError, match="mapper_version mismatch"):
        assert_cursor_binding(
            payload,
            expected_tool="artifact.search",
            expected_artifact_id="art_1",
        )
