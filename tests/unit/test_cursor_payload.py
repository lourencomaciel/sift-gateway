from __future__ import annotations

import datetime as dt

import pytest

from sift_mcp.cursor.payload import build_cursor_payload


def test_cursor_payload_has_required_fields() -> None:
    now = dt.datetime(2026, 2, 8, 12, 0, tzinfo=dt.UTC)
    payload = build_cursor_payload(
        tool="artifact.search",
        artifact_id="art_1",
        position_state={"offset": 10},
        ttl_minutes=30,
        now=now,
    )
    assert payload["tool"] == "artifact.search"
    assert payload["artifact_id"] == "art_1"
    assert payload["issued_at"] == "2026-02-08T12:00:00Z"
    assert payload["expires_at"] == "2026-02-08T12:30:00Z"
    assert payload["position_state"] == {"offset": 10}


def test_cursor_payload_rejects_extra_reserved_field_override() -> None:
    with pytest.raises(
        ValueError, match="reserved cursor fields"
    ) as exc_info:
        build_cursor_payload(
            tool="artifact.search",
            artifact_id="art_1",
            position_state={"offset": 10},
            ttl_minutes=30,
            extra={"expires_at": "2099-01-01T00:00:00Z"},
        )
    assert "expires_at" in str(exc_info.value)


def test_cursor_payload_merges_extra_fields() -> None:
    payload = build_cursor_payload(
        tool="artifact",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=10,
        extra={"root_path": "$.items", "scope": "single"},
    )
    assert payload["root_path"] == "$.items"
    assert payload["scope"] == "single"
