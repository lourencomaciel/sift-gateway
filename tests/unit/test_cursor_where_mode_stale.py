from __future__ import annotations

from mcp_artifact_gateway.cursor.payload import (
    CursorStaleError,
    assert_cursor_binding,
    build_cursor_payload,
)


def test_cursor_where_mode_stale() -> None:
    payload = build_cursor_payload(
        tool="artifact.select",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=10,
        where_canonicalization_mode="raw_string",
    )
    try:
        assert_cursor_binding(
            payload,
            expected_tool="artifact.select",
            expected_artifact_id="art_1",
            expected_where_mode="canonical_ast",
        )
    except CursorStaleError as exc:
        assert "where_canonicalization_mode mismatch" in str(exc)
    else:
        raise AssertionError("expected CursorStaleError")
