from __future__ import annotations

import pytest

from sift_mcp.cursor.payload import (
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
    with pytest.raises(
        CursorStaleError, match="where_canonicalization_mode mismatch"
    ):
        assert_cursor_binding(
            payload,
            expected_tool="artifact.select",
            expected_artifact_id="art_1",
            expected_where_mode="canonical_ast",
        )
