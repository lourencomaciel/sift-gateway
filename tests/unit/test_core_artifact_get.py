from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sift_mcp.core.artifact_get import execute_artifact_get


class _FakeCursor:
    def __init__(
        self,
        *,
        one: tuple[Any, ...] | None = None,
        all_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._one = one
        self._all_rows = all_rows or []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._all_rows


class _SeqConnection:
    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = cursors
        self.committed = False

    def execute(self, _sql: str, _params: tuple[Any, ...]) -> _FakeCursor:
        if not self._cursors:
            return _FakeCursor()
        return self._cursors.pop(0)

    def commit(self) -> None:
        self.committed = True


class _ConnectionContext:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _SeqConnection:
        return self._connection

    def __exit__(self, *_args: object) -> bool:
        return False


class _SeqPool:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


@dataclass
class _Runtime:
    db_pool: _SeqPool | None
    cursor_payload: dict[str, Any] | None = None
    visible: bool = True
    related: list[dict[str, Any]] = field(default_factory=list)
    touched: list[str] = field(default_factory=list)
    issued: list[dict[str, Any]] = field(default_factory=list)

    @property
    def max_jsonpath_length(self) -> int:
        return 1024

    @property
    def max_path_segments(self) -> int:
        return 64

    @property
    def max_wildcard_expansion_total(self) -> int:
        return 1000

    @property
    def related_query_max_artifacts(self) -> int:
        return 10

    @property
    def max_bytes_out(self) -> int:
        return 1_000_000

    @property
    def blobs_payload_dir(self) -> Path:
        return Path(".")

    def bounded_limit(self, _limit_value: Any) -> int:
        return 50

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        _ = token, tool, artifact_id
        if self.cursor_payload is None:
            raise ValueError("invalid cursor")
        return self.cursor_payload

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        position = payload.get("position_state", {"offset": 0})
        if not isinstance(position, dict):
            raise ValueError("cursor missing position_state")
        return position

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        self.issued.append(
            {
                "tool": tool,
                "artifact_id": artifact_id,
                "position_state": position_state,
                "extra": extra,
            }
        )
        return "cur_next"

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        return {"code": "CURSOR_STALE", "message": str(token_error)}

    def assert_cursor_field(
        self,
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        actual = payload.get(field)
        if actual != expected:
            raise ValueError(f"{field} mismatch")

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        _ = connection, session_id, artifact_id
        return self.visible

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        _ = connection, session_id
        self.touched.append(artifact_id)
        return True

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        _ = connection, session_id, anchor_artifact_id
        return self.related

    def compute_related_set_hash(
        self,
        artifacts: list[dict[str, Any]],
    ) -> str:
        _ = artifacts
        return "related_hash"

    def build_lineage_root_catalog(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        _ = entries
        return [{"root_path": "$.items", "compatible_for_select": True}]

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {"code": "NOT_IMPLEMENTED", "message": f"{tool_name} unavailable"}


def _artifact_row(artifact_id: str) -> tuple[Any, ...]:
    envelope = {
        "type": "mcp_envelope",
        "content": [{"type": "json", "value": {"id": 1}}],
    }
    return (
        artifact_id,
        "payload_hash",
        None,
        "full",
        "ready",
        1,
        0,
        "mbf",
        envelope,
        "none",
        "",
        False,
    )


def test_execute_artifact_get_returns_envelope_items_for_single_scope() -> None:
    conn = _SeqConnection([_FakeCursor(one=_artifact_row("art_1"))])
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_get(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
            "target": "envelope",
        },
    )

    assert result["target"] == "envelope"
    assert result["scope"] == "single"
    assert result["items"][0]["_locator"]["artifact_id"] == "art_1"
    assert result["items"][0]["value"]["type"] == "mcp_envelope"
    assert runtime.touched == ["art_1"]
    assert conn.committed is True


def test_execute_artifact_get_returns_cursor_stale_on_binding_mismatch() -> None:
    conn = _SeqConnection([_FakeCursor(one=_artifact_row("art_1"))])
    runtime = _Runtime(
        db_pool=_SeqPool(conn),
        cursor_payload={
            "position_state": {"offset": 0},
            "target": "mapped",
            "normalized_jsonpath": "$",
            "artifact_generation": 1,
        },
    )

    result = execute_artifact_get(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
            "target": "envelope",
            "cursor": "cur1.token",
        },
    )

    assert result["code"] == "CURSOR_STALE"
    assert "target mismatch" in result["message"]

