from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sift_mcp.core.artifact_search import execute_artifact_search


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.committed = False
        self.executed_sql: str | None = None
        self.executed_params: tuple[Any, ...] | None = None

    def execute(self, sql: str, params: tuple[Any, ...]) -> _FakeCursor:
        self.executed_sql = sql
        self.executed_params = params
        return _FakeCursor(self.rows)

    def commit(self) -> None:
        self.committed = True


class _ConnectionContext:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeConnection:
        return self._connection

    def __exit__(self, *_args: object) -> bool:
        return False


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


@dataclass
class _Runtime:
    db_pool: _FakePool | None
    artifact_search_max_limit: int = 2
    cursor_verify_error: Exception | None = None
    cursor_positions: dict[str, Any] = field(default_factory=lambda: {"offset": 0})
    cursor_errors: list[str] = field(default_factory=list)
    issued_cursors: list[dict[str, Any]] = field(default_factory=list)
    touches: list[tuple[str, list[str]]] = field(default_factory=list)

    def cursor_session_artifact_id(self, session_id: str, order_by: str) -> str:
        return f"session:{session_id}:{order_by}"

    def verify_cursor(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        _ = token, tool, artifact_id
        if self.cursor_verify_error is not None:
            raise self.cursor_verify_error
        return self.cursor_positions

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
    ) -> str:
        self.issued_cursors.append(
            {
                "tool": tool,
                "artifact_id": artifact_id,
                "position_state": position_state,
            }
        )
        return "cur_next"

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        self.cursor_errors.append(str(token_error))
        return {"code": "INVALID_ARGUMENT", "message": "invalid cursor"}

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        _ = connection
        self.touches.append((session_id, artifact_ids))
        return True

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {
            "code": "NOT_IMPLEMENTED",
            "message": f"{tool_name} unavailable",
        }


def _row(artifact_id: str, created_seq: int) -> tuple[Any, ...]:
    return (
        artifact_id,
        created_seq,
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
        "demo.echo",
        "inst_demo",
        "mcp_tool",
        "rk_demo",
        "ok",
        123,
        None,
        "none",
        "pending",
        None,
        "data",
    )


def test_execute_artifact_search_returns_items_and_cursor() -> None:
    conn = _FakeConnection(rows=[_row("art_1", 1), _row("art_2", 2), _row("art_3", 3)])
    runtime = _Runtime(db_pool=_FakePool(conn), artifact_search_max_limit=2)

    result = execute_artifact_search(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "filters": {},
            "limit": 2,
        },
    )

    assert result["truncated"] is True
    assert result["cursor"] == "cur_next"
    assert result["items"][0]["artifact_id"] == "art_1"
    assert result["items"][1]["artifact_id"] == "art_2"
    assert result["items"][0]["capture_kind"] == "mcp_tool"
    assert result["items"][0]["capture_key"] == "rk_demo"
    assert runtime.touches == [("sess_1", ["art_1", "art_2"])]
    assert runtime.issued_cursors == [
        {
            "tool": "artifact",
            "artifact_id": "session:sess_1:created_seq_desc",
            "position_state": {"offset": 2},
        }
    ]
    assert conn.committed is True


def test_execute_artifact_search_maps_cursor_errors() -> None:
    conn = _FakeConnection(rows=[])
    runtime = _Runtime(
        db_pool=_FakePool(conn),
        cursor_verify_error=ValueError("bad cursor"),
    )

    result = execute_artifact_search(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "filters": {},
            "cursor": "cur1.bad",
        },
    )

    assert result == {"code": "INVALID_ARGUMENT", "message": "invalid cursor"}
    assert runtime.cursor_errors == ["bad cursor"]


def test_execute_artifact_search_returns_not_implemented_without_db() -> None:
    runtime = _Runtime(db_pool=None)

    result = execute_artifact_search(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "filters": {},
        },
    )

    assert result == {
        "code": "NOT_IMPLEMENTED",
        "message": "artifact unavailable",
    }
