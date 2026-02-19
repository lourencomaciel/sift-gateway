from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from sift_mcp.core.artifact_next_page import execute_artifact_next_page
from sift_mcp.pagination.extract import PaginationState


class _FakeCursor:
    def __init__(self, one: tuple[Any, ...] | None = None) -> None:
        self._one = one

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._one


class _SeqConnection:
    def __init__(self, rows: list[tuple[Any, ...] | None]) -> None:
        self._rows = rows

    def execute(self, _sql: str, _params: tuple[Any, ...]) -> _FakeCursor:
        if not self._rows:
            return _FakeCursor(None)
        return _FakeCursor(self._rows.pop(0))


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
    mirrored_lookup: dict[str, dict[str, Any]]
    called_args: dict[str, Any] | None = None

    @property
    def blobs_payload_dir(self) -> Path:
        return Path(".")

    def artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        _ = connection, session_id, artifact_id
        return True

    def get_mirrored_tool(self, qualified_name: str) -> Any | None:
        return self.mirrored_lookup.get(qualified_name)

    async def call_mirrored_tool(
        self,
        mirrored: Any,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        self.called_args = {"mirrored": mirrored, "arguments": arguments}
        return {"type": "gateway_tool_result", "artifact_id": "art_next"}

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {"code": "NOT_IMPLEMENTED", "message": f"{tool_name} unavailable"}


def _row_with_pagination() -> tuple[Any, ...]:
    state = PaginationState(
        upstream_prefix="demo",
        tool_name="echo",
        original_args={"message": "hello"},
        next_params={"after": "CURSOR_2"},
        page_number=0,
    )
    envelope = {"meta": {"_gateway_pagination": state.to_dict()}}
    return ("art_1", None, "hash_1", envelope, "none", "")


@pytest.mark.asyncio
async def test_execute_artifact_next_page_replays_next_upstream_call() -> None:
    conn = _SeqConnection([_row_with_pagination()])
    runtime = _Runtime(
        db_pool=_SeqPool(conn),
        mirrored_lookup={"demo.echo": {"qualified_name": "demo.echo"}},
    )

    result = await execute_artifact_next_page(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        },
    )

    assert result["artifact_id"] == "art_next"
    assert runtime.called_args is not None
    assert runtime.called_args["arguments"] == {
        "message": "hello",
        "after": "CURSOR_2",
        "_gateway_context": {"session_id": "sess_1"},
        "_gateway_parent_artifact_id": "art_1",
        "_gateway_chain_seq": 1,
    }


@pytest.mark.asyncio
async def test_execute_artifact_next_page_rejects_missing_pagination_state() -> None:
    row = ("art_1", None, "hash_1", {"meta": {"warnings": []}}, "none", "")
    conn = _SeqConnection([row])
    runtime = _Runtime(
        db_pool=_SeqPool(conn),
        mirrored_lookup={"demo.echo": {"qualified_name": "demo.echo"}},
    )

    result = await execute_artifact_next_page(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
        },
    )

    assert result["code"] == "INVALID_ARGUMENT"
    assert "no upstream pagination state" in result["message"]

