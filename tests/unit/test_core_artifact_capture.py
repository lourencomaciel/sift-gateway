from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from sift_gateway.core import artifact_capture


class _Cursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _Connection:
    def __init__(
        self, rows_by_sql: dict[str, tuple[object, ...] | None]
    ) -> None:
        self.rows_by_sql = rows_by_sql
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.committed = False

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> _Cursor:
        self.calls.append((sql, params))
        for needle, row in self.rows_by_sql.items():
            if needle in sql:
                return _Cursor(row)
        return _Cursor(None)

    def commit(self) -> None:
        self.committed = True


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection_obj = connection

    @contextmanager
    def connection(self):
        yield self.connection_obj


class _Runtime:
    def __init__(self, connection: _Connection) -> None:
        self.db_pool = _Pool(connection)
        self.config = object()
        self.touches: list[tuple[str, str]] = []
        self.mapping_calls: list[tuple[str, str]] = []

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        del connection
        self.touches.append((session_id, artifact_id))
        return True

    def run_mapping_inline(
        self,
        connection: Any,
        *,
        handle: Any,
        envelope: Any,
    ) -> bool:
        del connection, envelope
        self.mapping_calls.append((handle.artifact_id, handle.map_status))
        return True

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {"code": "NOT_IMPLEMENTED", "message": tool_name}


def test_execute_artifact_capture_persists_and_maps_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        rows_by_sql={
            "SELECT map_status": ("ready",),
        }
    )
    runtime = _Runtime(connection)

    monkeypatch.setattr(
        artifact_capture,
        "persist_artifact",
        lambda **kwargs: SimpleNamespace(
            artifact_id="art_new",
            created_seq=99,
            status="ok",
            kind="data",
            capture_kind="stdin_pipe",
            capture_key="rk_new",
            payload_json_bytes=21,
            payload_binary_bytes_total=0,
            payload_total_bytes=21,
            map_status="pending",
        ),
    )

    payload = artifact_capture.execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "cli"},
            "capture_kind": "stdin_pipe",
            "capture_origin": {"cwd": "/tmp", "stdin_hash": "abc"},
            "capture_key": "rk_new",
            "prefix": "cli",
            "tool_name": "stdin",
            "upstream_instance_id": "cli_local",
            "request_key": "rk_new",
            "request_args_hash": "args_new",
            "request_args_prefix": "{}",
            "payload": {"items": [1, 2, 3]},
            "status": "ok",
            "ttl_seconds": 3600,
        },
    )

    assert payload["artifact_id"] == "art_new"
    assert payload["reused"] is False
    assert isinstance(payload["expires_at"], str)
    assert runtime.mapping_calls == [("art_new", "pending")]


def test_execute_artifact_capture_rejects_invalid_capture_kind() -> None:
    runtime = _Runtime(_Connection(rows_by_sql={}))

    payload = artifact_capture.execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "cli"},
            "capture_kind": "bad_kind",
            "capture_origin": {"cwd": "/tmp"},
            "capture_key": "rk_x",
            "prefix": "cli",
            "tool_name": "run",
            "upstream_instance_id": "cli_local",
            "request_key": "rk_x",
            "request_args_hash": "args_x",
            "request_args_prefix": "{}",
            "payload": {"k": "v"},
        },
    )

    assert payload["code"] == "INVALID_ARGUMENT"


def test_execute_artifact_capture_rejects_chain_seq_without_parent() -> None:
    runtime = _Runtime(_Connection(rows_by_sql={}))

    payload = artifact_capture.execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "cli"},
            "capture_kind": "cli_command",
            "capture_origin": {"cwd": "/tmp"},
            "capture_key": "rk_x",
            "prefix": "cli",
            "tool_name": "run",
            "upstream_instance_id": "cli_local",
            "request_key": "rk_x",
            "request_args_hash": "args_x",
            "request_args_prefix": "{}",
            "payload": {"k": "v"},
            "chain_seq": 1,
        },
    )

    assert payload["code"] == "INVALID_ARGUMENT"
    assert "chain_seq requires parent_artifact_id" in payload["message"]


def test_execute_artifact_capture_passes_parent_chain_to_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        rows_by_sql={
            "SELECT map_status": ("ready",),
        }
    )
    runtime = _Runtime(connection)
    persisted: dict[str, Any] = {}

    def _fake_persist_artifact(**kwargs: Any) -> Any:
        persisted.update(kwargs)
        return SimpleNamespace(
            artifact_id="art_new",
            created_seq=99,
            status="ok",
            kind="data",
            capture_kind="cli_command",
            capture_key="rk_new",
            payload_json_bytes=21,
            payload_binary_bytes_total=0,
            payload_total_bytes=21,
            map_status="pending",
        )

    monkeypatch.setattr(
        artifact_capture,
        "persist_artifact",
        _fake_persist_artifact,
    )

    payload = artifact_capture.execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "cli"},
            "capture_kind": "cli_command",
            "capture_origin": {"cwd": "/tmp"},
            "capture_key": "rk_new",
            "prefix": "cli",
            "tool_name": "run",
            "upstream_instance_id": "cli_local",
            "request_key": "rk_new",
            "request_args_hash": "args_new",
            "request_args_prefix": "{}",
            "payload": {"items": [1]},
            "parent_artifact_id": "art_parent",
            "chain_seq": 1,
        },
    )

    assert payload["artifact_id"] == "art_new"
    input_data = persisted["input_data"]
    assert input_data.parent_artifact_id == "art_parent"
    assert input_data.chain_seq == 1
