from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from sift_gateway.core.artifact_code import execute_artifact_code


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

    def execute(
        self, _sql: str, _params: tuple[Any, ...] | list[Any]
    ) -> _FakeCursor:
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
    code_query_max_input_records: int = 100_000
    code_query_max_input_bytes: int = 50_000_000
    persisted_calls: list[dict[str, Any]] = field(default_factory=list)

    @property
    def code_query_enabled(self) -> bool:
        return True

    @property
    def code_query_timeout_seconds(self) -> float:
        return 10.0

    @property
    def code_query_max_memory_mb(self) -> int:
        return 256

    @property
    def code_query_allowed_import_roots(self) -> list[str] | None:
        return ["json", "math"]

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

    def bounded_limit(self, limit_value: Any) -> int:
        del limit_value
        return 50

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        del token, tool, artifact_id
        raise ValueError("invalid cursor")

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        del payload
        raise NotImplementedError

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        del tool, artifact_id, position_state, extra
        return "cur_next"

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        return {"code": "CURSOR_STALE", "message": str(token_error)}

    def assert_cursor_field(
        self,
        payload: dict[str, Any],
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
        del connection, session_id, artifact_id
        return True

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        del connection, session_id, artifact_id
        return True

    def safe_touch_for_retrieval_many(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        del connection, session_id, artifact_ids
        return True

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        del connection, session_id, artifact_ids
        return True

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        del connection, session_id
        return [{"artifact_id": anchor_artifact_id, "generation": 1}]

    def compute_related_set_hash(
        self,
        artifacts: list[dict[str, Any]],
    ) -> str:
        del artifacts
        return "related_hash_1"

    def build_lineage_root_catalog(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        del entries
        return []

    def check_sample_corruption(
        self,
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        del root_row, sample_rows
        return None

    def increment_metric(self, attr: str, amount: int = 1) -> None:
        del attr, amount

    def observe_metric(self, attr: str, value: float) -> None:
        del attr, value

    def persist_code_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        requested_root_paths: dict[str, str],
        root_path: str,
        code_hash: str,
        params_hash: str,
        result_items: list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        self.persisted_calls.append(
            {
                "parent_artifact_ids": parent_artifact_ids,
                "requested_root_paths": requested_root_paths,
                "root_path": root_path,
                "code_hash": code_hash,
                "params_hash": params_hash,
                "result_items": result_items,
            }
        )
        return "art_derived", None

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {
            "code": "NOT_IMPLEMENTED",
            "message": f"{tool_name} unavailable",
        }


def _meta_row() -> tuple[Any, ...]:
    return ("art_1", "full", "ready", "off", None, 1, "mbf")


def _root_row() -> tuple[Any, ...]:
    return ("rk_1", "$.items", 1, "array", {"id": {"number": 1}}, None, {})


def _schema_root_row() -> tuple[Any, ...]:
    return (
        "rk_1",
        "$.items",
        "schema_v1",
        "sha256:schema_items",
        "exact",
        "complete",
        1,
        "sha256:dataset_items",
        "traversal_v1",
        "mbf",
    )


def _schema_field_row() -> tuple[Any, ...]:
    return (
        "$.id",
        ["number"],
        False,
        True,
        1,
        "1",
        None,
        None,
    )


def _artifact_row(items: list[dict[str, Any]]) -> tuple[Any, ...]:
    return (
        "art_1",
        "hash_art_1",
        None,
        "full",
        "ready",
        1,
        0,
        "mbf",
        {"content": [{"type": "json", "value": {"items": items}}]},
        "none",
        None,
        False,
    )


def test_execute_artifact_code_returns_items_and_persists_derived(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(all_rows=[_schema_field_row()]),
            _FakeCursor(one=_artifact_row([{"id": 1}])),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": "def run(data, schema, params): return data",
        },
    )

    assert result["total_matched"] == 1
    assert result["items"][0]["id"] == 1
    assert result["artifact_id"] == "art_derived"
    assert runtime.persisted_calls
    assert conn.committed is True


def test_execute_artifact_code_continues_when_describe_lookup_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(all_rows=[_schema_field_row()]),
            _FakeCursor(one=_artifact_row([{"id": 1}])),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_artifact_describe",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("describe lookup failed")
        ),
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": "def run(data, schema, params): return data",
        },
    )

    assert result["artifact_id"] == "art_derived"
    assert result["total_matched"] == 1
    assert result["response_mode"] == "full"
    assert runtime.persisted_calls


def test_execute_artifact_code_returns_error_on_missing_derived_artifact_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(all_rows=[_schema_field_row()]),
            _FakeCursor(one=_artifact_row([{"id": 1}])),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )
    monkeypatch.setattr(
        runtime,
        "persist_code_derived",
        lambda **_kwargs: (None, None),
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": "def run(data, schema, params): return data",
        },
    )

    assert result["code"] == "DERIVED_PERSISTENCE_FAILED"
    assert "invalid artifact_id" in result["message"]
    assert result["details"]["stage"] == "persist_code_derived"
    assert result["details"]["artifact_id"] is None


def test_execute_artifact_code_rejects_input_records_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(all_rows=[_schema_field_row()]),
            _FakeCursor(one=_artifact_row([{"id": 1}, {"id": 2}])),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn), code_query_max_input_records=1)

    called = False

    def _fake_exec(**kwargs: Any) -> list[dict[str, Any]]:
        nonlocal called
        called = True
        del kwargs
        return []

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    result = execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "code": "def run(data, schema, params): return data",
        },
    )

    assert result["code"] == "INVALID_ARGUMENT"
    assert result["details"]["code"] == "CODE_INPUT_TOO_LARGE"
    assert called is False
