from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sift_gateway.core.artifact_select import execute_artifact_select


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
    touched: list[str] = field(default_factory=list)
    persisted_calls: list[dict[str, Any]] = field(default_factory=list)

    @property
    def artifact_search_max_limit(self) -> int:
        return 50

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

    @property
    def select_missing_as_null(self) -> bool:
        return False

    def bounded_limit(self, _limit_value: Any) -> int:
        return 50

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
        raise NotImplementedError

    def verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        _ = token, tool, artifact_id
        raise ValueError("invalid cursor")

    def cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        _ = payload
        raise NotImplementedError

    def issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        _ = tool, artifact_id, position_state, extra
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
        return True

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

    def safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> bool:
        _ = connection, session_id, artifact_ids
        return True

    def resolve_related_artifacts(
        self,
        connection: Any,
        *,
        session_id: str,
        anchor_artifact_id: str,
    ) -> list[dict[str, Any]]:
        _ = connection, session_id, anchor_artifact_id
        return []

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
        return []

    def persist_select_derived(
        self,
        *,
        parent_artifact_ids: list[str],
        arguments: dict[str, Any],
        result_data: dict[str, Any] | list[Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        self.persisted_calls.append(
            {
                "parent_artifact_ids": parent_artifact_ids,
                "arguments": arguments,
                "result_data": result_data,
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


def test_execute_artifact_select_returns_items_and_persists_derived() -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(all_rows=[(0, {"$.id": 1})]),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_select(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
            "root_path": "$.items",
            "select_paths": ["id"],
        },
    )

    assert result["total_matched"] == 1
    assert result["items"][0]["projection"] == {"$.id": 1}
    assert result["derived_artifact_id"] == "art_derived"
    assert runtime.touched == ["art_1"]
    assert conn.committed is True
    assert len(runtime.persisted_calls) == 1


def test_execute_artifact_select_count_only_persists_count_derived() -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(one=_meta_row()),
            _FakeCursor(one=_root_row()),
            _FakeCursor(one=_schema_root_row()),
            _FakeCursor(one=(3,)),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_select(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
            "root_path": "$.items",
            "select_paths": ["id"],
            "count_only": True,
        },
    )

    assert result["count"] == 3
    assert result["derived_artifact_id"] == "art_derived"
    assert runtime.persisted_calls[0]["result_data"] == {"count": 3}
