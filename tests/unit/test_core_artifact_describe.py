from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sift_gateway.core.artifact_describe import execute_artifact_describe


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
    visible: bool = True
    related: list[dict[str, Any]] = field(default_factory=list)
    related_query_max_artifacts: int = 10
    touched: list[str] = field(default_factory=list)

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
        raise NotImplementedError

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
        raise NotImplementedError

    def cursor_error(self, token_error: Exception) -> dict[str, Any]:
        return {"code": "CURSOR_STALE", "message": str(token_error)}

    def assert_cursor_field(
        self,
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        _ = payload, field, expected
        raise NotImplementedError

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
        return [
            {
                "root_path": entries[0]["root_path"] if entries else "$.items",
                "compatible_for_select": True,
                "signature_groups": [
                    {
                        "schema_mode": entries[0].get("schema_mode")
                        if entries
                        else "sampled"
                    }
                ],
                "schema": entries[0].get("schema") if entries else None,
            }
        ]

    def not_implemented(self, tool_name: str) -> dict[str, Any]:
        return {
            "code": "NOT_IMPLEMENTED",
            "message": f"{tool_name} unavailable",
        }


def test_execute_artifact_describe_single_scope_returns_compact_roots() -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(
                one=(
                    "art_1",
                    "partial",
                    "ready",
                    "mapper_v1",
                    "mbf",
                    "backend",
                    "prng_xoshiro256ss_v1",
                    0,
                    None,
                    1,
                )
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        100,
                        1.0,
                        {},
                        10.0,
                        "array",
                        {"id": {"number": 10}},
                        [0, 1],
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        "schema_v1",
                        "sha256:schema_items",
                        "sampled",
                        "partial",
                        2,
                        "sha256:dataset_items",
                        "traversal_v1",
                        "mbf",
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "$.id",
                        ["number"],
                        False,
                        True,
                        2,
                        "1",
                        [1, 2],
                        2,
                    )
                ]
            ),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
        },
    )

    assert result["artifact_id"] == "art_1"
    assert result["scope"] == "single"
    assert result["roots"][0]["root_path"] == "$.items"
    assert result["roots"][0]["compatible_for_select"] is True
    assert "schema_legend" in result
    assert "schemas" in result
    assert runtime.touched == ["art_1"]
    assert conn.committed is True


def test_execute_artifact_describe_all_related_respects_max_artifacts() -> None:
    conn = _SeqConnection([])
    runtime = _Runtime(
        db_pool=_SeqPool(conn),
        related_query_max_artifacts=1,
        related=[
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )

    result = execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "all_related",
        },
    )

    assert result["code"] == "RESOURCE_EXHAUSTED"


def test_execute_artifact_describe_surfaces_upstream_pagination_meta() -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(
                one=(
                    "art_1",
                    "partial",
                    "ready",
                    "mapper_v1",
                    "mbf",
                    "backend",
                    "prng_xoshiro256ss_v1",
                    0,
                    None,
                    1,
                )
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        10,
                        1.0,
                        {},
                        10.0,
                        "array",
                        {"id": {"number": 10}},
                        [0, 1],
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        "schema_v1",
                        "sha256:schema_items",
                        "sampled",
                        "partial",
                        2,
                        "sha256:dataset_items",
                        "traversal_v1",
                        "mbf",
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "$.id",
                        ["number"],
                        False,
                        True,
                        2,
                        "1",
                        [1, 2],
                        2,
                    )
                ]
            ),
            _FakeCursor(
                one=(
                    "art_1",
                    "hash_1",
                    {
                        "meta": {
                            "_gateway_pagination": {
                                "upstream_prefix": "cli",
                                "tool_name": "run",
                                "original_args": {
                                    "command_argv": ["fake-api", "--after", "C1"]
                                },
                                "next_params": {"after": "C2"},
                                "page_number": 0,
                            }
                        }
                    },
                    "none",
                    "payload/path",
                )
            ),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
        },
    )

    assert result["pagination"]["layer"] == "upstream"
    assert result["pagination"]["has_next_page"] is False
    assert result["pagination"]["retrieval_status"] == "PARTIAL"
    assert result["pagination"]["next_action"] is None
    assert result["pagination"]["next_params"] == {"after": "C2"}


def test_execute_artifact_describe_surfaces_next_page_for_mirrored_state() -> None:
    conn = _SeqConnection(
        [
            _FakeCursor(
                one=(
                    "art_1",
                    "partial",
                    "ready",
                    "mapper_v1",
                    "mbf",
                    "backend",
                    "prng_xoshiro256ss_v1",
                    0,
                    None,
                    1,
                )
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        10,
                        1.0,
                        {},
                        10.0,
                        "array",
                        {"id": {"number": 10}},
                        [0, 1],
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        "schema_v1",
                        "sha256:schema_items",
                        "sampled",
                        "partial",
                        2,
                        "sha256:dataset_items",
                        "traversal_v1",
                        "mbf",
                    )
                ]
            ),
            _FakeCursor(
                all_rows=[
                    (
                        "$.id",
                        ["number"],
                        False,
                        True,
                        2,
                        "1",
                        [1, 2],
                        2,
                    )
                ]
            ),
            _FakeCursor(
                one=(
                    "art_1",
                    "hash_1",
                    {
                        "meta": {
                            "_gateway_pagination": {
                                "upstream_prefix": "github",
                                "tool_name": "list_prs",
                                "original_args": {"after": "C1"},
                                "next_params": {"after": "C2"},
                                "page_number": 0,
                            }
                        }
                    },
                    "none",
                    "payload/path",
                )
            ),
        ]
    )
    runtime = _Runtime(db_pool=_SeqPool(conn))

    result = execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": {"session_id": "sess_1"},
            "artifact_id": "art_1",
            "scope": "single",
        },
    )

    assert result["pagination"]["layer"] == "upstream"
    assert result["pagination"]["has_next_page"] is True
    assert result["pagination"]["retrieval_status"] == "PARTIAL"
    assert result["pagination"]["next_action"] == {
        "tool": "artifact",
        "arguments": {"action": "next_page", "artifact_id": "art_1"},
    }
