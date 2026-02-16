"""Tests for artifact query_kind=code handler."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from sift_mcp.codegen.runtime import CodeRuntimeError, CodeRuntimeMemoryLimit
from sift_mcp.config.settings import GatewayConfig
from sift_mcp.mcp.server import GatewayServer


class _SeqCursor:
    def __init__(
        self,
        *,
        one: tuple[object, ...] | None = None,
        all_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self._one = one
        self._all_rows = list(all_rows or [])

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._all_rows)


class _SeqConnection:
    def __init__(self, cursors: list[_SeqCursor]) -> None:
        self._cursors = list(cursors)

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _SeqCursor:
        if not self._cursors:
            return _SeqCursor()
        return self._cursors.pop(0)

    def commit(self) -> None:
        return None


class _SeqConnectionContext:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _SeqConnection:
        return self._connection

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _SeqPool:
    def __init__(self, connection: _SeqConnection) -> None:
        self._connection = connection

    def connection(self) -> _SeqConnectionContext:
        return _SeqConnectionContext(self._connection)


def _server(
    tmp_path: Path,
    connection: _SeqConnection,
    *,
    code_query_max_input_records: int = 100_000,
    code_query_max_input_bytes: int = 50_000_000,
    code_query_allowed_import_roots: list[str] | None = None,
    max_bytes_out: int = 5_000_000,
) -> GatewayServer:
    config = GatewayConfig(
        data_dir=tmp_path,
        code_query_max_input_records=code_query_max_input_records,
        code_query_max_input_bytes=code_query_max_input_bytes,
        code_query_allowed_import_roots=code_query_allowed_import_roots,
        max_bytes_out=max_bytes_out,
    )
    server = GatewayServer(
        config=config,
        db_pool=_SeqPool(connection),  # type: ignore[arg-type]
    )
    server._artifact_visible = lambda *_args, **_kwargs: True
    return server


def _meta_row(
    artifact_id: str,
    *,
    map_kind: str = "full",
    generation: int = 1,
) -> tuple[object, ...]:
    return (
        artifact_id,
        map_kind,
        "ready",
        "off",
        None,
        generation,
        "mbf",
    )


def _root_row(root_key: str, root_path: str = "$.items") -> tuple[object, ...]:
    return (
        root_key,
        root_path,
        1,
        "array",
        {"id": {"number": 1}},
        None,
        {},
    )


def _schema_root_row(
    root_key: str,
    root_path: str = "$.items",
    *,
    schema_hash: str = "sha256:schema_items",
) -> tuple[object, ...]:
    return (
        root_key,
        root_path,
        "schema_v1",
        schema_hash,
        "exact",
        "complete",
        1,
        "sha256:dataset",
        "traversal_v1",
        None,
    )


def _schema_field_row(path: str = "$.id") -> tuple[object, ...]:
    return (
        path,
        ["number"],
        False,
        True,
        1,
        "1",
        None,
        None,
    )


def _artifact_row(artifact_id: str, envelope: dict[str, Any]) -> tuple[object, ...]:
    return (
        artifact_id,
        f"hash_{artifact_id}",
        None,
        "full",
        "ready",
        1,
        0,
        "mbf",
        envelope,
        "none",
        b"",
        0,
        False,
    )


def test_code_query_all_related_merges_records(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(one=_meta_row("art_2")),
            _SeqCursor(one=_root_row("rk_2")),
            _SeqCursor(one=_schema_root_row("rk_2")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
            _SeqCursor(
                one=_artifact_row(
                    "art_2",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 2}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: [
            {
                "id": item["id"],
                "artifact": item["_locator"]["artifact_id"],
            }
            for item in kwargs["data"]
        ],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )

    assert response["total_matched"] == 2
    assert response["lineage"]["artifact_count"] == 2
    assert response["scope"] == "all_related"
    assert response["truncated"] is False
    assert "cursor" not in response
    assert {item["artifact"] for item in response["items"]} == {
        "art_1",
        "art_2",
    }


def test_code_query_normalizes_scalar_return_to_list(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.compute_related_set_hash",
        lambda _rows: "related_hash_1",
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: {"ok": True},
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return {'ok': True}",
            }
        )
    )

    assert response["items"] == [{"ok": True}]
    assert response["total_matched"] == 1


def test_code_query_scope_argument_is_ignored(tmp_path: Path) -> None:
    server = GatewayServer(config=GatewayConfig(data_dir=tmp_path))
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return []",
            }
        )
    )
    assert response["code"] == "NOT_IMPLEMENTED"


def test_code_query_accepts_float_params(tmp_path: Path, monkeypatch) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: [],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return []",
                "params": {"min_spend": 10.5},
            }
        )
    )

    assert response["items"] == []
    assert response["determinism"]["params_hash"].startswith("sha256:")


def test_code_query_ignores_cursor_parameter(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
                "cursor": "cur_1",
            }
        )
    )
    assert response["total_matched"] == 1
    assert response["items"][0]["id"] == 1


def test_code_query_partial_mapping_marks_sampled_only(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1", map_kind="partial")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                all_rows=[
                    (0, {"id": 10}, 10, "h1"),
                    (1, {"id": 20}, 10, "h2"),
                ]
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **kwargs: kwargs["data"],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )

    assert response["sampled_only"] is True
    assert any(
        warning.get("code") == "SAMPLED_MAPPING_USED"
        for warning in response.get("warnings", [])
    )


def test_code_query_rejects_when_input_records_exceed_limit(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}, {"id": 2}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn, code_query_max_input_records=1)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    runtime_called = False

    def _fake_exec(**_kwargs: Any) -> list[dict[str, Any]]:
        nonlocal runtime_called
        runtime_called = True
        return []

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )

    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "CODE_INPUT_TOO_LARGE"
    assert runtime_called is False


def test_code_query_rejects_when_input_bytes_exceed_limit(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {
                                    "items": [
                                        {"id": 1, "blob": "x" * 300}
                                    ]
                                },
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn, code_query_max_input_bytes=120)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    runtime_called = False

    def _fake_exec(**_kwargs: Any) -> list[dict[str, Any]]:
        nonlocal runtime_called
        runtime_called = True
        return []

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )

    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "CODE_INPUT_TOO_LARGE"
    assert "input_bytes" in response["details"]
    assert runtime_called is False


def test_code_query_maps_memory_limit_error(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: (_ for _ in ()).throw(
            CodeRuntimeMemoryLimit(
                code="CODE_RUNTIME_MEMORY_LIMIT",
                message="memory limit reached",
            )
        ),
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "CODE_RUNTIME_MEMORY_LIMIT"


def test_code_query_passes_import_allowlist_from_configured_roots(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(
        tmp_path,
        conn,
        code_query_allowed_import_roots=["jmespath", "json", "math"],
    )

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    captured: dict[str, Any] = {}

    def _fake_exec(**kwargs: Any) -> list[dict[str, int]]:
        captured.update(kwargs)
        return [{"ok": 1}]

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return [{'ok': 1}]",
            }
        )
    )
    assert response["items"] == [{"ok": 1}]
    assert captured["allowed_import_roots"] == ["jmespath", "json", "math"]


def test_code_query_passes_custom_import_allowlist(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(
        tmp_path,
        conn,
        code_query_allowed_import_roots=["math", "json"],
    )

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    captured: dict[str, Any] = {}

    def _fake_exec(**kwargs: Any) -> list[dict[str, int]]:
        captured.update(kwargs)
        return [{"ok": 1}]

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return [{'ok': 1}]",
            }
        )
    )
    assert response["items"] == [{"ok": 1}]
    assert captured["allowed_import_roots"] == ["json", "math"]


def test_code_query_rejects_output_above_transport_budget(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn, max_bytes_out=30)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: [{"value": "x" * 200}],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )
    assert response["code"] == "RESPONSE_TOO_LARGE"
    assert "max_bytes_out" in response["message"]


def test_code_query_runtime_error_includes_traceback_details(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: (_ for _ in ()).throw(
            CodeRuntimeError(
                code="CODE_RUNTIME_EXCEPTION",
                message="boom",
                traceback="Traceback (most recent call last): ...",
            )
        ),
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "CODE_RUNTIME_EXCEPTION"
    assert "traceback" in response["details"]


def test_code_query_multi_artifact_passes_artifacts_payload(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(one=_schema_root_row("rk_1")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
            _SeqCursor(one=_meta_row("art_2")),
            _SeqCursor(one=_root_row("rk_2")),
            _SeqCursor(one=_schema_root_row("rk_2")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_2",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 2}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **kwargs: [
            {"artifact_id": kwargs["anchor_artifact_id"], "generation": 1}
        ],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    captured: dict[str, Any] = {}

    def _fake_exec(**kwargs: Any) -> list[dict[str, int]]:
        captured.update(kwargs)
        return [{"records": 2}]

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_ids": ["art_1", "art_2"],
                "root_path": "$.items",
                "code": (
                    "def run(artifacts, schemas, params): "
                    "return [{'records': len(artifacts)}]"
                ),
            }
        )
    )

    assert response["items"] == [{"records": 2}]
    assert "artifacts" in captured
    assert "schemas" in captured
    assert "data" not in captured
    assert sorted(captured["artifacts"].keys()) == ["art_1", "art_2"]


def test_code_query_multi_artifact_supports_per_artifact_root_paths(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1", "$.items_a")),
            _SeqCursor(one=_schema_root_row("rk_1", "$.items_a")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_1",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items_a": [{"id": 1}]},
                            }
                        ]
                    },
                )
            ),
            _SeqCursor(one=_meta_row("art_2")),
            _SeqCursor(one=_root_row("rk_2", "$.items_b")),
            _SeqCursor(one=_schema_root_row("rk_2", "$.items_b")),
            _SeqCursor(all_rows=[_schema_field_row()]),
            _SeqCursor(
                one=_artifact_row(
                    "art_2",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items_b": [{"id": 2}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.resolve_related_artifacts",
        lambda *_args, **kwargs: [
            {"artifact_id": kwargs["anchor_artifact_id"], "generation": 1}
        ],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    captured: dict[str, Any] = {}

    def _fake_exec(**kwargs: Any) -> list[dict[str, int]]:
        captured.update(kwargs)
        return [{"records": 2}]

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_code.execute_code_in_subprocess",
        _fake_exec,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_ids": ["art_1", "art_2"],
                "root_paths": {
                    "art_1": "$.items_a",
                    "art_2": "$.items_b",
                },
                "code": (
                    "def run(artifacts, schemas, params): "
                    "return [{'records': len(artifacts)}]"
                ),
            }
        )
    )

    assert response["items"] == [{"records": 2}]
    assert "root_paths" in response["lineage"]
    assert response["lineage"]["root_paths"] == {
        "art_1": "$.items_a",
        "art_2": "$.items_b",
    }
    assert captured["artifacts"]["art_1"][0]["_locator"]["root_path"] == "$.items_a"
    assert captured["artifacts"]["art_2"][0]["_locator"]["root_path"] == "$.items_b"


def test_code_query_multi_artifact_requires_root_path_or_root_paths(
    tmp_path: Path,
) -> None:
    server = GatewayServer(config=GatewayConfig(data_dir=tmp_path))
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_ids": ["art_1", "art_2"],
                "code": "def run(artifacts, schemas, params): return []",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "missing root_path or root_paths" in response["message"]
