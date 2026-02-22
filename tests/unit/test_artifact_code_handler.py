"""Tests for artifact query_kind=code handler."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from sift_gateway.codegen.runtime import (
    CodeRuntimeError,
    CodeRuntimeMemoryLimitError,
)
from sift_gateway.config.settings import GatewayConfig
from sift_gateway.core.artifact_code import (
    _enrich_entrypoint_hint,
    _enrich_install_hint,
    _module_to_dist,
)
from sift_gateway.mcp.server import GatewayServer


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


def _artifact_row(
    artifact_id: str, envelope: dict[str, Any]
) -> tuple[object, ...]:
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
        None,
        False,
    )


@pytest.fixture(autouse=True)
def _mock_derived_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.persist_code_derived",
        lambda self, **_kwargs: ("art_derived", None),
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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


def test_code_query_all_related_rejects_incompatible_schema_signatures(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=_meta_row("art_1")),
            _SeqCursor(one=_root_row("rk_1")),
            _SeqCursor(
                one=_schema_root_row(
                    "rk_1", schema_hash="sha256:schema_items_a"
                )
            ),
            _SeqCursor(one=_meta_row("art_2")),
            _SeqCursor(one=_root_row("rk_2")),
            _SeqCursor(
                one=_schema_root_row(
                    "rk_2", schema_hash="sha256:schema_items_b"
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)

    monkeypatch.setattr(
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
    runtime_called = False

    def _fake_exec(**_kwargs: Any) -> list[dict[str, Any]]:
        nonlocal runtime_called
        runtime_called = True
        return []

    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
    assert response["message"] == "incompatible lineage schema for root_path"
    assert response["details"]["code"] == "INCOMPATIBLE_LINEAGE_SCHEMA"
    assert response["details"]["root_path"] == "$.items"
    assert len(response["details"]["signature_groups"]) == 2
    assert runtime_called is False


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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.compute_related_set_hash",
        lambda self, _rows: "related_hash_1",
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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


def test_code_query_accepts_scope_argument(tmp_path: Path) -> None:
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


def test_code_query_single_scope_uses_only_anchor_artifact(
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
                "scope": "single",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return data",
            }
        )
    )

    assert response["total_matched"] == 1
    assert response["scope"] == "single"
    assert response["lineage"]["scope"] == "single"
    assert response["lineage"]["artifact_count"] == 1
    assert {item["artifact"] for item in response["items"]} == {"art_1"}


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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
                                    "items": [{"id": 1, "blob": "x" * 300}]
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: (_ for _ in ()).throw(
            CodeRuntimeMemoryLimitError(
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: [{"value": "x" * 200}],
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_artifact_describe",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("schema fallback should not run")
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
    assert response["response_mode"] == "schema_ref"
    assert response["artifact_id"].startswith("art_")
    assert "schemas" not in response
    assert response["sample_item_source_index"] == 0
    assert response["sample_item_count"] == 1
    assert response["sample_item_text_truncated"] is True
    sample_item = response["sample_item"]
    assert isinstance(sample_item, dict)
    sample_value = sample_item["value"]
    assert isinstance(sample_value, str)
    assert sample_value.endswith("more chars truncated)")


def test_code_query_schema_ref_preserves_describe_schema(
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: [{"value": "x" * 200}, {"value": 2}],
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_artifact_describe",
        lambda *_args, **_kwargs: {
            "schemas": [
                {
                    "version": "schema_v1",
                    "schema_hash": "sha256:derived",
                    "root_path": "$",
                    "mode": "exact",
                    "coverage": {
                        "completeness": "complete",
                        "observed_records": 1,
                    },
                    "fields": [
                        {
                            "path": "$.id",
                            "types": ["number"],
                            "nullable": False,
                            "required": True,
                        }
                    ],
                    "determinism": {
                        "dataset_hash": "sha256:dataset",
                        "traversal_contract_version": "traversal_v1",
                        "map_budget_fingerprint": None,
                    },
                }
            ]
        },
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
    assert response["response_mode"] == "schema_ref"
    schema = response["schemas"][0]
    assert schema["root_path"] == "$"
    assert schema["schema_hash"] == "sha256:derived"
    assert schema["coverage"]["observed_records"] == 1


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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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


def test_code_query_entrypoint_missing_adds_single_entrypoint_hint(
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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
        lambda *_args, **_kwargs: [{"artifact_id": "art_1", "generation": 1}],
    )
    monkeypatch.setattr(
        server,
        "_safe_touch_for_retrieval_many",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
        lambda **_kwargs: (_ for _ in ()).throw(
            CodeRuntimeError(
                code="CODE_ENTRYPOINT_MISSING",
                message="run(...) entrypoint not found",
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
                "code": "def not_run(data, schema, params): return data",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "CODE_ENTRYPOINT_MISSING"
    assert "def run(data, schema, params):" in response["message"]


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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
    assert any(
        warning.get("code") == "OVERLAPPING_INPUT_DATASETS"
        for warning in response.get("warnings", [])
    )


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
        "sift_gateway.mcp.adapters.artifact_query_runtime.GatewayArtifactQueryRuntime.resolve_related_artifacts",
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
        "sift_gateway.core.artifact_code.execute_code_in_subprocess",
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
    assert (
        captured["artifacts"]["art_1"][0]["_locator"]["root_path"]
        == "$.items_a"
    )
    assert (
        captured["artifacts"]["art_2"][0]["_locator"]["root_path"]
        == "$.items_b"
    )


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
    assert response["details"]["code"] == "ROOT_PATH_REQUIRED"
    assert "hint" in response["details"]


def test_code_query_multi_artifact_root_paths_requires_object_shape(
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
                "root_paths": ["$.items_a", "$.items_b"],
                "code": "def run(artifacts, schemas, params): return []",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert (
        response["message"]
        == "root_paths must be an object keyed by artifact id"
    )
    assert response["details"]["code"] == "ROOT_PATHS_SHAPE_INVALID"
    assert "hint" in response["details"]


def test_code_query_multi_artifact_root_paths_keys_must_match_artifact_ids(
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
                "root_paths": {
                    "art_1": "$.items_a",
                    "art_3": "$.items_c",
                },
                "code": "def run(artifacts, schemas, params): return []",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["message"] == "root_paths keys do not match artifact_ids"
    details = response["details"]
    assert details["code"] == "ROOT_PATH_KEYS_MISMATCH"
    assert details["expected_artifact_ids"] == ["art_1", "art_2"]
    assert details["provided_root_paths_keys"] == ["art_1", "art_3"]
    assert details["missing_keys"] == ["art_2"]
    assert details["extra_keys"] == ["art_3"]
    assert "hint" in details


# ------------------------------------------------------------------
# _enrich_install_hint
# ------------------------------------------------------------------


def test_enrich_install_hint_no_module() -> None:
    msg = "No module named 'pandas'"
    result = _enrich_install_hint(msg)
    assert result.endswith("sift-gateway install pandas")


def test_enrich_entrypoint_hint_single_shape() -> None:
    msg = "run(...) entrypoint not found"
    result = _enrich_entrypoint_hint(
        msg,
        details_code="CODE_ENTRYPOINT_MISSING",
        multi_artifact=False,
    )
    assert "def run(data, schema, params):" in result
    assert "data is list[dict]" in result


def test_enrich_entrypoint_hint_multi_shape() -> None:
    msg = "run(...) entrypoint not found"
    result = _enrich_entrypoint_hint(
        msg,
        details_code="CODE_ENTRYPOINT_MISSING",
        multi_artifact=True,
    )
    assert "def run(artifacts, schemas, params):" in result
    assert "dict[artifact_id -> list[dict]]" in result


def test_enrich_install_hint_no_module_uses_static_map() -> None:
    """Well-known mismatches use the distribution name."""
    msg = "No module named 'sklearn'"
    result = _enrich_install_hint(msg)
    assert result.endswith("sift-gateway install scikit-learn")


def test_enrich_install_hint_import_not_allowed_third_party(
    monkeypatch,
) -> None:
    # Simulate runtime metadata returning the dist name.
    import sift_gateway.core.artifact_code as _ac_mod

    monkeypatch.setattr(
        _ac_mod,
        "packages_distributions",
        lambda: {"some_lib": ["some-lib"]},
    )
    msg = "import not allowed: some_lib"
    result = _enrich_install_hint(msg)
    assert result.endswith("sift-gateway install some-lib")


def test_enrich_install_hint_import_not_allowed_stdlib_no_hint() -> None:
    """Policy-blocked stdlib imports should not get install hints."""
    msg = "import not allowed: os"
    assert _enrich_install_hint(msg) == msg

    msg2 = "import not allowed: subprocess"
    assert _enrich_install_hint(msg2) == msg2


def test_enrich_install_hint_unrelated_message() -> None:
    msg = "something completely different"
    assert _enrich_install_hint(msg) == msg


# ------------------------------------------------------------------
# _module_to_dist
# ------------------------------------------------------------------


def test_module_to_dist_static_map(monkeypatch) -> None:
    """Static map resolves well-known mismatches."""
    # Disable runtime metadata so the static map is exercised.
    import sift_gateway.core.artifact_code as _ac_mod

    monkeypatch.setattr(
        _ac_mod,
        "packages_distributions",
        dict,
    )
    assert _module_to_dist("sklearn") == "scikit-learn"
    assert _module_to_dist("PIL") == "pillow"
    assert _module_to_dist("yaml") == "pyyaml"
    assert _module_to_dist("cv2") == "opencv-python"
    assert _module_to_dist("dateutil") == "python-dateutil"


def test_module_to_dist_runtime_metadata(monkeypatch) -> None:
    """Runtime metadata takes precedence over static map."""
    import sift_gateway.core.artifact_code as _ac_mod

    monkeypatch.setattr(
        _ac_mod,
        "packages_distributions",
        lambda: {"PIL": ["pillow-simd"]},
    )
    assert _module_to_dist("PIL") == "pillow-simd"


def test_module_to_dist_passthrough() -> None:
    """Unknown modules fall through to the root name."""
    assert _module_to_dist("pandas") == "pandas"
