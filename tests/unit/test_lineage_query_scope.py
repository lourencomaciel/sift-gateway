"""Tests for lineage-native all_related query behavior."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from sift_mcp.config.settings import GatewayConfig
from sift_mcp.mcp.server import GatewayServer
from sift_mcp.query.select_paths import select_paths_hash


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


def _artifact_row(
    artifact_id: str,
    envelope: dict[str, Any],
    *,
    generation: int = 1,
    map_kind: str = "full",
) -> tuple[object, ...]:
    return (
        artifact_id,
        f"hash_{artifact_id}",
        None,
        map_kind,
        "ready",
        generation,
        0,
        "mbf",
        envelope,
        "none",
        b"",
        0,
        False,
    )


def _server(tmp_path: Path, connection: _SeqConnection) -> GatewayServer:
    return GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(connection),  # type: ignore[arg-type]
    )


def test_select_all_related_merges_records_with_artifact_locator(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=("art_1", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=("rk_1", "$.items", 1, "array", {"id": {"number": 1}}, None, {})
            ),
            _SeqCursor(one=("art_2", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=("rk_2", "$.items", 1, "array", {"id": {"number": 1}}, None, {})
            ),
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
        "sift_mcp.mcp.handlers.artifact_select.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval_many", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
            }
        )
    )
    assert response["total_matched"] == 2
    assert {item["_locator"]["artifact_id"] for item in response["items"]} == {
        "art_1",
        "art_2",
    }
    assert response["lineage"]["artifact_count"] == 2
    assert response["lineage"]["scope"] == "all_related"


def test_select_all_related_fails_fast_on_incompatible_signatures(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=("art_1", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=("rk_1", "$.items", 1, "array", {"id": {"number": 1}}, None, {})
            ),
            _SeqCursor(one=("art_2", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=(
                    "rk_2",
                    "$.items",
                    1,
                    "array",
                    {"name": {"string": 1}},
                    None,
                    {},
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_select.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["details"]["code"] == "INCOMPATIBLE_LINEAGE_SCHEMA"


def test_select_all_related_cursor_stale_on_related_set_change(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=("art_1", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=("rk_1", "$.items", 1, "array", {"id": {"number": 1}}, None, {})
            ),
            _SeqCursor(one=("art_2", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=("rk_2", "$.items", 1, "array", {"id": {"number": 1}}, None, {})
            ),
        ]
    )
    server = _server(tmp_path, conn)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_select.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "scope": "all_related",
            "anchor_artifact_id": "art_1",
            "related_set_hash": "stale_hash",
            "root_path": "$.items",
            "select_paths_hash": select_paths_hash(["$.id"]),
            "where_hash": "__none__",
        },
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "cursor": "cur_stale",
            }
        )
    )
    assert response["code"] == "CURSOR_STALE"
    assert "related_set_hash mismatch" in response["message"]


def test_select_all_related_respects_related_artifact_limit(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(
        data_dir=tmp_path,
        related_query_max_artifacts=1,
    )
    server = GatewayServer(
        config=config,
        db_pool=_SeqPool(_SeqConnection([])),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_select.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
            }
        )
    )
    assert response["code"] == "RESOURCE_EXHAUSTED"


def test_get_all_related_jsonpath_merges_values_with_provenance(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
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
            _SeqCursor(
                one=_artifact_row(
                    "art_2",
                    {
                        "content": [
                            {
                                "type": "json",
                                "value": {"items": [{"id": 3}]},
                            }
                        ]
                    },
                )
            ),
        ]
    )
    server = _server(tmp_path, conn)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_get.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval_many", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
                "jsonpath": "$.items[*]",
            }
        )
    )
    assert len(response["items"]) == 3
    assert response["items"][0]["_locator"]["artifact_id"] == "art_1"
    assert response["items"][-1]["_locator"]["artifact_id"] == "art_2"
    assert response["lineage"]["scope"] == "all_related"
    assert isinstance(response["lineage"]["related_set_hash"], str)


def test_describe_all_related_returns_lineage_root_catalog(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(
                one=(
                    "art_1",
                    "full",
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
            _SeqCursor(
                one=(
                    "art_2",
                    "full",
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
            _SeqCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        2,
                        1.0,
                        {},
                        10.0,
                        "array",
                        {"id": {"number": 2}},
                        None,
                    )
                ]
            ),
            _SeqCursor(
                all_rows=[
                    (
                        "rk_2",
                        "$.items",
                        1,
                        1.0,
                        {},
                        10.0,
                        "array",
                        {"id": {"number": 1}},
                        None,
                    )
                ]
            ),
        ]
    )
    server = _server(tmp_path, conn)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.artifact_describe.resolve_related_artifacts",
        lambda *_args, **_kwargs: [
            {"artifact_id": "art_1", "generation": 1},
            {"artifact_id": "art_2", "generation": 1},
        ],
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval_many", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "describe",
                "scope": "all_related",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["lineage"]["artifact_count"] == 2
    assert response["roots"][0]["root_path"] == "$.items"
    assert response["roots"][0]["compatible_for_select"] is True
    assert len(response["roots"][0]["signature_groups"]) == 1
