"""Tests for explicit query_kind routing in artifact_consolidated."""

from __future__ import annotations

import asyncio
from pathlib import Path

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.mcp.server import GatewayServer


def _server(tmp_path: Path) -> GatewayServer:
    return GatewayServer(config=GatewayConfig(data_dir=tmp_path))


def test_query_requires_query_kind(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "query_kind is required" in response["message"]


def test_query_search_rejects_artifact_id_and_scope(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response_with_artifact = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "search",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response_with_artifact["code"] == "INVALID_ARGUMENT"
    assert "does not accept artifact_id" in response_with_artifact["message"]

    response_with_scope = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "search",
                "_gateway_context": {"session_id": "sess_1"},
                "scope": "all_related",
            }
        )
    )
    assert response_with_scope["code"] == "INVALID_ARGUMENT"
    assert "does not accept scope" in response_with_scope["message"]


def test_query_non_search_requires_artifact_id(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "describe",
                "_gateway_context": {"session_id": "sess_1"},
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert (
        "artifact_id is required for query_kind=describe" in response["message"]
    )


def test_query_get_rejects_where(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "where": "to_number(spend) > 0",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "only supported with query_kind=select" in response["message"]


def test_query_select_rejects_target_and_jsonpath(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "only supported with query_kind=get" in response["message"]


def test_query_code_ignores_scope_single(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "scope": "single",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return []",
            }
        )
    )
    assert response["code"] == "NOT_IMPLEMENTED"


def test_query_code_accepts_artifact_ids_argument(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_ids": ["art_1", "art_2"],
                "root_path": "$.items",
                "code": "def run(artifacts, schemas, params): return []",
            }
        )
    )
    assert response["code"] == "NOT_IMPLEMENTED"


def test_query_code_rejects_select_only_args(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return []",
                "where": "to_number(spend) > 0",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "does not accept" in response["message"]


def test_query_rejects_unsupported_query_kind(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "schema",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "where": "to_number(spend) > 0",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "query_kind is required" in response["message"]
