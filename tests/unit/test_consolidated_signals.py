"""Tests for contract-v1 routing in artifact_consolidated."""

from __future__ import annotations

import asyncio
from pathlib import Path

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.mcp.server import GatewayServer


def _server(tmp_path: Path) -> GatewayServer:
    return GatewayServer(config=GatewayConfig(data_dir=tmp_path))


def test_query_requires_query_kind_code(tmp_path: Path) -> None:
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
    assert "must be: code" in response["message"]


def test_query_rejects_non_code_query_kind(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "describe",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "must be: code" in response["message"]


def test_query_code_accepts_scope_single(tmp_path: Path) -> None:
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


def test_query_code_rejects_invalid_scope(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "code",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "scope": "tenant",
                "root_path": "$.items",
                "code": "def run(data, schema, params): return []",
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "scope must be one of: all_related, single" in response["message"]


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


def test_query_code_rejects_disallowed_select_args(tmp_path: Path) -> None:
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
                "where": {"path": "$.state", "op": "eq", "value": "open"},
            }
        )
    )
    assert response["code"] == "INVALID_ARGUMENT"
    assert "does not accept" in response["message"]
