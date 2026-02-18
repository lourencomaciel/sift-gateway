from __future__ import annotations

import asyncio
from pathlib import Path
import socket

import pytest

from sift_mcp.artifacts.create import ArtifactHandle
from sift_mcp.config.settings import (
    GatewayConfig,
    PaginationConfig,
    UpstreamConfig,
)
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.cursor.sample_set_hash import compute_sample_set_hash
from sift_mcp.cursor.token import CursorExpiredError
from sift_mcp.mcp.handlers.common import VISIBLE_ARTIFACT_SQL
from sift_mcp.mcp.server import (
    GatewayServer,
    _check_sample_corruption,
)
from sift_mcp.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)
from sift_mcp.obs.metrics import GatewayMetrics, counter_value
from sift_mcp.pagination.extract import PaginationState
from sift_mcp.query.filters import filter_hash, parse_filter_dict
from sift_mcp.query.select_paths import select_paths_hash
from sift_mcp.storage.payload_store import prepare_payload


def _server(tmp_path: Path) -> GatewayServer:
    config = GatewayConfig(data_dir=tmp_path)
    return GatewayServer(config=config)


def _upstream(prefix: str = "demo") -> UpstreamInstance:
    config = UpstreamConfig(
        prefix=prefix,
        transport="stdio",
        command="/usr/bin/printf",
    )
    return UpstreamInstance(
        config=config,
        instance_id=f"inst_{prefix}",
        tools=[
            UpstreamToolSchema(
                name="echo",
                description="Echo tool",
                input_schema={
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                },
                schema_hash="schema_echo",
            )
        ],
    )


def _upstream_with_pagination(
    prefix: str = "demo",
) -> UpstreamInstance:
    config = UpstreamConfig(
        prefix=prefix,
        transport="stdio",
        command="/usr/bin/printf",
        pagination=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.paging.next",
        ),
    )
    return UpstreamInstance(
        config=config,
        instance_id=f"inst_{prefix}",
        tools=[
            UpstreamToolSchema(
                name="echo",
                description="Echo tool",
                input_schema={
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                },
                schema_hash="schema_echo",
            )
        ],
    )


def _server_with_upstream(tmp_path: Path) -> GatewayServer:
    config = GatewayConfig(data_dir=tmp_path)
    return GatewayServer(config=config, upstreams=[_upstream()])


def _persisted_handle() -> ArtifactHandle:
    return ArtifactHandle(
        artifact_id="art_new",
        created_seq=10,
        generation=1,
        session_id="sess_1",
        source_tool="demo.echo",
        upstream_instance_id="inst_demo",
        request_key="rk_1",
        payload_hash_full="payload_hash",
        payload_json_bytes=32,
        payload_binary_bytes_total=0,
        payload_total_bytes=32,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="off",
        status="ok",
        error_summary=None,
    )


def _schema_ready_inline_describe(
    _connection: object,
    artifact_id: str,
    *,
    code_query_packages: list[str] | None = None,
) -> tuple[dict[str, object], str]:
    del code_query_packages
    return (
        {
            "artifact_id": artifact_id,
            "mapping": {
                "map_kind": "full",
                "map_status": "ready",
                "mapper_version": "mapper_v1",
                "map_budget_fingerprint": None,
                "map_backend_id": None,
                "prng_version": None,
                "traversal_contract_version": "traversal_v1",
            },
            "roots": [],
            "schemas": [
                {
                    "version": "schema_v1",
                    "schema_hash": "sha256:test_schema",
                    "root_path": "$",
                    "mode": "sampled",
                    "coverage": {
                        "completeness": "partial",
                        "observed_records": 1,
                    },
                    "fields": [],
                    "determinism": {
                        "dataset_hash": "sha256:test_dataset",
                        "traversal_contract_version": "traversal_v1",
                        "map_budget_fingerprint": None,
                    },
                }
            ],
        },
        "hint",
    )


def _patch_schema_ready_describe(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool._fetch_inline_describe",
        _schema_ready_inline_describe,
    )


class _FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _FakeDbCursor:
    rowcount: int = 1

    def __enter__(self) -> _FakeDbCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> None:
        return None


class _FakeConnection:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row
        self.committed = False

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _FakeCursor:
        return _FakeCursor(self._row)

    def cursor(self) -> _FakeDbCursor:
        return _FakeDbCursor()

    def commit(self) -> None:
        self.committed = True


class _FakeConnectionContext:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._connection = _FakeConnection(row)

    def __enter__(self) -> _FakeConnection:
        return self._connection

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def connection(self) -> _FakeConnectionContext:
        return _FakeConnectionContext(self._row)


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
        self.committed = False
        self.executed = 0

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _SeqCursor:
        self.executed += 1
        if not self._cursors:
            return _SeqCursor()
        return self._cursors.pop(0)

    def commit(self) -> None:
        self.committed = True


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


class _CaptureConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> _SeqCursor:
        self.calls.append((" ".join(query.split()), params))
        return _SeqCursor()


def test_register_tools_returns_callable_handlers(tmp_path: Path) -> None:
    server = _server(tmp_path)
    tools = server.register_tools()
    assert "gateway.status" in tools
    assert "artifact" in tools
    for handler in tools.values():
        assert callable(handler)


def test_status_handler_returns_status_payload(tmp_path: Path) -> None:
    server = _server(tmp_path)
    response = asyncio.run(server.handle_status({}))
    assert response["type"] == "gateway_status"
    assert "versions" in response
    # Without a db_pool, db health should report not configured
    assert response["db"]["ok"] is False
    # FS probes the actual filesystem; data_dir (tmp_path) exists
    # but state_dir and blobs_bin_dir do not
    assert response["fs"]["ok"] is False


def test_status_handler_probes_db_live(tmp_path: Path) -> None:
    """handle_status should run a live SELECT 1 against the pool."""
    probe_calls: list[str] = []

    class _ProbeCursor:
        def fetchone(self) -> tuple[int]:
            return (1,)

    class _ProbeConn:
        def execute(self, query: str) -> _ProbeCursor:
            probe_calls.append(query)
            return _ProbeCursor()

    class _ProbeConnCtx:
        def __enter__(self) -> _ProbeConn:
            return _ProbeConn()

        def __exit__(self, *args: object) -> None:
            pass

    class _ProbePool:
        def connection(self) -> _ProbeConnCtx:
            return _ProbeConnCtx()

    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_ProbePool(),  # type: ignore[arg-type]
    )
    response = asyncio.run(server.handle_status({}))
    assert response["db"]["ok"] is True
    assert "SELECT 1" in probe_calls


def test_status_handler_probes_fs_live(tmp_path: Path) -> None:
    """handle_status should check actual filesystem directories."""
    config = GatewayConfig(data_dir=tmp_path)
    # Create all required dirs
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.blobs_bin_dir.mkdir(parents=True, exist_ok=True)

    server = GatewayServer(config=config)
    response = asyncio.run(server.handle_status({}))
    assert response["fs"]["ok"] is True
    assert response["fs"]["paths"]["data_dir"] is True
    assert response["fs"]["paths"]["state_dir"] is True
    assert response["fs"]["paths"]["blobs_bin_dir"] is True


def test_status_handler_rejects_non_boolean_probe_upstreams(
    tmp_path: Path,
) -> None:
    server = _server(tmp_path)
    response = asyncio.run(server.handle_status({"probe_upstreams": "yes"}))
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"


def test_status_handler_cursor_section_has_ttl_only(
    tmp_path: Path,
) -> None:
    """Cursor section should only contain TTL setting."""
    server = _server(tmp_path)
    response = asyncio.run(server.handle_status({}))
    cursor = response["cursor"]
    assert cursor["cursor_ttl_minutes"] == 60
    assert "secrets_loaded" not in cursor
    assert "active_secret_count" not in cursor


def test_status_handler_includes_upstream_connectivity(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    server.upstream_runtime["demo"] = {
        "last_error_code": "UPSTREAM_DNS_FAILURE",
        "last_error_message": "name not known",
    }
    server.upstream_errors["broken"] = "timeout"
    response = asyncio.run(server.handle_status({}))
    upstreams = response["upstreams"]
    connected = [u for u in upstreams if u.get("connected") is True]
    disconnected = [u for u in upstreams if u.get("connected") is False]
    assert len(connected) == 1
    assert connected[0]["prefix"] == "demo"
    assert connected[0]["tool_count"] == 1
    assert connected[0]["transport"] == "stdio"
    assert "command_resolvable" in connected[0]
    assert connected[0]["runtime"]["last_error_code"] == "UPSTREAM_DNS_FAILURE"
    assert len(disconnected) == 1
    assert disconnected[0]["prefix"] == "broken"
    assert (
        disconnected[0]["startup_error"]["code"] == "UPSTREAM_STARTUP_FAILURE"
    )
    assert disconnected[0]["startup_error"]["message"] == "timeout"


def test_status_handler_runs_active_upstream_probes(
    tmp_path: Path,
) -> None:
    server = _server_with_upstream(tmp_path)
    response = asyncio.run(server.handle_status({"probe_upstreams": True}))
    upstream = response["upstreams"][0]
    assert "active_probe" in upstream
    assert isinstance(upstream["active_probe"]["ok"], bool)


def test_status_handler_module_probe_handles_missing_parent_module(
    tmp_path: Path,
) -> None:
    config = UpstreamConfig(
        prefix="demo",
        transport="stdio",
        command="/usr/bin/python3",
        args=["-m", "a.b.c"],
    )
    upstream = UpstreamInstance(
        config=config,
        instance_id="inst_demo",
        tools=[],
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[upstream],
    )
    response = asyncio.run(server.handle_status({}))
    probe = response["upstreams"][0]["module_probe"]
    assert probe["module"] == "a.b.c"
    assert probe["importable"] is False
    assert isinstance(probe["error"], str)


def test_cursor_error_updates_metrics(tmp_path: Path) -> None:
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path), metrics=GatewayMetrics()
    )
    expired = server._cursor_error(CursorExpiredError("expired"))
    assert expired["code"] == "CURSOR_EXPIRED"
    assert counter_value(server.metrics.cursor_expired) == 1

    stale_budget = server._cursor_error(
        CursorStaleError("cursor map_budget_fingerprint mismatch")
    )
    assert stale_budget["code"] == "CURSOR_STALE"
    assert counter_value(server.metrics.cursor_stale_map_budget) == 1

    invalid = server._cursor_error(ValueError("bad token"))
    assert invalid["code"] == "INVALID_ARGUMENT"
    assert counter_value(server.metrics.cursor_invalid) == 1


def test_artifact_handlers_return_validation_or_not_implemented(
    tmp_path: Path,
) -> None:
    server = _server(tmp_path)

    invalid_legacy = asyncio.run(server.handle_artifact({"action": "get"}))
    assert invalid_legacy["code"] == "INVALID_ARGUMENT"
    assert "query" in invalid_legacy["message"]

    invalid_query = asyncio.run(server.handle_artifact({"action": "query"}))
    assert invalid_query["code"] == "INVALID_ARGUMENT"

    valid_get = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert valid_get["code"] == "NOT_IMPLEMENTED"

    valid_search = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "search",
                "_gateway_context": {"session_id": "sess_1"},
                "filters": {},
            }
        )
    )
    assert valid_search["code"] == "NOT_IMPLEMENTED"

    invalid_schema = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "schema",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert invalid_schema["code"] == "INVALID_ARGUMENT"


def test_build_fastmcp_app_includes_mirrored_tools(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    app = server.build_fastmcp_app()
    tools = asyncio.run(app.get_tools())
    tool_names = set(tools.keys())
    assert "gateway_status" in tool_names
    assert "demo_echo" in tool_names
    assert "retrieval_status == COMPLETE" in tools["demo_echo"].description
    assert "retrieval_status == COMPLETE" in tools["artifact"].description
    artifact_schema = tools["artifact"].parameters
    assert "scope" in artifact_schema["properties"]
    assert artifact_schema["properties"]["scope"]["enum"] == [
        "all_related",
        "single",
    ]


def test_build_fastmcp_app_rejects_safe_name_collisions(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    upstream_a = UpstreamInstance(
        config=UpstreamConfig(
            prefix="acme",
            transport="stdio",
            command="/usr/bin/printf",
        ),
        instance_id="inst_acme",
        tools=[
            UpstreamToolSchema(
                name="foo_bar",
                description="Tool A",
                input_schema={"type": "object", "properties": {}},
                schema_hash="schema_a",
            )
        ],
    )
    upstream_b = UpstreamInstance(
        config=UpstreamConfig(
            prefix="acme_foo",
            transport="stdio",
            command="/usr/bin/printf",
        ),
        instance_id="inst_acme_foo",
        tools=[
            UpstreamToolSchema(
                name="bar",
                description="Tool B",
                input_schema={"type": "object", "properties": {}},
                schema_hash="schema_b",
            )
        ],
    )
    server = GatewayServer(
        config=config,
        upstreams=[upstream_a, upstream_b],
    )
    with pytest.raises(ValueError, match="sanitization"):
        server.build_fastmcp_app()


def test_handle_mirrored_tool_rejects_schema_violations(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    mirrored = server.mirrored_tools["demo.echo"]
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {"_gateway_context": {"session_id": "sess_1"}},
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"
    assert "violations" in response["details"]


def test_handle_mirrored_tool_rejects_invalid_chain_seq(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    mirrored = server.mirrored_tools["demo.echo"]
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "_gateway_chain_seq": -1,
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"


def test_handle_mirrored_tool_rejects_oversized_arguments(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(
        data_dir=tmp_path,
        max_inbound_request_bytes=32,
    )
    server = GatewayServer(config=config, upstreams=[_upstream()])
    mirrored = server.mirrored_tools["demo.echo"]

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "x" * 512,
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["message"] == "arguments exceed max_inbound_request_bytes"
    assert response["details"]["max_inbound_request_bytes"] == 32
    assert response["details"]["actual_bytes"] > 32


def test_handle_mirrored_tool_rejects_oversized_reserved_arguments(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(
        data_dir=tmp_path,
        max_inbound_request_bytes=64,
    )
    server = GatewayServer(config=config, upstreams=[_upstream()])
    mirrored = server.mirrored_tools["demo.echo"]

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                    "padding": "x" * 512,
                },
                "message": "ok",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["message"] == "arguments exceed max_inbound_request_bytes"
    assert response["details"]["max_inbound_request_bytes"] == 64
    assert response["details"]["actual_bytes"] > 64


def test_handle_mirrored_tool_rejects_non_utf8_json_arguments(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(data_dir=tmp_path, max_inbound_request_bytes=1024)
    server = GatewayServer(config=config, upstreams=[_upstream()])
    mirrored = server.mirrored_tools["demo.echo"]

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "\ud800",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INVALID_ARGUMENT"
    assert response["message"] == "arguments must be valid UTF-8 JSON"


def test_handle_mirrored_tool_requires_db_for_schema_first_response(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    server = GatewayServer(config=config, upstreams=[_upstream()])
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {"trace_id": "abc"},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "NOT_IMPLEMENTED"


def test_handle_mirrored_tool_without_db_returns_not_implemented(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    server = GatewayServer(
        config=config,
        upstreams=[_upstream_with_pagination()],
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [],
            "structuredContent": {
                "data": [{"id": "1"}],
                "paging": {
                    "cursors": {"after": "CURSOR_2"},
                    "next": "https://example.com/page2",
                },
            },
            "isError": False,
            "meta": {"trace_id": "abc"},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "NOT_IMPLEMENTED"


def test_handle_mirrored_tool_sets_stable_upstream_error_code(
    tmp_path: Path,
    monkeypatch,
) -> None:
    server = GatewayServer(
        config=GatewayConfig(
            data_dir=tmp_path,
            quota_enforcement_enabled=False,
        ),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _dns_failure(*_args, **_kwargs):
        raise socket.gaierror("nodename nor servname provided")

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _dns_failure)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kwargs: _persisted_handle(),
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **_kwargs: True,
    )
    _patch_schema_ready_describe(monkeypatch)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_new"
    assert (
        server.upstream_runtime["demo"]["last_error_code"]
        == "UPSTREAM_DNS_FAILURE"
    )


def test_artifact_search_db_runtime_returns_items(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(
                all_rows=[
                    (
                        "art_1",
                        1,
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        "demo.echo",
                        "inst_demo",
                        "ok",
                        123,
                        None,
                        "none",
                        "pending",
                    )
                ]
            )
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_search", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "search",
                "_gateway_context": {"session_id": "sess_1"},
                "filters": {},
            }
        )
    )
    assert response["truncated"] is False
    assert response["pagination"]["layer"] == "artifact_retrieval"
    assert response["pagination"]["retrieval_status"] == "COMPLETE"
    assert response["items"][0]["artifact_id"] == "art_1"
    assert conn.committed is False


def test_artifact_search_touches_session_when_not_mocked(
    tmp_path: Path,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(
                all_rows=[
                    (
                        "art_1",
                        1,
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        "demo.echo",
                        "inst_demo",
                        "ok",
                        123,
                        None,
                        "none",
                        "pending",
                    )
                ]
            )
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "search",
                "_gateway_context": {"session_id": "sess_1"},
                "filters": {},
            }
        )
    )
    assert response["truncated"] is False
    assert conn.committed is True


def test_artifact_get_db_runtime_returns_envelope_items(
    tmp_path: Path, monkeypatch
) -> None:
    envelope = {
        "type": "mcp_envelope",
        "upstream_instance_id": "inst_demo",
        "upstream_prefix": "demo",
        "tool": "echo",
        "status": "ok",
        "content": [{"type": "json", "value": {"id": 1}}],
        "error": None,
        "meta": {"warnings": []},
    }
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,
                    "mbf",
                    envelope,
                    "gzip",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
            }
        )
    )
    assert response["target"] == "envelope"
    assert response["pagination"]["layer"] == "artifact_retrieval"
    assert response["pagination"]["retrieval_status"] == "COMPLETE"
    assert response["items"][0]["_locator"]["artifact_id"] == "art_1"
    assert response["items"][0]["value"]["type"] == "mcp_envelope"
    assert conn.committed is False


def test_artifact_get_touches_recency_when_not_mocked(
    tmp_path: Path,
) -> None:
    envelope = {
        "type": "mcp_envelope",
        "upstream_instance_id": "inst_demo",
        "upstream_prefix": "demo",
        "tool": "echo",
        "status": "ok",
        "content": [{"type": "json", "value": {"id": 1}}],
        "error": None,
        "meta": {"warnings": []},
    }
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,
                    "mbf",
                    envelope,
                    "gzip",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
            }
        )
    )
    assert response["target"] == "envelope"
    assert conn.committed is True


def test_visible_artifact_sql_does_not_hide_deleted_rows() -> None:
    assert "deleted_at IS NULL" not in VISIBLE_ARTIFACT_SQL


def test_safe_touch_for_retrieval_writes_session_and_artifact(
    tmp_path: Path,
) -> None:
    server = _server(tmp_path)
    conn = _CaptureConnection()

    touched = server._safe_touch_for_retrieval(
        conn,
        session_id="sess_1",
        artifact_id="art_1",
    )

    assert touched is True
    assert len(conn.calls) == 2
    assert "INSERT INTO sessions" in conn.calls[0][0]
    assert conn.calls[0][1] == ("local", "sess_1")
    assert "UPDATE artifacts" in conn.calls[1][0]
    assert conn.calls[1][1] == ("local", "art_1")


def test_safe_touch_for_retrieval_many_deduplicates_artifacts(
    tmp_path: Path,
) -> None:
    server = _server(tmp_path)
    conn = _CaptureConnection()

    touched = server._safe_touch_for_retrieval_many(
        conn,
        session_id="sess_1",
        artifact_ids=["art_1", "art_1", "art_2"],
    )

    assert touched is True
    update_calls = [
        call for call in conn.calls if "UPDATE artifacts" in call[0]
    ]
    assert len(update_calls) == 2
    assert update_calls[0][1] == ("local", "art_1")
    assert update_calls[1][1] == ("local", "art_2")


def test_safe_touch_for_search_updates_session_only(
    tmp_path: Path,
) -> None:
    server = _server(tmp_path)
    conn = _CaptureConnection()

    touched = server._safe_touch_for_search(
        conn,
        session_id="sess_1",
        artifact_ids=["art_1", "art_2"],
    )

    assert touched is True
    assert len(conn.calls) == 1
    assert "INSERT INTO sessions" in conn.calls[0][0]
    assert conn.calls[0][1] == ("local", "sess_1")


def test_artifact_get_cursor_includes_target_and_jsonpath_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,
                    "mbf",
                    {"content": [], "items": [{"id": 1}, {"id": 2}]},
                    "gzip",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
                "jsonpath": "$['items'][*]",
                "limit": 1,
            }
        )
    )
    assert response["truncated"] is True
    assert response["cursor"] == "cur_next"
    assert response["pagination"]["retrieval_status"] == "PARTIAL"
    assert response["pagination"]["partial_reason"] == "CURSOR_AVAILABLE"
    assert response["pagination"]["next_cursor"] == "cur_next"
    extra = issued["extra"]
    assert isinstance(extra, dict)
    assert extra["target"] == "envelope"
    assert extra["normalized_jsonpath"] == "$.items[*]"
    assert extra["artifact_generation"] == 1


def test_artifact_get_jsonpath_evaluates_against_json_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Jsonpath evaluates against the mapped JSON part, not the envelope."""
    envelope = {
        "type": "mcp_envelope",
        "content": [
            {
                "type": "json",
                "value": {"users": [{"id": 1}, {"id": 2}]},
            }
        ],
    }
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,  # mapped_part_index
                    "",
                    envelope,
                    "none",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
                "jsonpath": "$.users[*]",
            }
        )
    )
    # $.users[*] resolves through the JSON content part, not the envelope.
    assert len(response["items"]) == 2
    assert response["items"][0]["value"] == {"id": 1}
    assert response["items"][1]["value"] == {"id": 2}
    assert response["pagination"]["retrieval_status"] == "COMPLETE"


def test_artifact_get_cursor_target_mismatch_returns_stale(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,
                    "mbf",
                    {"content": [], "items": [{"id": 1}]},
                    "gzip",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "target": "mapped",
            "normalized_jsonpath": "$.items[*]",
            "artifact_generation": 1,
        },
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
                "jsonpath": "$.items[*]",
                "cursor": "cursor_1",
            }
        )
    )
    assert response["code"] == "CURSOR_STALE"
    assert "target mismatch" in response["message"]


def test_artifact_get_cursor_restores_scope_when_omitted(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "payload_hash",
                    None,
                    "full",
                    "ready",
                    1,
                    0,
                    "mbf",
                    {"content": [], "items": [{"id": 1}]},
                    "gzip",
                    b"",
                    0,
                    False,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "target": "envelope",
            "normalized_jsonpath": "$",
            "scope": "single",
            "artifact_generation": 1,
        },
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
                "cursor": "cursor_1",
            }
        )
    )
    assert response["scope"] == "single"
    assert response["items"][0]["_locator"]["artifact_id"] == "art_1"


def test_artifact_describe_db_runtime_returns_roots(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
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
            _SeqCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        100,
                        1.0,
                        {
                            "sampled_record_count": 2,
                            "sampled_prefix_len": 7,
                            "prefix_coverage": True,
                            "stop_reason": "max_compute",
                            "skipped_oversize_records": 1,
                        },
                        10.0,
                        "array",
                        {"id": {"number": 10}},
                        [0, 1],
                    )
                ]
            ),
            _SeqCursor(
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
            _SeqCursor(all_rows=[("$.id", ["number"], False, True, 2)]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "describe",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["artifact_id"] == "art_1"
    assert response["roots"][0]["root_path"] == "$.items"
    assert response["roots"][0]["compatible_for_select"] is True
    assert (
        response["roots"][0]["signature_groups"][0]["schema_mode"] == "sampled"
    )


def test_artifact_select_db_runtime_partial_projects_records(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    "schema_v1",
                    "sha256:schema_items",
                    "sampled",
                    "partial",
                    1,
                    "sha256:dataset_items",
                    "traversal_v1",
                    "mbf",
                )
            ),
            _SeqCursor(all_rows=[(0,)]),
            _SeqCursor(all_rows=[(0, {"$.id": 1})]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "where": {"path": "$.id", "op": "eq", "value": 1},
            }
        )
    )
    assert response["sampled_only"] is True
    assert response["sampled_prefix_len"] == 9
    assert response["items"][0]["projection"]["$.id"] == 1
    assert response["pagination"]["retrieval_status"] == "COMPLETE"


def test_artifact_select_cursor_sample_set_mismatch_returns_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(all_rows=[(0,), (1,)]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    where_expr = {"path": "$.id", "op": "eq", "value": 1}
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "scope": "single",
            "root_path": "$.items",
            "select_paths_hash": select_paths_hash(["$.id"]),
            "where_hash": filter_hash(
                parse_filter_dict(where_expr),
            ),
            "artifact_generation": 1,
            "map_budget_fingerprint": "mbf",
            "sample_set_hash": "bad_hash",
        },
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "where": where_expr,
                "cursor": "cursor_1",
            }
        )
    )
    assert response["code"] == "CURSOR_STALE"
    assert "sample_set_hash mismatch" in response["message"]
    assert counter_value(server.metrics.cursor_stale_sample_set) == 1


def test_artifact_select_cursor_includes_partial_binding_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(all_rows=[(0,), (1,)]),
            _SeqCursor(
                all_rows=[
                    (0, {"$.id": 1}),
                    (1, {"$.id": 2}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "limit": 1,
            }
        )
    )
    assert response["truncated"] is True
    assert response["cursor"] == "cur_next"
    assert response["pagination"]["retrieval_status"] == "PARTIAL"
    assert response["pagination"]["next_cursor"] == "cur_next"

    extra = issued["extra"]
    assert isinstance(extra, dict)
    assert extra["root_path"] == "$.items"
    assert extra["select_paths"] == ["$.id"]
    assert extra["where_serialized"] is None
    assert extra["select_paths_hash"] == select_paths_hash(["$.id"])
    assert extra["where_hash"] == "__none__"
    assert extra["artifact_generation"] == 1
    assert extra["map_budget_fingerprint"] == "mbf"
    assert extra["sample_set_hash"] == compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[0, 1],
        map_budget_fingerprint="mbf",
    )


def test_artifact_select_cursor_restores_scope_when_omitted(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "full", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    2,
                    "array",
                    {"id": {"number": 2}},
                    None,
                    {},
                )
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    "schema_v1",
                    "sha256:schema_items_full",
                    "exact",
                    "complete",
                    2,
                    "sha256:dataset_items_full",
                    "traversal_v1",
                    None,
                )
            ),
            _SeqCursor(
                all_rows=[
                    (0, {"$.id": 1}),
                    (1, {"$.id": 2}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "scope": "single",
            "root_path": "$.items",
            "select_paths_hash": select_paths_hash(["$.id"]),
            "where_hash": "__none__",
            "artifact_generation": 1,
        },
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "cursor": "cursor_1",
            }
        )
    )
    assert response["scope"] == "single"
    assert response["total_matched"] == 2


def test_artifact_select_cursor_always_binds_order_by(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(all_rows=[(0,), (1,)]),
            _SeqCursor(
                all_rows=[
                    (0, {"$.id": 1}),
                    (1, {"$.id": 2}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "order_by": "created_seq_desc",
                "limit": 1,
            }
        )
    )
    assert response["truncated"] is True
    extra = issued["extra"]
    assert isinstance(extra, dict)
    assert extra["order_by"] == "created_seq_desc"


def test_artifact_next_page_returns_gone_for_deleted_artifact(
    tmp_path: Path,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    "2026-01-01T00:00:00Z",
                    {"meta": {"warnings": []}},
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "next_page",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["code"] == "GONE"
    assert response["message"] == "artifact has been deleted"


def test_artifact_next_page_uses_canonical_envelope_when_jsonb_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state = PaginationState(
        upstream_prefix="demo",
        tool_name="echo",
        original_args={"message": "hello"},
        next_params={"after": "CURSOR_2"},
        page_number=0,
    )
    envelope = {
        "type": "mcp_envelope",
        "upstream_instance_id": "inst_demo",
        "upstream_prefix": "demo",
        "tool": "echo",
        "status": "ok",
        "content": [],
        "error": None,
        "meta": {"_gateway_pagination": state.to_dict()},
    }
    prepared = prepare_payload(envelope)
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=(
                    "art_1",
                    None,
                    prepared.payload_hash,
                    None,
                    prepared.encoding,
                    prepared.compressed_bytes,
                )
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream_with_pagination()],
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )

    captured: dict[str, object] = {}

    async def _fake_next_page_call(_ctx, mirrored, arguments):
        captured["qualified_name"] = mirrored.qualified_name
        captured["arguments"] = arguments
        return {"type": "gateway_tool_result", "artifact_id": "art_2"}

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.handle_mirrored_tool",
        _fake_next_page_call,
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "next_page",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["artifact_id"] == "art_2"
    assert captured["qualified_name"] == "demo.echo"
    assert captured["arguments"] == {
        "message": "hello",
        "after": "CURSOR_2",
        "_gateway_context": {"session_id": "sess_1"},
        "_gateway_parent_artifact_id": "art_1",
        "_gateway_chain_seq": 1,
    }


# ---------------------------------------------------------------------------
# Sample corruption detection
# ---------------------------------------------------------------------------


def test_check_sample_corruption_returns_none_when_indices_match() -> None:
    root_row = {"root_key": "rk_1", "sample_indices": [0, 3, 7]}
    sample_rows = [
        {"sample_index": 0},
        {"sample_index": 3},
        {"sample_index": 7},
    ]
    assert _check_sample_corruption(root_row, sample_rows) is None


def test_check_sample_corruption_returns_none_when_no_expected_indices() -> (
    None
):
    root_row = {"root_key": "rk_1", "sample_indices": None}
    assert _check_sample_corruption(root_row, []) is None

    root_row_empty = {"root_key": "rk_1", "sample_indices": []}
    assert _check_sample_corruption(root_row_empty, []) is None


def test_check_sample_corruption_returns_internal_when_rows_missing() -> None:
    root_row = {"root_key": "rk_1", "sample_indices": [0, 3, 7]}
    sample_rows = [{"sample_index": 0}]  # indices 3 and 7 missing
    result = _check_sample_corruption(root_row, sample_rows)
    assert result is not None
    assert result["code"] == "INTERNAL"
    assert "corruption" in result["message"]
    assert result["details"]["missing_indices"] == [3, 7]
    assert result["details"]["expected_count"] == 3
    assert result["details"]["actual_count"] == 1



# ---------------------------------------------------------------------------
# Health gate: INTERNAL on unhealthy DB or FS
# ---------------------------------------------------------------------------


def test_handle_mirrored_tool_returns_internal_when_db_unhealthy(
    tmp_path: Path,
) -> None:
    """When db_pool exists, db_ok=False, and recovery probe fails, return INTERNAL."""
    import sqlite3

    class _DeadPool:
        def connection(self):
            raise sqlite3.OperationalError("connection refused")

    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_DeadPool(),  # type: ignore[arg-type]
        db_ok=False,
    )
    mirrored = server.mirrored_tools["demo.echo"]
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "database" in response["message"]
    assert server.db_ok is False


def test_handle_mirrored_tool_recovers_db_ok_on_successful_probe(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When db_ok=False but DB probe succeeds, recover and proceed normally."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]  # probe SELECT 1 succeeds
        db_ok=False,
        metrics=GatewayMetrics(),
    )
    assert server.db_ok is False

    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    _fake_handle = ArtifactHandle(
        artifact_id="art_recovered",
        created_seq=1,
        generation=1,
        session_id="sess_1",
        source_tool="demo.echo",
        upstream_instance_id="demo",
        request_key="rk_1",
        payload_hash_full="h_1",
        payload_json_bytes=10,
        payload_binary_bytes_total=0,
        payload_total_bytes=10,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="pending",
        status="ok",
        error_summary=None,
    )

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kw: _fake_handle,
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **_kwargs: True,
    )
    _patch_schema_ready_describe(monkeypatch)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_recovered"
    assert server.db_ok is True  # recovered from transient failure


def test_handle_mirrored_tool_returns_internal_when_fs_unhealthy(
    tmp_path: Path,
) -> None:
    """When fs_ok=False, return INTERNAL before calling upstream."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        fs_ok=False,
    )
    mirrored = server.mirrored_tools["demo.echo"]
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "filesystem" in response["message"]


def test_handle_mirrored_tool_returns_internal_on_ref_resolution_connectivity_failure(
    tmp_path: Path,
) -> None:
    """When ref resolution checkout fails, return INTERNAL and mark db_ok=False."""
    import sqlite3

    class _FailPool:
        def connection(self):
            raise sqlite3.OperationalError("connection refused")

    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FailPool(),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "artifact ref resolution failed" in response["message"].lower()
    assert server.db_ok is False


def test_handle_mirrored_tool_returns_internal_on_db_persist_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When persist_artifact raises a DB error, return INTERNAL and mark db_ok=False."""
    import sqlite3

    class _FailPool:
        def __init__(self) -> None:
            self._checkouts = 0

        def connection(self):
            self._checkouts += 1
            if self._checkouts == 1:
                return _FakeConnectionContext(None)
            raise sqlite3.OperationalError("connection lost")

    server = GatewayServer(
        config=GatewayConfig(
            data_dir=tmp_path,
            quota_enforcement_enabled=False,
        ),
        upstreams=[_upstream()],
        db_pool=_FailPool(),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)
    # Proceed to persist on the first DB checkout.
    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "persistence failed" in response["message"]
    assert "unhealthy" in response["message"]
    assert server.db_ok is False


def test_handle_mirrored_tool_keeps_db_ok_on_integrity_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """IntegrityError (FK violation, unique conflict) returns INTERNAL but keeps db_ok=True."""
    import sqlite3

    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    def _fk_persist(**_kw):
        raise sqlite3.IntegrityError("violates foreign key constraint")

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        _fk_persist,
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "persistence failed" in response["message"]
    assert "unhealthy" not in response["message"]
    assert server.db_ok is True


def test_handle_mirrored_tool_returns_internal_on_non_db_persist_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When persist_artifact raises a non-DB error, return INTERNAL but keep db_ok=True."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    def _bad_persist(**_kw):
        raise ValueError("canonicalization rejected float")

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        _bad_persist,
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "persistence failed" in response["message"]
    assert "unhealthy" not in response["message"]
    assert response["details"]["stage"] == "persist_artifact"
    assert response["details"]["error_type"] == "ValueError"
    assert server.db_ok is True


def test_handle_mirrored_tool_falls_back_when_inline_describe_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Inline describe failures should degrade to a minimal describe payload."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kw: _persisted_handle(),
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **_kwargs: True,
    )

    def _boom_inline_describe(*_args, **_kwargs):
        raise RuntimeError("column distinct_values does not exist")

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool._fetch_inline_describe",
        _boom_inline_describe,
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )

    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_new"
    assert response["mapping"]["map_status"] == "pending"
    assert "Mapping in progress" in response["usage_hint"]


def test_handle_mirrored_tool_returns_internal_when_mapping_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When inline mapping does not complete, return INTERNAL."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)

    _fake_handle = ArtifactHandle(
        artifact_id="art_mapping_fail",
        created_seq=1,
        generation=1,
        session_id="sess_1",
        source_tool="demo.echo",
        upstream_instance_id="demo",
        request_key="rk_1",
        payload_hash_full="h_1",
        payload_json_bytes=10,
        payload_binary_bytes_total=0,
        payload_total_bytes=10,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="pending",
        status="ok",
        error_summary=None,
    )

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kw: _fake_handle,
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **_kwargs: False,
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_error"
    assert response["code"] == "INTERNAL"
    assert "mapping did not complete" in response["message"]
    assert server.db_ok is True


def test_handle_mirrored_tool_triggers_mapping_on_single_connection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Mapping uses the persist connection after one resolve checkout."""
    import sqlite3

    checkout_count = 0
    mapping_called = False

    class _TwoCheckoutPool:
        """Allow resolve + persist checkouts; third would raise."""

        def connection(self):
            nonlocal checkout_count
            checkout_count += 1
            if checkout_count > 2:
                raise sqlite3.OperationalError("pool exhausted")
            return _FakeConnectionContext(None)

    server = GatewayServer(
        config=GatewayConfig(
            data_dir=tmp_path,
            quota_enforcement_enabled=False,
        ),
        upstreams=[_upstream()],
        db_pool=_TwoCheckoutPool(),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)
    _fake_handle = ArtifactHandle(
        artifact_id="art_single_conn",
        created_seq=1,
        generation=1,
        session_id="sess_1",
        source_tool="demo.echo",
        upstream_instance_id="demo",
        request_key="rk_1",
        payload_hash_full="h_1",
        payload_json_bytes=10,
        payload_binary_bytes_total=0,
        payload_total_bytes=10,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="pending",
        status="ok",
        error_summary=None,
    )

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kw: _fake_handle,
    )

    def _track_mapping(*_args, **_kwargs):
        nonlocal mapping_called
        mapping_called = True
        return True

    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        _track_mapping,
    )
    _patch_schema_ready_describe(monkeypatch)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {
                    "session_id": "sess_1",
                },
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_single_conn"
    assert checkout_count == 2  # resolve + persist/mapping
    assert mapping_called is True  # mapping still ran


# ---------------------------------------------------------------------------
# Quota preflight removed
# ---------------------------------------------------------------------------


def test_handle_mirrored_tool_does_not_run_quota_preflight(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Mirrored calls proceed without quota preflight blocking."""
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]
    quota_called = False

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {},
        }

    monkeypatch.setattr("sift_mcp.mcp.server.call_upstream_tool", _fake_call)
    # The handler no longer imports quota preflight helpers. If someone
    # injects one, it must not be consulted on request flow.
    def _forbidden_quota(*_args, **_kwargs):
        nonlocal quota_called
        quota_called = True
        raise AssertionError("quota preflight should not run")

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.enforce_quota",
        _forbidden_quota,
        raising=False,
    )

    _fake_handle = ArtifactHandle(
        artifact_id="art_no_quota_preflight",
        created_seq=1,
        generation=1,
        session_id="sess_1",
        source_tool="demo.echo",
        upstream_instance_id="demo",
        request_key="rk_1",
        payload_hash_full="h_1",
        payload_json_bytes=10,
        payload_binary_bytes_total=0,
        payload_total_bytes=10,
        contains_binary_refs=False,
        map_kind="none",
        map_status="pending",
        index_status="pending",
        status="ok",
        error_summary=None,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kw: _fake_handle,
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **_kwargs: True,
    )
    _patch_schema_ready_describe(monkeypatch)

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {
                "_gateway_context": {"session_id": "sess_1"},
                "message": "hello",
            },
        )
    )
    assert response["type"] == "gateway_tool_result"
    assert response["artifact_id"] == "art_no_quota_preflight"
    assert quota_called is False


def test_artifact_select_includes_total_matched(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(all_rows=[(0,), (1,)]),
            _SeqCursor(
                all_rows=[
                    (0, {"$.id": 1}),
                    (1, {"$.id": 2}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
            }
        )
    )
    assert response["total_matched"] == 2
    assert len(response["items"]) == 2


def test_artifact_select_count_only(tmp_path: Path, monkeypatch) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(one=(2,)),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "count_only": True,
            }
        )
    )
    assert response["count"] == 2
    assert "items" not in response
    assert response["truncated"] is False


def test_artifact_select_count_only_with_where_filter(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"id": {"number": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(one=(1,)),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
                "where": {"path": "$.id", "op": "eq", "value": 1},
                "count_only": True,
            }
        )
    )
    assert response["count"] == 1


def test_artifact_select_distinct_deduplicates(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"name": {"string": 1}},
                    [0, 1, 2],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    "schema_v1",
                    "sha256:schema_items",
                    "sampled",
                    "partial",
                    3,
                    "sha256:dataset_items",
                    "traversal_v1",
                    "mbf",
                )
            ),
            _SeqCursor(all_rows=[(0,), (1,), (2,)]),
            _SeqCursor(
                all_rows=[
                    (0, {"$.name": "A"}),
                    (1, {"$.name": "A"}),
                    (2, {"$.name": "B"}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["name"],
                "distinct": True,
            }
        )
    )
    assert response["total_matched"] == 2
    projections = [item["projection"] for item in response["items"]]
    assert {"$.name": "A"} in projections
    assert {"$.name": "B"} in projections
    assert len(projections) == 2


def test_artifact_select_distinct_embeds_in_cursor(
    tmp_path: Path, monkeypatch
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                one=("art_1", "partial", "ready", "off", None, 1, "mbf")
            ),
            _SeqCursor(
                one=(
                    "rk_1",
                    "$.items",
                    100,
                    "array",
                    {"name": {"string": 1}},
                    [0, 1],
                    {"sampled_prefix_len": 9},
                )
            ),
            _SeqCursor(
                one=(
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
            ),
            _SeqCursor(all_rows=[(0,), (1,)]),
            _SeqCursor(
                all_rows=[
                    (0, {"$.name": "A"}),
                    (1, {"$.name": "B"}),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None
    )
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "select",
                "scope": "single",
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["name"],
                "distinct": True,
                "limit": 1,
            }
        )
    )
    assert response["truncated"] is True
    extra = issued["extra"]
    assert extra["distinct"] is True


def test_artifact_select_rejects_wildcard_star() -> None:
    from sift_mcp.tools.artifact_select import (
        validate_select_args,
    )

    err = validate_select_args(
        {
            "_gateway_context": {"session_id": "s"},
            "artifact_id": "art_1",
            "root_path": "$.items",
            "select_paths": ["*"],
        }
    )
    assert err is not None
    assert err["code"] == "INVALID_ARGUMENT"
    assert "Wildcard" in err["message"]
    assert "action='query'" in err["message"]


def test_artifact_get_rejects_where_param(
    tmp_path: Path,
) -> None:
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(  # type: ignore[arg-type]
            _SeqConnection([])
        ),
    )
    response = asyncio.run(
        server.handle_artifact(
            {
                "action": "query",
                "query_kind": "get",
                "scope": "single",
                "_gateway_context": {"session_id": "s"},
                "artifact_id": "art_1",
                "target": "envelope",
                "where": 'spend != "0"',
            }
        )
    )
    assert response.get("code") == "INVALID_ARGUMENT"
    assert "only supported with query_kind=select" in response["message"]


def test_jsonpath_rejects_union_syntax() -> None:
    import pytest

    from sift_mcp.query.jsonpath import (
        JsonPathError,
        parse_jsonpath,
    )

    with pytest.raises(JsonPathError, match="union"):
        parse_jsonpath("$.data[name,spend]")
