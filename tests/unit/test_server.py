from __future__ import annotations

import asyncio
from pathlib import Path

from mcp_artifact_gateway.artifacts.create import ArtifactHandle
from mcp_artifact_gateway.config.settings import GatewayConfig, UpstreamConfig
from mcp_artifact_gateway.cursor.hmac import CursorExpiredError
from mcp_artifact_gateway.cursor.payload import CursorStaleError
from mcp_artifact_gateway.cursor.sample_set_hash import compute_sample_set_hash
from mcp_artifact_gateway.cursor.secrets import CursorSecrets
from mcp_artifact_gateway.mcp.server import GatewayServer
from mcp_artifact_gateway.mcp.upstream import UpstreamInstance, UpstreamToolSchema
from mcp_artifact_gateway.obs.metrics import GatewayMetrics
from mcp_artifact_gateway.query.select_paths import select_paths_hash
from mcp_artifact_gateway.query.where_hash import where_hash


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


class _FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def execute(
        self,
        _query: str,
        _params: tuple[object, ...] | None = None,
    ) -> _FakeCursor:
        return _FakeCursor(self._row)


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


def test_register_tools_returns_callable_handlers(tmp_path: Path) -> None:
    server = _server(tmp_path)
    tools = server.register_tools()
    assert "gateway.status" in tools
    assert "artifact.search" in tools
    assert "artifact.get" in tools
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
    assert response["mapping_mode"] == server.config.mapping_mode.value


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


def test_status_handler_includes_cursor_secrets_info(tmp_path: Path) -> None:
    """handle_status should include signing_version and active_versions when secrets are loaded."""
    secrets = CursorSecrets(
        active={"v1": "secret_a", "v2": "secret_b"},
        signing_version="v2",
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        cursor_secrets=secrets,
    )
    response = asyncio.run(server.handle_status({}))
    cursor = response["cursor"]
    assert cursor["signing_version"] == "v2"
    assert cursor["active_versions"] == ["v1", "v2"]


def test_status_handler_omits_cursor_secrets_when_not_loaded(tmp_path: Path) -> None:
    """When cursor_secrets is None, the status response should not include secret fields."""
    server = _server(tmp_path)
    assert server.cursor_secrets is None
    response = asyncio.run(server.handle_status({}))
    cursor = response["cursor"]
    assert "signing_version" not in cursor
    assert "active_versions" not in cursor


def test_status_handler_includes_upstream_connectivity(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    server.upstream_errors["broken"] = "timeout"
    response = asyncio.run(server.handle_status({}))
    upstreams = response["upstreams"]
    connected = [u for u in upstreams if u.get("connected") is True]
    disconnected = [u for u in upstreams if u.get("connected") is False]
    assert len(connected) == 1
    assert connected[0]["prefix"] == "demo"
    assert connected[0]["tool_count"] == 1
    assert len(disconnected) == 1
    assert disconnected[0]["prefix"] == "broken"
    assert disconnected[0]["error"] == "timeout"


def test_cursor_error_updates_metrics(tmp_path: Path) -> None:
    server = GatewayServer(config=GatewayConfig(data_dir=tmp_path), metrics=GatewayMetrics())
    expired = server._cursor_error(CursorExpiredError("expired"))
    assert expired["code"] == "CURSOR_EXPIRED"
    assert server.metrics.cursor_expired.value == 1

    stale = server._cursor_error(CursorStaleError("cursor where_canonicalization_mode mismatch"))
    assert stale["code"] == "CURSOR_STALE"
    assert server.metrics.cursor_stale_where_mode.value == 1

    stale_budget = server._cursor_error(CursorStaleError("cursor map_budget_fingerprint mismatch"))
    assert stale_budget["code"] == "CURSOR_STALE"
    assert server.metrics.cursor_stale_map_budget.value == 1

    invalid = server._cursor_error(ValueError("bad token"))
    assert invalid["code"] == "INVALID_ARGUMENT"
    assert server.metrics.cursor_invalid.value == 1


def test_artifact_handlers_return_validation_or_not_implemented(tmp_path: Path) -> None:
    server = _server(tmp_path)

    invalid_get = asyncio.run(server.handle_artifact_get({}))
    assert invalid_get["code"] == "INVALID_ARGUMENT"

    valid_get = asyncio.run(
        server.handle_artifact_get(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert valid_get["code"] == "NOT_IMPLEMENTED"

    valid_search = asyncio.run(
        server.handle_artifact_search(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "filters": {},
            }
        )
    )
    assert valid_search["code"] == "NOT_IMPLEMENTED"


def test_build_fastmcp_app_includes_mirrored_tools(tmp_path: Path) -> None:
    server = _server_with_upstream(tmp_path)
    app = server.build_fastmcp_app()
    tool_names = set(asyncio.run(app.get_tools()).keys())
    assert "gateway.status" in tool_names
    assert "demo.echo" in tool_names


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


def test_handle_mirrored_tool_success_path_without_db(tmp_path: Path, monkeypatch) -> None:
    server = _server_with_upstream(tmp_path)
    mirrored = server.mirrored_tools["demo.echo"]

    async def _fake_call(_instance, _tool_name, _arguments):
        return {
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {"ok": True},
            "isError": False,
            "meta": {"trace_id": "abc"},
        }

    monkeypatch.setattr("mcp_artifact_gateway.mcp.server.call_upstream_tool", _fake_call)

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
    assert response["artifact_id"].startswith("art_")
    assert response["meta"]["cache"]["reused"] is False


def test_handle_mirrored_tool_returns_cached_artifact_when_reused(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    server = GatewayServer(
        config=config,
        upstreams=[_upstream()],
        db_pool=_FakePool(("art_cached", "payload_hash", "schema_echo", "ready", 3)),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _must_not_call(*_args, **_kwargs):
        raise AssertionError("upstream should not be called on reuse")

    monkeypatch.setattr("mcp_artifact_gateway.mcp.server.call_upstream_tool", _must_not_call)

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
    assert response["artifact_id"] == "art_cached"
    assert response["meta"]["cache"]["reused"] is True
    assert server.metrics.cache_hits.value == 1


def test_handle_mirrored_tool_returns_busy_when_lock_times_out(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    server = GatewayServer(
        config=config,
        upstreams=[_upstream()],
        db_pool=_FakePool(None),  # type: ignore[arg-type]
    )
    mirrored = server.mirrored_tools["demo.echo"]

    async def _lock_fail(*_args, **_kwargs):
        return False

    monkeypatch.setattr("mcp_artifact_gateway.mcp.handlers.mirrored_tool.acquire_advisory_lock_async", _lock_fail)

    async def _must_not_call(*_args, **_kwargs):
        raise AssertionError("upstream should not be called when lock fails")

    monkeypatch.setattr("mcp_artifact_gateway.mcp.server.call_upstream_tool", _must_not_call)

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
    assert response["code"] == "RESOURCE_BUSY"


def test_handle_mirrored_tool_runs_inline_mapping_in_sync_mode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path, mapping_mode="sync"),
        upstreams=[_upstream()],
        db_pool=_FakePool((True,)),  # type: ignore[arg-type]
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

    inline_calls: list[str] = []
    scheduled_calls: list[str] = []

    monkeypatch.setattr("mcp_artifact_gateway.mcp.server.call_upstream_tool", _fake_call)
    monkeypatch.setattr(
        "mcp_artifact_gateway.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kwargs: _persisted_handle(),
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **kwargs: inline_calls.append(kwargs["handle"].artifact_id) or True,
    )
    monkeypatch.setattr(
        server,
        "_schedule_background_mapping",
        lambda **kwargs: scheduled_calls.append(kwargs["handle"].artifact_id),
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {"_gateway_context": {"session_id": "sess_1"}, "message": "hello"},
        )
    )

    assert response["type"] == "gateway_tool_result"
    assert inline_calls == ["art_new"]
    assert scheduled_calls == []
    assert server.metrics.cache_misses.value == 1
    assert server.metrics.upstream_calls.value == 1


def test_handle_mirrored_tool_schedules_background_mapping_in_async_mode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path, mapping_mode="async"),
        upstreams=[_upstream()],
        db_pool=_FakePool((True,)),  # type: ignore[arg-type]
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

    inline_calls: list[str] = []
    scheduled_calls: list[str] = []

    monkeypatch.setattr("mcp_artifact_gateway.mcp.server.call_upstream_tool", _fake_call)
    monkeypatch.setattr(
        "mcp_artifact_gateway.mcp.handlers.mirrored_tool.persist_artifact",
        lambda **_kwargs: _persisted_handle(),
    )
    monkeypatch.setattr(
        server,
        "_run_mapping_inline",
        lambda *_args, **kwargs: inline_calls.append(kwargs["handle"].artifact_id) or True,
    )
    monkeypatch.setattr(
        server,
        "_schedule_background_mapping",
        lambda **kwargs: scheduled_calls.append(kwargs["handle"].artifact_id),
    )

    response = asyncio.run(
        server.handle_mirrored_tool(
            mirrored,
            {"_gateway_context": {"session_id": "sess_1"}, "message": "hello"},
        )
    )

    assert response["type"] == "gateway_tool_result"
    assert inline_calls == []
    assert scheduled_calls == ["art_new"]
    assert server.metrics.upstream_calls.value == 1


def test_artifact_search_db_runtime_returns_items(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setattr(server, "_safe_touch_for_search", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_search(
            {"_gateway_context": {"session_id": "sess_1"}, "filters": {}}
        )
    )
    assert response["truncated"] is False
    assert response["items"][0]["artifact_id"] == "art_1"
    assert conn.committed is True


def test_artifact_get_db_runtime_returns_envelope_items(tmp_path: Path, monkeypatch) -> None:
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
                    "zstd",
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
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_get(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "target": "envelope",
            }
        )
    )
    assert response["target"] == "envelope"
    assert response["items"][0]["type"] == "mcp_envelope"
    assert conn.committed is True


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
                    "zstd",
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
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact_get(
            {
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
    extra = issued["extra"]
    assert isinstance(extra, dict)
    assert extra["target"] == "envelope"
    assert extra["normalized_jsonpath"] == "$.items[*]"
    assert extra["artifact_generation"] == 1


def test_artifact_get_cursor_target_mismatch_returns_stale(tmp_path: Path, monkeypatch) -> None:
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
                    "zstd",
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
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)
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
        server.handle_artifact_get(
            {
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


def test_artifact_describe_db_runtime_returns_roots(tmp_path: Path, monkeypatch) -> None:
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
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_describe(
            {"_gateway_context": {"session_id": "sess_1"}, "artifact_id": "art_1"}
        )
    )
    assert response["artifact_id"] == "art_1"
    assert response["roots"][0]["root_path"] == "$.items"
    assert response["roots"][0]["sampled_prefix_len"] == 7
    assert response["roots"][0]["prefix_coverage"] is True
    assert response["roots"][0]["stop_reason"] == "max_compute"


def test_artifact_select_db_runtime_partial_projects_records(tmp_path: Path, monkeypatch) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
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
            _SeqCursor(all_rows=[(0, {"id": 1, "name": "A"}, 16, "h1")]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_select(
            {
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


def test_artifact_select_cursor_sample_set_mismatch_returns_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
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
                all_rows=[
                    (0, {"id": 1}, 10, "h1"),
                    (1, {"id": 2}, 10, "h2"),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    where_expr = {"path": "$.id", "op": "eq", "value": 1}
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "root_path": "$.items",
            "select_paths_hash": select_paths_hash(["$.id"]),
            "where_hash": where_hash(
                where_expr,
                mode=server.config.where_canonicalization_mode.value,
            ),
            "artifact_generation": 1,
            "map_budget_fingerprint": "mbf",
            "sample_set_hash": "bad_hash",
        },
    )

    response = asyncio.run(
        server.handle_artifact_select(
            {
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
    assert server.metrics.cursor_stale_sample_set.value == 1


def test_artifact_select_cursor_includes_partial_binding_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
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
                all_rows=[
                    (0, {"id": 1}, 10, "h1"),
                    (1, {"id": 2}, 10, "h2"),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)
    issued: dict[str, object] = {}

    def _issue_cursor(**kwargs):
        issued.update(kwargs)
        return "cur_next"

    monkeypatch.setattr(server, "_issue_cursor", _issue_cursor)

    response = asyncio.run(
        server.handle_artifact_select(
            {
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

    extra = issued["extra"]
    assert isinstance(extra, dict)
    assert extra["root_path"] == "$.items"
    assert extra["select_paths_hash"] == select_paths_hash(["$.id"])
    assert extra["where_hash"] == "__none__"
    assert extra["artifact_generation"] == 1
    assert extra["map_budget_fingerprint"] == "mbf"
    assert extra["sample_set_hash"] == compute_sample_set_hash(
        root_path="$.items",
        sample_indices=[0, 1],
        map_budget_fingerprint="mbf",
    )


def test_artifact_find_db_runtime_filters_samples(tmp_path: Path, monkeypatch) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
            _SeqCursor(
                all_rows=[
                    (
                        "rk_1",
                        "$.items",
                        2,
                        1.0,
                        {"n": 2},
                        1.0,
                        "array",
                        {"id": {"number": 2}},
                        [0, 1],
                    )
                ]
            ),
            _SeqCursor(
                all_rows=[
                    ("rk_1", 0, {"id": 1}, 10, "h1"),
                    ("rk_1", 1, {"id": 2}, 10, "h2"),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_find(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "where": {"path": "$.id", "op": "eq", "value": 2},
            }
        )
    )
    assert len(response["items"]) == 1
    assert response["items"][0]["sample_index"] == 1


def test_artifact_find_cursor_map_budget_mismatch_returns_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
        metrics=GatewayMetrics(),
    )
    monkeypatch.setattr(
        server,
        "_verify_cursor_payload",
        lambda **_kwargs: {
            "position_state": {"offset": 0},
            "root_path_filter": "__any__",
            "where_hash": "__none__",
            "artifact_generation": 1,
            "map_budget_fingerprint": "old_mbf",
        },
    )

    response = asyncio.run(
        server.handle_artifact_find(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "cursor": "cursor_1",
            }
        )
    )
    assert response["code"] == "CURSOR_STALE"
    assert "map_budget_fingerprint mismatch" in response["message"]
    assert server.metrics.cursor_stale_map_budget.value == 1


def test_artifact_chain_pages_db_runtime_returns_cursor_when_truncated(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            _SeqCursor(one=(1,)),
            _SeqCursor(
                all_rows=[
                    (
                        "art_page_1",
                        10,
                        "2026-01-01T00:00:00Z",
                        0,
                        "demo.echo",
                        100,
                        "none",
                        "pending",
                    ),
                    (
                        "art_page_2",
                        11,
                        "2026-01-01T00:00:01Z",
                        1,
                        "demo.echo",
                        100,
                        "none",
                        "pending",
                    ),
                ]
            ),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_search", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_issue_cursor", lambda *args, **kwargs: "cur_next")

    response = asyncio.run(
        server.handle_artifact_chain_pages(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "parent_artifact_id": "art_parent",
                "limit": 1,
            }
        )
    )
    assert response["truncated"] is True
    assert response["cursor"] == "cur_next"
    assert response["items"][0]["artifact_id"] == "art_page_1"


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
    assert GatewayServer._check_sample_corruption(root_row, sample_rows) is None


def test_check_sample_corruption_returns_none_when_no_expected_indices() -> None:
    root_row = {"root_key": "rk_1", "sample_indices": None}
    assert GatewayServer._check_sample_corruption(root_row, []) is None

    root_row_empty = {"root_key": "rk_1", "sample_indices": []}
    assert GatewayServer._check_sample_corruption(root_row_empty, []) is None


def test_check_sample_corruption_returns_internal_when_rows_missing() -> None:
    root_row = {"root_key": "rk_1", "sample_indices": [0, 3, 7]}
    sample_rows = [{"sample_index": 0}]  # indices 3 and 7 missing
    result = GatewayServer._check_sample_corruption(root_row, sample_rows)
    assert result is not None
    assert result["code"] == "INTERNAL"
    assert "corruption" in result["message"]
    assert result["details"]["missing_indices"] == [3, 7]
    assert result["details"]["expected_count"] == 3
    assert result["details"]["actual_count"] == 1


def test_artifact_select_returns_internal_on_sample_corruption(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # root says sample_indices=[0, 1] but only sample_index=0 exists
    conn = _SeqConnection(
        [
            # artifact_visible
            _SeqCursor(one=(1,)),
            # artifact_meta
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
            # root_row with sample_indices=[0, 1]
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
            # sample_rows: only index 0 present
            _SeqCursor(all_rows=[(0, {"id": 1}, 8, "h0")]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_select(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
                "root_path": "$.items",
                "select_paths": ["id"],
            }
        )
    )
    assert response["code"] == "INTERNAL"
    assert response["details"]["missing_indices"] == [1]


def test_artifact_find_returns_internal_on_sample_corruption(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn = _SeqConnection(
        [
            # artifact_visible
            _SeqCursor(one=(1,)),
            # artifact_meta
            _SeqCursor(one=("art_1", "partial", "ready", "off", None, 1, "mbf")),
            # roots: one root with sample_indices=[0, 2]
            # columns: root_key, root_path, count_estimate, inventory_coverage,
            #          root_summary, root_score, root_shape, fields_top, sample_indices
            _SeqCursor(
                all_rows=[
                    ("rk_1", "$.data", 50, None, None, None, None, None, [0, 2])
                ]
            ),
            # batch sample_rows: root_key, sample_index, record, record_bytes, record_hash
            # only index 0 present (index 2 missing)
            _SeqCursor(all_rows=[("rk_1", 0, {"x": 1}, 8, "h0")]),
        ]
    )
    server = GatewayServer(
        config=GatewayConfig(data_dir=tmp_path),
        db_pool=_SeqPool(conn),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server, "_safe_touch_for_retrieval", lambda *args, **kwargs: None)

    response = asyncio.run(
        server.handle_artifact_find(
            {
                "_gateway_context": {"session_id": "sess_1"},
                "artifact_id": "art_1",
            }
        )
    )
    assert response["code"] == "INTERNAL"
    assert response["details"]["missing_indices"] == [2]
