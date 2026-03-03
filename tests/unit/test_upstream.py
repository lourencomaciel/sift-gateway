from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest

from sift_gateway.config.settings import UpstreamConfig
from sift_gateway.config.upstream_secrets import write_secret
from sift_gateway.mcp.upstream import (
    UpstreamInstance,
    _atomic_json_write,
    _build_http_headers,
    _build_stdio_env,
    _client_result_content,
    _client_transport,
    _effective_external_user_id,
    _file_lock,
    call_upstream_tool,
    compute_auth_fingerprint,
    compute_upstream_instance_id,
    connect_upstream,
    connect_upstreams,
    discover_tools,
    resolve_external_user_id,
)


def _stdio_config(**overrides) -> UpstreamConfig:
    defaults = {
        "prefix": "github",
        "transport": "stdio",
        "command": "/usr/bin/github-mcp",
        "args": ["--mode", "prod"],
        "env": {"GITHUB_TOKEN": "secret123", "GITHUB_ORG": "acme"},
    }
    defaults.update(overrides)
    return UpstreamConfig(**defaults)


def _http_config(**overrides) -> UpstreamConfig:
    defaults = {
        "prefix": "jira",
        "transport": "http",
        "url": "https://jira.example.com/mcp",
        "headers": {"Authorization": "Bearer tok", "X-Org": "acme"},
    }
    defaults.update(overrides)
    return UpstreamConfig(**defaults)


# ---- determinism ----


def test_instance_id_deterministic_stdio() -> None:
    cfg = _stdio_config()
    id1 = compute_upstream_instance_id(cfg)
    id2 = compute_upstream_instance_id(cfg)
    assert id1 == id2
    assert len(id1) == 32


def test_instance_id_deterministic_http() -> None:
    cfg = _http_config()
    id1 = compute_upstream_instance_id(cfg)
    id2 = compute_upstream_instance_id(cfg)
    assert id1 == id2
    assert len(id1) == 32


# ---- different configs yield different ids ----


def test_different_prefix_different_id() -> None:
    id1 = compute_upstream_instance_id(_stdio_config(prefix="github"))
    id2 = compute_upstream_instance_id(_stdio_config(prefix="gitlab"))
    assert id1 != id2


def test_different_command_different_id() -> None:
    id1 = compute_upstream_instance_id(
        _stdio_config(command="/usr/bin/github-mcp")
    )
    id2 = compute_upstream_instance_id(
        _stdio_config(command="/usr/local/bin/github-mcp")
    )
    assert id1 != id2


def test_different_url_different_id() -> None:
    id1 = compute_upstream_instance_id(
        _http_config(url="https://a.example.com/mcp")
    )
    id2 = compute_upstream_instance_id(
        _http_config(url="https://b.example.com/mcp")
    )
    assert id1 != id2


# ---- secrets excluded ----


def test_secret_env_excluded_from_instance_id() -> None:
    """Changing a secret (non-salt) env var must NOT change the instance id."""
    id1 = compute_upstream_instance_id(
        _stdio_config(env={"GITHUB_TOKEN": "secret_A", "GITHUB_ORG": "acme"})
    )
    id2 = compute_upstream_instance_id(
        _stdio_config(env={"GITHUB_TOKEN": "secret_B", "GITHUB_ORG": "acme"})
    )
    assert id1 == id2


def test_secret_header_excluded_from_instance_id() -> None:
    """Changing a secret (non-salt) header must NOT change the instance id."""
    id1 = compute_upstream_instance_id(
        _http_config(headers={"Authorization": "Bearer A", "X-Org": "acme"})
    )
    id2 = compute_upstream_instance_id(
        _http_config(headers={"Authorization": "Bearer B", "X-Org": "acme"})
    )
    assert id1 == id2


# ---- semantic salt included ----


def test_semantic_salt_env_included() -> None:
    """Semantic salt env values SHOULD change the instance id."""
    id1 = compute_upstream_instance_id(
        _stdio_config(
            env={"GITHUB_TOKEN": "tok", "GITHUB_ORG": "acme"},
            semantic_salt_env_keys=["GITHUB_ORG"],
        )
    )
    id2 = compute_upstream_instance_id(
        _stdio_config(
            env={"GITHUB_TOKEN": "tok", "GITHUB_ORG": "other_org"},
            semantic_salt_env_keys=["GITHUB_ORG"],
        )
    )
    assert id1 != id2


def test_semantic_salt_header_included() -> None:
    """Semantic salt header values SHOULD change the instance id."""
    id1 = compute_upstream_instance_id(
        _http_config(
            headers={"Authorization": "Bearer tok", "X-Org": "acme"},
            semantic_salt_headers=["X-Org"],
        )
    )
    id2 = compute_upstream_instance_id(
        _http_config(
            headers={"Authorization": "Bearer tok", "X-Org": "other"},
            semantic_salt_headers=["X-Org"],
        )
    )
    assert id1 != id2


# ---- auth fingerprint ----


def test_auth_fingerprint_none_when_no_secret_values() -> None:
    cfg = _stdio_config(env={})
    assert compute_auth_fingerprint(cfg) is None


def test_auth_fingerprint_excludes_salt_keys() -> None:
    cfg = _stdio_config(
        env={"GITHUB_ORG": "acme"},
        semantic_salt_env_keys=["GITHUB_ORG"],
    )
    # All env keys are salt, so no auth fingerprint
    assert compute_auth_fingerprint(cfg) is None


def test_auth_fingerprint_returns_string_for_secrets() -> None:
    cfg = _stdio_config(env={"GITHUB_TOKEN": "secret"})
    fp = compute_auth_fingerprint(cfg)
    assert isinstance(fp, str)
    assert len(fp) == 16


def test_auth_fingerprint_http_none_when_only_salt_headers() -> None:
    cfg = _http_config(
        headers={"X-Org": "acme"},
        semantic_salt_headers=["X-Org"],
    )
    assert compute_auth_fingerprint(cfg) is None


def test_auth_fingerprint_uses_secret_env_for_stdio(tmp_path) -> None:
    cfg = _stdio_config(
        env={},
        secret_ref="gh",
        semantic_salt_env_keys=[],
    )
    write_secret(
        tmp_path,
        "gh",
        transport="stdio",
        env={"TOKEN": "abc"},
    )

    fp = compute_auth_fingerprint(cfg, str(tmp_path))
    assert isinstance(fp, str)
    assert len(fp) == 16


def test_file_lock_noop_when_fcntl_unavailable(monkeypatch, tmp_path) -> None:
    marker = tmp_path / "marker.txt"
    monkeypatch.setattr("sift_gateway.mcp.upstream._fcntl", None)

    with _file_lock(tmp_path / "lockfile.lock"):
        marker.write_text("ok", encoding="utf-8")

    assert marker.read_text(encoding="utf-8") == "ok"


def test_atomic_json_write_cleans_tmp_file_on_write_error(
    monkeypatch, tmp_path
) -> None:
    dest = tmp_path / "ids.json"

    def _fail_write(_fd: int, _data: bytes) -> int:
        raise OSError("disk full")

    monkeypatch.setattr("sift_gateway.mcp.upstream.os.write", _fail_write)

    with pytest.raises(OSError, match="disk full"):
        _atomic_json_write(dest, {"a": "b"})

    assert not dest.exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_effective_external_user_id_returns_none_for_malformed_flag() -> None:
    assert _effective_external_user_id(["--external-user-id"], "resolved") is None


def test_effective_external_user_id_returns_none_for_empty_equals() -> None:
    assert (
        _effective_external_user_id(["--external-user-id="], "resolved")
        is None
    )


def test_http_transport_returns_raw_url_without_headers_or_oauth() -> None:
    cfg = _http_config(headers={})
    assert _client_transport(cfg) == "https://jira.example.com/mcp"


def test_client_result_content_returns_empty_when_content_not_list() -> None:
    class _Result:
        content = "not-a-list"

    assert _client_result_content(_Result()) == []


def test_client_result_content_handles_dict_model_and_fallback() -> None:
    class _ModelBlock:
        def model_dump(self, *, by_alias: bool, exclude_none: bool) -> dict:
            _ = by_alias, exclude_none
            return {"type": "model", "value": 1}

    class _Result:
        def __init__(self) -> None:
            self.content = [
                {"type": "text", "text": "ok"},
                _ModelBlock(),
                123,
            ]

    assert _client_result_content(_Result()) == [
        {"type": "text", "text": "ok"},
        {"type": "model", "value": 1},
        {"type": "text", "text": "123"},
    ]


class _FakeTool:
    def __init__(self, name: str, description: str, schema: dict) -> None:
        self.name = name
        self.description = description
        self.inputSchema = schema


class _FakeContentBlock:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def model_dump(
        self, *, by_alias: bool = False, exclude_none: bool = False
    ) -> dict:
        return dict(self._payload)


class _FakeCallResult:
    def __init__(self) -> None:
        self.content = [_FakeContentBlock({"type": "text", "text": "ok"})]
        self.structured_content = {"value": 1}
        self.is_error = False
        self.meta = {"trace_id": "t-1"}


class _FakeClient:
    instances: ClassVar[list[_FakeClient]] = []
    tools: ClassVar[list[_FakeTool]] = []

    def __init__(self, transport, timeout: float | None = None) -> None:
        self.transport = transport
        self.timeout = timeout
        self.calls: list[tuple[str, dict]] = []
        _FakeClient.instances.append(self)

    async def __aenter__(self) -> _FakeClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def list_tools(self) -> list[_FakeTool]:
        return list(_FakeClient.tools)

    async def call_tool(self, name: str, arguments: dict) -> _FakeCallResult:
        self.calls.append((name, dict(arguments)))
        return _FakeCallResult()


@pytest.mark.asyncio
async def test_discover_tools_fetches_and_hashes_tool_schemas(
    monkeypatch,
) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [
        _FakeTool(
            "list_issues",
            "List issues",
            {"type": "object", "properties": {"repo": {"type": "string"}}},
        ),
        _FakeTool(
            "list_prs",
            "List pull requests",
            {"type": "object", "properties": {}},
        ),
    ]
    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _stdio_config()
    tools = await discover_tools(cfg)

    assert [tool.name for tool in tools] == ["list_issues", "list_prs"]
    assert all(len(tool.schema_hash) == 32 for tool in tools)
    assert _FakeClient.instances
    created = _FakeClient.instances[0]
    from fastmcp.client.transports import StdioTransport

    assert isinstance(created.transport, StdioTransport)
    assert created.transport.command == cfg.command
    assert created.transport.args == cfg.args


@pytest.mark.asyncio
async def test_call_upstream_tool_normalizes_result(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = []
    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _http_config()
    instance = UpstreamInstance(config=cfg, instance_id="inst1", tools=[])

    result = await call_upstream_tool(instance, "tool_a", {"x": 1})

    assert result["isError"] is False
    assert result["structuredContent"] == {"value": 1}
    assert result["content"] == [{"type": "text", "text": "ok"}]
    assert result["meta"] == {"trace_id": "t-1"}
    created = _FakeClient.instances[0]
    # HTTP configs with headers produce StreamableHttpTransport
    from fastmcp.client.transports import StreamableHttpTransport

    assert isinstance(created.transport, StreamableHttpTransport)
    assert created.transport.url == cfg.url
    assert created.calls == [("tool_a", {"x": 1})]


@pytest.mark.asyncio
async def test_call_upstream_tool_retries_once_after_oauth_auth_failure(
    monkeypatch,
) -> None:
    seen: dict[str, object] = {"call_count": 0, "stale_calls": 0}

    class _RetryClient:
        def __init__(self, transport, timeout: float | None = None) -> None:
            _ = (transport, timeout)

        async def __aenter__(self) -> _RetryClient:
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def call_tool(self, name: str, arguments: dict) -> _FakeCallResult:
            _ = (name, arguments)
            call_count = int(seen["call_count"])
            seen["call_count"] = call_count + 1
            if call_count == 0:
                raise RuntimeError("401 unauthorized")
            return _FakeCallResult()

    async def _mark_stale(**_kwargs) -> bool:
        stale_calls = int(seen["stale_calls"])
        seen["stale_calls"] = stale_calls + 1
        return True

    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _RetryClient)
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._client_transport",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._mark_runtime_oauth_access_token_stale",
        _mark_stale,
    )

    cfg = _http_config()
    instance = UpstreamInstance(
        config=cfg,
        instance_id="inst1",
        tools=[],
        secret_data={"oauth": {"enabled": True}},
    )
    result = await call_upstream_tool(instance, "tool_a", {"x": 1})

    assert result["isError"] is False
    assert seen["call_count"] == 2
    assert seen["stale_calls"] == 1


@pytest.mark.asyncio
async def test_call_upstream_tool_no_retry_when_no_refresh_capability(
    monkeypatch,
) -> None:
    seen: dict[str, int] = {"call_count": 0}

    class _FailClient:
        def __init__(self, transport, timeout: float | None = None) -> None:
            _ = (transport, timeout)

        async def __aenter__(self) -> _FailClient:
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def call_tool(self, name: str, arguments: dict) -> _FakeCallResult:
            _ = (name, arguments)
            seen["call_count"] += 1
            raise RuntimeError("401 unauthorized")

    async def _mark_stale(**_kwargs) -> bool:
        return False

    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FailClient)
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._client_transport",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._mark_runtime_oauth_access_token_stale",
        _mark_stale,
    )

    cfg = _http_config()
    instance = UpstreamInstance(
        config=cfg,
        instance_id="inst1",
        tools=[],
        secret_data={"oauth": {"enabled": True}},
    )
    with pytest.raises(RuntimeError, match="401 unauthorized"):
        await call_upstream_tool(instance, "tool_a", {"x": 1})

    assert seen["call_count"] == 1


@pytest.mark.asyncio
async def test_connect_upstream_builds_instance(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [_FakeTool("search", "Search", {"type": "object"})]
    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _stdio_config()
    instance = await connect_upstream(cfg)

    assert instance.config is cfg
    assert len(instance.instance_id) == 32
    assert [tool.name for tool in instance.tools] == ["search"]


@pytest.mark.asyncio
async def test_connect_upstreams_preserves_config_order(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [_FakeTool("search", "Search", {"type": "object"})]
    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FakeClient)

    cfg1 = _stdio_config(prefix="gh")
    cfg2 = _http_config(prefix="jira")

    upstreams = await connect_upstreams([cfg1, cfg2])

    assert [u.prefix for u in upstreams] == ["gh", "jira"]


# ---- stdio env isolation ----


def test_stdio_env_excludes_arbitrary_parent_env(
    monkeypatch,
) -> None:
    """Arbitrary parent env vars must not leak to upstreams."""
    monkeypatch.setenv("SIFT_TEST_SECRET", "hidden")
    cfg = _stdio_config(env={})
    env = _build_stdio_env(cfg)
    assert "SIFT_TEST_SECRET" not in env


def test_stdio_env_includes_allowlisted_keys(
    monkeypatch,
) -> None:
    """Allowlisted parent env vars (PATH, HOME) appear."""
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("HOME", "/home/user")
    cfg = _stdio_config(env={})
    env = _build_stdio_env(cfg)
    assert env["PATH"] == "/usr/bin"
    assert env["HOME"] == "/home/user"


def test_stdio_env_config_env_overrides_base(
    monkeypatch,
) -> None:
    """Explicit config.env values override allowlisted base."""
    monkeypatch.setenv("PATH", "/original")
    cfg = _stdio_config(env={"PATH": "/custom"})
    env = _build_stdio_env(cfg)
    assert env["PATH"] == "/custom"


def test_stdio_env_inherit_parent_env_true(
    monkeypatch,
) -> None:
    """inherit_parent_env=True passes all parent env vars."""
    monkeypatch.setenv("SIFT_TEST_SECRET", "visible")
    cfg = _stdio_config(env={}, inherit_parent_env=True)
    env = _build_stdio_env(cfg)
    assert env.get("SIFT_TEST_SECRET") == "visible"


def test_stdio_env_merges_secret_env(tmp_path) -> None:
    cfg = _stdio_config(
        env={"LOCAL_FLAG": "1"},
        secret_ref="github",
    )
    write_secret(
        tmp_path,
        "github",
        transport="stdio",
        env={"FROM_SECRET": "abc"},
    )

    env = _build_stdio_env(cfg, str(tmp_path))
    assert env["FROM_SECRET"] == "abc"
    assert env["LOCAL_FLAG"] == "1"


def test_stdio_env_ignores_non_dict_secret_env() -> None:
    cfg = _stdio_config(env={"LOCAL_FLAG": "1"})
    env = _build_stdio_env(cfg, secret={"env": "not-a-dict"})
    assert env["LOCAL_FLAG"] == "1"


@pytest.mark.asyncio
async def test_stdio_transport_always_gets_env_dict(
    monkeypatch,
) -> None:
    """Stdio transport env is always a dict, never None."""
    _FakeClient.instances.clear()
    _FakeClient.tools = []
    monkeypatch.setattr("sift_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _stdio_config(env={})
    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg)

    from fastmcp.client.transports import StdioTransport

    assert isinstance(transport, StdioTransport)
    assert isinstance(transport.env, dict)


# ---- external_user_id ----


def test_resolve_external_user_id_none() -> None:
    """None field returns None — no injection."""
    cfg = _stdio_config(external_user_id=None)
    assert resolve_external_user_id(cfg) is None


def test_resolve_external_user_id_explicit() -> None:
    """Explicit value returned verbatim."""
    cfg = _stdio_config(external_user_id="my-user")
    assert resolve_external_user_id(cfg) == "my-user"


def test_resolve_external_user_id_auto_generates(
    tmp_path,
) -> None:
    """Auto mode generates a UUID and persists it."""
    data_dir = str(tmp_path)
    (tmp_path / "state").mkdir()
    cfg = _stdio_config(external_user_id="auto")

    uid = resolve_external_user_id(cfg, data_dir)
    assert uid is not None
    assert len(uid) == 36  # UUID4 format

    # File was written
    import json

    path = tmp_path / "state" / "upstream_user_ids.json"
    assert path.exists()
    stored = json.loads(path.read_text())
    assert stored["github"] == uid


def test_resolve_external_user_id_auto_stable(
    tmp_path,
) -> None:
    """Auto mode returns same ID across calls."""
    data_dir = str(tmp_path)
    (tmp_path / "state").mkdir()
    cfg = _stdio_config(external_user_id="auto")

    uid1 = resolve_external_user_id(cfg, data_dir)
    uid2 = resolve_external_user_id(cfg, data_dir)
    assert uid1 == uid2


def test_resolve_external_user_id_auto_per_prefix(
    tmp_path,
) -> None:
    """Different prefixes get different UUIDs."""
    data_dir = str(tmp_path)
    (tmp_path / "state").mkdir()
    cfg_a = _stdio_config(prefix="alpha", external_user_id="auto")
    cfg_b = _stdio_config(prefix="beta", external_user_id="auto")

    uid_a = resolve_external_user_id(cfg_a, data_dir)
    uid_b = resolve_external_user_id(cfg_b, data_dir)
    assert uid_a != uid_b


def test_resolve_external_user_id_corrupt_file(
    tmp_path,
) -> None:
    """Corrupt JSON file is recovered gracefully."""
    data_dir = str(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    path = state / "upstream_user_ids.json"
    path.write_text("{truncated", encoding="utf-8")

    cfg = _stdio_config(external_user_id="auto")
    uid = resolve_external_user_id(cfg, data_dir)
    assert uid is not None
    assert len(uid) == 36

    import json

    stored = json.loads(path.read_text())
    assert stored["github"] == uid


def test_resolve_external_user_id_non_utf8_file(
    tmp_path,
) -> None:
    """Non-UTF-8 file is recovered gracefully."""
    data_dir = str(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    path = state / "upstream_user_ids.json"
    path.write_bytes(b"\xff\xfe invalid utf-8")

    cfg = _stdio_config(external_user_id="auto")
    uid = resolve_external_user_id(cfg, data_dir)
    assert uid is not None
    assert len(uid) == 36

    import json

    stored = json.loads(path.read_text())
    assert stored["github"] == uid


def test_resolve_external_user_id_wrong_type_in_file(
    tmp_path,
) -> None:
    """Non-dict JSON content is recovered gracefully."""
    data_dir = str(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    path = state / "upstream_user_ids.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    cfg = _stdio_config(external_user_id="auto")
    uid = resolve_external_user_id(cfg, data_dir)
    assert uid is not None

    import json

    stored = json.loads(path.read_text())
    assert isinstance(stored, dict)
    assert stored["github"] == uid


def test_resolve_external_user_id_non_string_stored_value(
    tmp_path,
) -> None:
    """Non-string stored value for prefix is regenerated."""
    import json

    data_dir = str(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    path = state / "upstream_user_ids.json"
    path.write_text(json.dumps({"github": 123}), encoding="utf-8")

    cfg = _stdio_config(external_user_id="auto")
    uid = resolve_external_user_id(cfg, data_dir)
    assert uid is not None
    assert isinstance(uid, str)
    assert len(uid) == 36  # Valid UUID4

    stored = json.loads(path.read_text())
    assert stored["github"] == uid


def test_resolve_external_user_id_rechecks_under_lock(
    monkeypatch, tmp_path
) -> None:
    cfg = _stdio_config(external_user_id="auto")
    values = iter([{}, {"github": "already-written"}])

    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._read_user_ids",
        lambda _path: next(values),
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._atomic_json_write",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not write when value appears under lock")
        ),
    )

    uid = resolve_external_user_id(cfg, str(tmp_path))
    assert uid == "already-written"


def test_instance_id_varies_with_external_user_id() -> None:
    """Different external_user_id values produce different instance IDs."""
    cfg = _stdio_config()

    id_none = compute_upstream_instance_id(cfg, resolved_user_id=None)
    id_a = compute_upstream_instance_id(cfg, resolved_user_id="user-a")
    id_b = compute_upstream_instance_id(cfg, resolved_user_id="user-b")

    # None keeps baseline; explicit values diverge from each other
    assert id_none != id_a
    assert id_a != id_b


def test_stdio_transport_injects_external_user_id(
    tmp_path,
) -> None:
    """Args include --external-user-id when field is set."""
    data_dir = str(tmp_path)
    (tmp_path / "state").mkdir()
    cfg = _stdio_config(external_user_id="test-user")

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg, data_dir)
    assert "--external-user-id" in transport.args
    idx = transport.args.index("--external-user-id")
    assert transport.args[idx + 1] == "test-user"


def test_stdio_transport_uses_pre_resolved_user_id(
    tmp_path,
) -> None:
    """Transport uses resolved_user_id kwarg without re-resolving."""
    data_dir = str(tmp_path)
    (tmp_path / "state").mkdir()
    cfg = _stdio_config(external_user_id="auto")

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg, data_dir, resolved_user_id="pinned-id")
    idx = transport.args.index("--external-user-id")
    assert transport.args[idx + 1] == "pinned-id"


def test_stdio_transport_no_duplicate_flag_separate() -> None:
    """No duplicate when --external-user-id <val> already in args."""
    cfg = _stdio_config(
        args=["--mode", "prod", "--external-user-id", "existing"],
        external_user_id="other-value",
    )

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg)
    count = transport.args.count("--external-user-id")
    assert count == 1
    idx = transport.args.index("--external-user-id")
    assert transport.args[idx + 1] == "existing"


def test_stdio_transport_no_duplicate_flag_equals() -> None:
    """No duplicate when --external-user-id=val already in args."""
    cfg = _stdio_config(
        args=["--external-user-id=existing"],
        external_user_id="other-value",
    )

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg)
    assert len(transport.args) == 1
    assert transport.args[0] == "--external-user-id=existing"


def test_instance_id_matches_effective_user_id() -> None:
    """Identity hash uses the args value when args already have the flag."""
    cfg_via_args = _stdio_config(
        args=["--external-user-id", "from-args"],
        external_user_id="from-config",
    )
    cfg_direct = _stdio_config(
        args=["--external-user-id", "from-args"],
    )

    # Both should hash the same — "from-args" is effective in both
    id_with_config = compute_upstream_instance_id(
        cfg_via_args, resolved_user_id="from-config"
    )
    id_without_config = compute_upstream_instance_id(
        cfg_direct, resolved_user_id=None
    )
    assert id_with_config == id_without_config


def test_stdio_transport_no_injection_when_none() -> None:
    """Args unchanged when external_user_id is None."""
    cfg = _stdio_config(external_user_id=None)

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg)
    assert "--external-user-id" not in transport.args
    assert transport.args == ["--mode", "prod"]


def test_build_http_headers_ignores_non_dict_secret_headers() -> None:
    cfg = _http_config(headers={"X-Org": "acme"})
    headers = _build_http_headers(cfg, secret={"headers": "invalid"})
    assert headers == {"X-Org": "acme"}


def test_http_transport_oauth_mode_uses_auth_and_drops_authorization(
    tmp_path,
    monkeypatch,
) -> None:
    cfg = _http_config(
        prefix="notion",
        headers={"Authorization": "Bearer inline", "X-Org": "acme"},
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer secret", "X-Extra": "1"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )
    sentinel_auth = object()
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._build_runtime_oauth_auth",
        lambda *_args, **_kwargs: sentinel_auth,
    )

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg, str(tmp_path))

    from fastmcp.client.transports import StreamableHttpTransport

    assert isinstance(transport, StreamableHttpTransport)
    assert transport.auth is sentinel_auth
    assert "Authorization" not in transport.headers
    assert transport.headers["X-Org"] == "acme"
    assert transport.headers["X-Extra"] == "1"


def test_http_transport_without_oauth_mode_keeps_authorization(
    tmp_path,
) -> None:
    cfg = _http_config(
        prefix="notion",
        headers={"Authorization": "Bearer inline"},
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer secret"},
    )

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg, str(tmp_path))

    from fastmcp.client.transports import StreamableHttpTransport

    assert isinstance(transport, StreamableHttpTransport)
    assert transport.auth is None
    assert transport.headers["Authorization"] == "Bearer inline"


def test_build_runtime_oauth_auth_returns_none_without_oauth_metadata(
    tmp_path,
) -> None:
    cfg = _http_config(
        prefix="notion",
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer tok"},
    )

    from sift_gateway.mcp.upstream import _build_runtime_oauth_auth

    assert _build_runtime_oauth_auth(cfg, str(tmp_path)) is None


def test_build_runtime_oauth_auth_requires_url(tmp_path) -> None:
    cfg = _stdio_config(
        prefix="notion",
        env={},
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer tok"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    from sift_gateway.mcp.upstream import _build_runtime_oauth_auth

    assert _build_runtime_oauth_auth(cfg, str(tmp_path)) is None


def test_build_runtime_oauth_auth_redirect_handler_is_non_interactive(
    tmp_path,
) -> None:
    cfg = _http_config(
        prefix="notion",
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer tok"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    from sift_gateway.mcp.upstream import _build_runtime_oauth_auth

    auth = _build_runtime_oauth_auth(cfg, str(tmp_path))
    assert auth is not None
    with pytest.raises(RuntimeError, match="requires interactive login"):
        asyncio.run(auth.redirect_handler("https://auth.example.test/start"))


def test_http_transport_oauth_mode_uses_sse_transport(
    tmp_path,
    monkeypatch,
) -> None:
    cfg = _http_config(
        prefix="notion",
        url="https://example.com/sse",
        headers={"Authorization": "Bearer inline"},
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer secret"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "sse",
    )
    sentinel_auth = object()
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._build_runtime_oauth_auth",
        lambda *_args, **_kwargs: sentinel_auth,
    )

    from sift_gateway.mcp.upstream import _client_transport

    transport = _client_transport(cfg, str(tmp_path))

    from fastmcp.client.transports import SSETransport

    assert isinstance(transport, SSETransport)
    assert transport.auth is sentinel_auth
    assert "Authorization" not in transport.headers


def test_auth_fingerprint_oauth_mode_ignores_authorization_header(
    tmp_path,
) -> None:
    cfg_a = _http_config(
        prefix="notion",
        headers={"Authorization": "Bearer inline_A"},
        secret_ref="notion",
    )
    cfg_b = _http_config(
        prefix="notion",
        headers={"Authorization": "Bearer inline_B"},
        secret_ref="notion",
    )
    write_secret(
        tmp_path,
        "notion",
        transport="http",
        headers={"Authorization": "Bearer secret"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    fp_a = compute_auth_fingerprint(cfg_a, str(tmp_path))
    fp_b = compute_auth_fingerprint(cfg_b, str(tmp_path))
    assert fp_a == fp_b
    assert isinstance(fp_a, str)
