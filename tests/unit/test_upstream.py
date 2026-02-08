from __future__ import annotations

import pytest

from mcp_artifact_gateway.config.settings import UpstreamConfig
from mcp_artifact_gateway.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstream,
    connect_upstreams,
    compute_auth_fingerprint,
    compute_upstream_instance_id,
    discover_tools,
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
    id1 = compute_upstream_instance_id(_stdio_config(command="/usr/bin/github-mcp"))
    id2 = compute_upstream_instance_id(_stdio_config(command="/usr/local/bin/github-mcp"))
    assert id1 != id2


def test_different_url_different_id() -> None:
    id1 = compute_upstream_instance_id(_http_config(url="https://a.example.com/mcp"))
    id2 = compute_upstream_instance_id(_http_config(url="https://b.example.com/mcp"))
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


class _FakeTool:
    def __init__(self, name: str, description: str, schema: dict) -> None:
        self.name = name
        self.description = description
        self.inputSchema = schema


class _FakeContentBlock:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def model_dump(self, *, by_alias: bool = False, exclude_none: bool = False) -> dict:
        return dict(self._payload)


class _FakeCallResult:
    def __init__(self) -> None:
        self.content = [_FakeContentBlock({"type": "text", "text": "ok"})]
        self.structured_content = {"value": 1}
        self.is_error = False
        self.meta = {"trace_id": "t-1"}


class _FakeClient:
    instances: list["_FakeClient"] = []
    tools: list[_FakeTool] = []

    def __init__(self, transport, timeout: float | None = None) -> None:
        self.transport = transport
        self.timeout = timeout
        self.calls: list[tuple[str, dict]] = []
        _FakeClient.instances.append(self)

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def list_tools(self) -> list[_FakeTool]:
        return list(_FakeClient.tools)

    async def call_tool(self, name: str, arguments: dict) -> _FakeCallResult:
        self.calls.append((name, dict(arguments)))
        return _FakeCallResult()


@pytest.mark.asyncio
async def test_discover_tools_fetches_and_hashes_tool_schemas(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [
        _FakeTool("list_issues", "List issues", {"type": "object", "properties": {"repo": {"type": "string"}}}),
        _FakeTool("list_prs", "List pull requests", {"type": "object", "properties": {}}),
    ]
    monkeypatch.setattr("mcp_artifact_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _stdio_config()
    tools = await discover_tools(cfg)

    assert [tool.name for tool in tools] == ["list_issues", "list_prs"]
    assert all(len(tool.schema_hash) == 32 for tool in tools)
    assert _FakeClient.instances
    created = _FakeClient.instances[0]
    assert created.transport["transport"] == "stdio"
    assert created.transport["command"] == cfg.command
    assert created.transport["args"] == cfg.args


@pytest.mark.asyncio
async def test_call_upstream_tool_normalizes_result(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = []
    monkeypatch.setattr("mcp_artifact_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _http_config()
    instance = UpstreamInstance(config=cfg, instance_id="inst1", tools=[])

    result = await call_upstream_tool(instance, "tool_a", {"x": 1})

    assert result["isError"] is False
    assert result["structuredContent"] == {"value": 1}
    assert result["content"] == [{"type": "text", "text": "ok"}]
    assert result["meta"] == {"trace_id": "t-1"}
    created = _FakeClient.instances[0]
    assert created.transport["transport"] == "http"
    assert created.transport["url"] == cfg.url
    assert created.calls == [("tool_a", {"x": 1})]


@pytest.mark.asyncio
async def test_connect_upstream_builds_instance(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [_FakeTool("search", "Search", {"type": "object"})]
    monkeypatch.setattr("mcp_artifact_gateway.mcp.upstream.Client", _FakeClient)

    cfg = _stdio_config()
    instance = await connect_upstream(cfg)

    assert instance.config is cfg
    assert len(instance.instance_id) == 32
    assert [tool.name for tool in instance.tools] == ["search"]


@pytest.mark.asyncio
async def test_connect_upstreams_preserves_config_order(monkeypatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.tools = [_FakeTool("search", "Search", {"type": "object"})]
    monkeypatch.setattr("mcp_artifact_gateway.mcp.upstream.Client", _FakeClient)

    cfg1 = _stdio_config(prefix="gh")
    cfg2 = _http_config(prefix="jira")

    upstreams = await connect_upstreams([cfg1, cfg2])

    assert [u.prefix for u in upstreams] == ["gh", "jira"]
