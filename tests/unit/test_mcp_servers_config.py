"""Tests for standard mcpServers config format parsing and integration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mcp_artifact_gateway.config.mcp_servers import (
    extract_mcp_servers,
    resolve_mcp_servers_config,
    to_upstream_configs,
)
from mcp_artifact_gateway.config.settings import load_gateway_config


# ---------------------------------------------------------------------------
# extract_mcp_servers
# ---------------------------------------------------------------------------


class TestExtractMcpServers:
    def test_claude_desktop_format(self) -> None:
        raw = {
            "mcpServers": {
                "github": {"command": "gh", "args": ["mcp"]},
                "jira": {"url": "https://jira.example.com/mcp"},
            }
        }
        servers = extract_mcp_servers(raw)
        assert set(servers.keys()) == {"github", "jira"}
        assert servers["github"]["command"] == "gh"

    def test_vscode_format(self) -> None:
        raw = {
            "mcp": {
                "servers": {
                    "github": {"command": "gh", "args": ["mcp"]},
                }
            }
        }
        servers = extract_mcp_servers(raw)
        assert "github" in servers
        assert servers["github"]["command"] == "gh"

    def test_mcpservers_takes_precedence_over_vscode(self) -> None:
        raw = {
            "mcpServers": {"a": {"command": "a"}},
            "mcp": {"servers": {"b": {"command": "b"}}},
        }
        servers = extract_mcp_servers(raw)
        assert "a" in servers
        assert "b" not in servers

    def test_empty_config(self) -> None:
        assert extract_mcp_servers({}) == {}

    def test_no_matching_keys(self) -> None:
        assert extract_mcp_servers({"upstreams": []}) == {}

    def test_invalid_mcpservers_type(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON object"):
            extract_mcp_servers({"mcpServers": "not-a-dict"})


# ---------------------------------------------------------------------------
# Transport inference
# ---------------------------------------------------------------------------


class TestTransportInference:
    def test_command_infers_stdio(self) -> None:
        configs = to_upstream_configs({"gh": {"command": "/usr/bin/gh"}})
        assert configs[0]["transport"] == "stdio"
        assert configs[0]["prefix"] == "gh"

    def test_url_infers_http(self) -> None:
        configs = to_upstream_configs({"api": {"url": "https://example.com/mcp"}})
        assert configs[0]["transport"] == "http"

    def test_both_command_and_url_raises(self) -> None:
        with pytest.raises(ValueError, match="both 'command' and 'url'"):
            to_upstream_configs({"bad": {"command": "x", "url": "y"}})

    def test_neither_command_nor_url_raises(self) -> None:
        with pytest.raises(ValueError, match="neither 'command' nor 'url'"):
            to_upstream_configs({"bad": {"args": ["x"]}})

    def test_invalid_entry_type_raises(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON object"):
            to_upstream_configs({"bad": "not-a-dict"})


# ---------------------------------------------------------------------------
# to_upstream_configs — field mapping
# ---------------------------------------------------------------------------


class TestToUpstreamConfigs:
    def test_stdio_fields_mapped(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "/usr/bin/gh",
                    "args": ["mcp", "--mode", "prod"],
                    "env": {"GITHUB_TOKEN": "secret"},
                }
            }
        )
        c = configs[0]
        assert c["prefix"] == "gh"
        assert c["transport"] == "stdio"
        assert c["command"] == "/usr/bin/gh"
        assert c["args"] == ["mcp", "--mode", "prod"]
        assert c["env"] == {"GITHUB_TOKEN": "secret"}

    def test_http_fields_mapped(self) -> None:
        configs = to_upstream_configs(
            {
                "api": {
                    "url": "https://api.example.com/mcp",
                    "headers": {"Authorization": "Bearer tok"},
                }
            }
        )
        c = configs[0]
        assert c["prefix"] == "api"
        assert c["transport"] == "http"
        assert c["url"] == "https://api.example.com/mcp"
        assert c["headers"] == {"Authorization": "Bearer tok"}

    def test_gateway_extensions_promoted(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "gh",
                    "_gateway": {
                        "semantic_salt_env_keys": ["GITHUB_ORG"],
                        "strict_schema_reuse": False,
                        "passthrough_allowed": False,
                        "dedupe_exclusions": ["$.meta.timestamp"],
                    },
                }
            }
        )
        c = configs[0]
        assert c["semantic_salt_env_keys"] == ["GITHUB_ORG"]
        assert c["strict_schema_reuse"] is False
        assert c["passthrough_allowed"] is False
        assert c["dedupe_exclusions"] == ["$.meta.timestamp"]

    def test_gateway_extensions_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="_gateway must be a JSON object"):
            to_upstream_configs({"gh": {"command": "gh", "_gateway": "bad"}})

    def test_no_gateway_extensions(self) -> None:
        configs = to_upstream_configs({"gh": {"command": "gh"}})
        c = configs[0]
        assert "semantic_salt_env_keys" not in c
        assert "strict_schema_reuse" not in c

    def test_multiple_servers_preserved(self) -> None:
        configs = to_upstream_configs(
            {
                "alpha": {"command": "a"},
                "beta": {"url": "https://b.example.com"},
            }
        )
        prefixes = {c["prefix"] for c in configs}
        assert prefixes == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# resolve_mcp_servers_config — full pipeline
# ---------------------------------------------------------------------------


class TestResolveMcpServersConfig:
    def test_returns_none_for_legacy_format(self) -> None:
        assert resolve_mcp_servers_config({"upstreams": []}) is None

    def test_returns_none_for_empty_config(self) -> None:
        assert resolve_mcp_servers_config({}) is None

    def test_basic_mcp_servers(self) -> None:
        raw = {
            "mcpServers": {
                "github": {"command": "gh", "args": ["mcp"]},
                "api": {"url": "https://example.com/mcp"},
            }
        }
        configs = resolve_mcp_servers_config(raw)
        assert configs is not None
        assert len(configs) == 2
        prefixes = {c["prefix"] for c in configs}
        assert prefixes == {"github", "api"}

    def test_vscode_format(self) -> None:
        raw = {"mcp": {"servers": {"gh": {"command": "gh"}}}}
        configs = resolve_mcp_servers_config(raw)
        assert configs is not None
        assert len(configs) == 1
        assert configs[0]["prefix"] == "gh"


# ---------------------------------------------------------------------------
# Integration with load_gateway_config
# ---------------------------------------------------------------------------


class TestLoadGatewayConfigMcpServers:
    def test_mcp_servers_format_loads(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "github": {
                            "command": "/usr/bin/gh",
                            "args": ["mcp"],
                            "env": {"GITHUB_TOKEN": "secret"},
                        },
                        "api": {
                            "url": "https://api.example.com/mcp",
                            "headers": {"Authorization": "Bearer tok"},
                        },
                    }
                }
            )
        )
        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(config.upstreams) == 2
        prefixes = {u.prefix for u in config.upstreams}
        assert prefixes == {"github", "api"}

        gh = next(u for u in config.upstreams if u.prefix == "github")
        assert gh.transport == "stdio"
        assert gh.command == "/usr/bin/gh"
        assert gh.args == ["mcp"]
        assert gh.env == {"GITHUB_TOKEN": "secret"}

        api = next(u for u in config.upstreams if u.prefix == "api")
        assert api.transport == "http"
        assert api.url == "https://api.example.com/mcp"

    def test_mcp_servers_with_gateway_extensions(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "github": {
                            "command": "gh",
                            "_gateway": {
                                "semantic_salt_env_keys": ["GITHUB_ORG"],
                                "strict_schema_reuse": False,
                            },
                        },
                    }
                }
            )
        )
        config = load_gateway_config(data_dir_override=str(tmp_path))
        gh = config.upstreams[0]
        assert gh.semantic_salt_env_keys == ["GITHUB_ORG"]
        assert gh.strict_schema_reuse is False

    def test_legacy_upstreams_still_works(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "upstreams": [
                        {"prefix": "gh", "transport": "http", "url": "https://example.com"},
                    ]
                }
            )
        )
        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(config.upstreams) == 1
        assert config.upstreams[0].prefix == "gh"

    def test_mixed_format_raises(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {"gh": {"command": "gh"}},
                    "upstreams": [{"prefix": "x", "transport": "http", "url": "http://x"}],
                }
            )
        )
        with pytest.raises(ValueError, match="use one format or the other"):
            load_gateway_config(data_dir_override=str(tmp_path))

    def test_empty_mcp_servers_ok(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(json.dumps({"mcpServers": {}}))
        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert config.upstreams == []
