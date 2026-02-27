"""Tests for standard mcpServers config format parsing and integration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sift_gateway.config.mcp_servers import (
    extract_mcp_servers,
    resolve_mcp_servers_config,
    to_upstream_configs,
)
from sift_gateway.config.settings import load_gateway_config

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

    def test_zed_format_command_string(self) -> None:
        raw = {
            "context_servers": {
                "github": {
                    "source": "custom",
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-github"],
                    "env": {"GITHUB_TOKEN": "tok"},
                }
            }
        }
        servers = extract_mcp_servers(raw)
        assert "github" in servers
        assert servers["github"]["command"] == "npx"
        assert servers["github"]["args"] == [
            "-y",
            "@modelcontextprotocol/server-github",
        ]
        assert servers["github"]["env"] == {"GITHUB_TOKEN": "tok"}

    def test_zed_format_command_object(self) -> None:
        raw = {
            "context_servers": {
                "tracker": {
                    "source": "custom",
                    "command": {
                        "path": "uvx",
                        "args": ["yandex-tracker-mcp@latest"],
                        "env": {"TRACKER_TOKEN": "tok"},
                    },
                }
            }
        }
        servers = extract_mcp_servers(raw)
        assert servers["tracker"]["command"] == "uvx"
        assert servers["tracker"]["args"] == ["yandex-tracker-mcp@latest"]
        assert servers["tracker"]["env"] == {"TRACKER_TOKEN": "tok"}

    def test_zed_format_url(self) -> None:
        raw = {
            "context_servers": {
                "remote": {
                    "url": "https://example.com/mcp",
                    "headers": {"Authorization": "Bearer token"},
                }
            }
        }
        servers = extract_mcp_servers(raw)
        assert servers["remote"]["url"] == "https://example.com/mcp"
        assert servers["remote"]["headers"] == {"Authorization": "Bearer token"}

    def test_invalid_zed_server_entry_raises(self) -> None:
        raw = {"context_servers": {"bad": "not-a-dict"}}
        with pytest.raises(ValueError, match="must be a JSON object"):
            extract_mcp_servers(raw)

    def test_invalid_zed_command_path_raises(self) -> None:
        raw = {
            "context_servers": {
                "bad": {
                    "command": {"path": 123},
                }
            }
        }
        with pytest.raises(ValueError, match=r"command\.path must be a string"):
            extract_mcp_servers(raw)

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
        configs = to_upstream_configs(
            {"api": {"url": "https://example.com/mcp"}}
        )
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
                        "pagination": {
                            "strategy": "cursor",
                            "cursor_response_path": "$.paging.cursors.after",
                            "cursor_param_name": "after",
                            "has_more_response_path": "$.paging.next",
                        },
                    },
                }
            }
        )
        c = configs[0]
        assert c["semantic_salt_env_keys"] == ["GITHUB_ORG"]
        assert c["pagination"]["strategy"] == "cursor"

    def test_gateway_extensions_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="_gateway must be a JSON object"):
            to_upstream_configs({"gh": {"command": "gh", "_gateway": "bad"}})

    def test_gateway_enabled_false_skips_server(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "gh",
                    "_gateway": {"enabled": False},
                },
                "api": {"url": "https://example.com/mcp"},
            }
        )
        assert len(configs) == 1
        assert configs[0]["prefix"] == "api"

    def test_gateway_enabled_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match=r"_gateway\.enabled"):
            to_upstream_configs(
                {"gh": {"command": "gh", "_gateway": {"enabled": "false"}}}
            )

    def test_gateway_secret_ref_promoted(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "gh",
                    "_gateway": {
                        "secret_ref": "vault://secrets/github",
                    },
                }
            }
        )
        c = configs[0]
        assert c["secret_ref"] == "vault://secrets/github"

    def test_gateway_inherit_parent_env_promoted(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "gh",
                    "_gateway": {
                        "inherit_parent_env": True,
                    },
                }
            }
        )
        c = configs[0]
        assert c["inherit_parent_env"] is True

    def test_gateway_external_user_id_promoted(self) -> None:
        configs = to_upstream_configs(
            {
                "pd": {
                    "command": "npx",
                    "args": ["-y", "@pipedream/mcp", "stdio"],
                    "_gateway": {
                        "external_user_id": "auto",
                    },
                }
            }
        )
        c = configs[0]
        assert c["external_user_id"] == "auto"

    def test_gateway_passthrough_allowed_promoted(self) -> None:
        configs = to_upstream_configs(
            {
                "gh": {
                    "command": "gh",
                    "_gateway": {
                        "passthrough_allowed": False,
                    },
                }
            }
        )
        c = configs[0]
        assert c["passthrough_allowed"] is False

    def test_no_gateway_extensions(self) -> None:
        configs = to_upstream_configs({"gh": {"command": "gh"}})
        c = configs[0]
        assert "semantic_salt_env_keys" not in c
        assert "pagination" not in c

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

    def test_zed_format(self) -> None:
        raw = {
            "context_servers": {
                "gh": {
                    "source": "custom",
                    "command": "gh",
                }
            }
        }
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
        assert gh.env == {}
        assert gh.secret_ref == "github"

        api = next(u for u in config.upstreams if u.prefix == "api")
        assert api.transport == "http"
        assert api.url == "https://api.example.com/mcp"
        assert api.headers == {}
        assert api.secret_ref == "api"

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
                                "passthrough_allowed": False,
                                "pagination": {
                                    "strategy": "cursor",
                                    "cursor_response_path": (
                                        "$.paging.cursors.after"
                                    ),
                                    "cursor_param_name": "after",
                                },
                            },
                        },
                    }
                }
            )
        )
        config = load_gateway_config(data_dir_override=str(tmp_path))
        gh = config.upstreams[0]
        assert gh.semantic_salt_env_keys == ["GITHUB_ORG"]
        assert gh.passthrough_allowed is False
        assert gh.pagination is not None
        assert gh.pagination.strategy == "cursor"
        assert gh.pagination.cursor_param_name == "after"

    def test_registry_secret_ref_rows_accept_env_overrides(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "gh": {
                            "command": "gh",
                            "env": {"BASE_TOKEN": "from-file"},
                        }
                    }
                }
            )
        )

        initial = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(initial.upstreams) == 1
        assert initial.upstreams[0].secret_ref == "gh"
        assert initial.upstreams[0].env == {}

        monkeypatch.setenv(
            "SIFT_GATEWAY_UPSTREAMS__0__ENV__OVERRIDE_TOKEN",
            "from-env",
        )
        merged = load_gateway_config(data_dir_override=str(tmp_path))
        gh = merged.upstreams[0]
        assert gh.env == {
            "BASE_TOKEN": "from-file",
            "OVERRIDE_TOKEN": "from-env",
        }
        assert gh.secret_ref is None

    def test_registry_http_secret_ref_rows_keep_headers_on_env_override(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "api": {
                            "url": "https://api.example.com/mcp",
                            "headers": {"Authorization": "Bearer from-file"},
                        }
                    }
                }
            )
        )

        initial = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(initial.upstreams) == 1
        assert initial.upstreams[0].secret_ref == "api"
        assert initial.upstreams[0].headers == {}

        monkeypatch.setenv(
            "SIFT_GATEWAY_UPSTREAMS__0__ENV__OVERRIDE_TOKEN",
            "from-env",
        )
        merged = load_gateway_config(data_dir_override=str(tmp_path))
        api = merged.upstreams[0]
        assert api.env == {"OVERRIDE_TOKEN": "from-env"}
        assert api.headers == {"Authorization": "Bearer from-file"}
        assert api.secret_ref is None

    def test_registry_stdio_secret_ref_rows_keep_env_on_header_override(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "gh": {
                            "command": "gh",
                            "env": {"BASE_TOKEN": "from-file"},
                        }
                    }
                }
            )
        )

        initial = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(initial.upstreams) == 1
        assert initial.upstreams[0].secret_ref == "gh"
        assert initial.upstreams[0].env == {}

        monkeypatch.setenv(
            "SIFT_GATEWAY_UPSTREAMS__0__HEADERS__X_TRACE",
            "1",
        )
        merged = load_gateway_config(data_dir_override=str(tmp_path))
        gh = merged.upstreams[0]
        assert gh.env == {"BASE_TOKEN": "from-file"}
        assert gh.headers == {"X_TRACE": "1"}
        assert gh.secret_ref is None

    def test_registry_secret_ref_override_requires_resolvable_secret(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        from sift_gateway.config.upstream_registry import (
            replace_registry_from_mcp_servers,
        )

        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}}})
        )
        replace_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={"gh": {"command": "gh", "_gateway": {"secret_ref": "gh"}}},
            source_kind="manual",
        )

        monkeypatch.setenv(
            "SIFT_GATEWAY_UPSTREAMS__0__ENV__OVERRIDE_TOKEN",
            "from-env",
        )
        with pytest.raises(
            ValueError,
            match=(
                "Cannot specify both inline env/headers and "
                "secret_ref for upstream"
            ),
        ):
            load_gateway_config(data_dir_override=str(tmp_path))

    def test_legacy_upstreams_raises_migration_error(
        self, tmp_path: Path
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "upstreams": [
                        {
                            "prefix": "gh",
                            "transport": "http",
                            "url": "https://example.com",
                        },
                    ]
                }
            )
        )
        with pytest.raises(ValueError, match="no longer supported"):
            load_gateway_config(data_dir_override=str(tmp_path))

    def test_mixed_format_raises(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {"gh": {"command": "gh"}},
                    "upstreams": [
                        {"prefix": "x", "transport": "http", "url": "http://x"}
                    ],
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

    def test_disabled_mcp_server_not_loaded(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "gh": {
                            "command": "gh",
                            "_gateway": {"enabled": False},
                        },
                        "api": {"url": "https://example.com/mcp"},
                    }
                }
            )
        )
        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert len(config.upstreams) == 1
        assert config.upstreams[0].prefix == "api"

    def test_registry_only_disabled_rows_do_not_fallback_to_mcp_servers(
        self, tmp_path: Path
    ) -> None:
        from sift_gateway.config.upstream_registry import (
            replace_registry_from_mcp_servers,
            set_registry_upstream_enabled,
        )

        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}}})
        )

        replace_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={"gh": {"command": "gh"}},
            source_kind="manual",
        )
        set_registry_upstream_enabled(
            data_dir=tmp_path,
            prefix="gh",
            enabled=False,
        )

        # Simulate stale config drift where the compatibility mirror is edited.
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}}})
        )

        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert config.upstreams == []

    def test_registry_rows_load_even_when_config_mirror_is_invalid(
        self, tmp_path: Path
    ) -> None:
        from sift_gateway.config.upstream_registry import (
            replace_registry_from_mcp_servers,
        )

        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}}})
        )

        replace_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={"gh": {"command": "gh"}},
            source_kind="manual",
        )

        # Simulate compatibility mirror drift while canonical rows remain valid.
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}, "bad": "oops"}})
        )

        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert [upstream.prefix for upstream in config.upstreams] == ["gh"]

    def test_registry_rows_load_when_mirror_has_invalid_gateway_values(
        self, tmp_path: Path
    ) -> None:
        from sift_gateway.config.upstream_registry import (
            replace_registry_from_mcp_servers,
        )

        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps({"mcpServers": {"gh": {"command": "gh"}}})
        )

        replace_registry_from_mcp_servers(
            data_dir=tmp_path,
            servers={"gh": {"command": "gh"}},
            source_kind="manual",
        )

        # Simulate compatibility mirror drift with structurally valid entries
        # that fail per-field gateway validation.
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "gh": {"command": "gh"},
                        "bad": {
                            "command": "npx",
                            "_gateway": {"enabled": "false"},
                        },
                    }
                }
            )
        )

        config = load_gateway_config(data_dir_override=str(tmp_path))
        assert [upstream.prefix for upstream in config.upstreams] == ["gh"]

    def test_registry_preserves_config_order_for_index_overrides(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "zeta": {"command": "zcmd"},
                        "alpha": {"command": "acmd"},
                    }
                }
            )
        )
        monkeypatch.setenv("SIFT_GATEWAY_UPSTREAMS__0__COMMAND", "override")

        config = load_gateway_config(data_dir_override=str(tmp_path))

        assert [upstream.prefix for upstream in config.upstreams] == [
            "zeta",
            "alpha",
        ]
        assert config.upstreams[0].command == "override"
        assert config.upstreams[1].command == "acmd"

    def test_registry_index_overrides_remain_stable_with_disabled_rows(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "alpha": {
                            "command": "acmd",
                            "_gateway": {"enabled": False},
                        },
                        "beta": {"command": "bcmd"},
                        "gamma": {"command": "gcmd"},
                    }
                }
            )
        )
        # Index 1 targets "beta" in canonical row order, even when index 0
        # ("alpha") is disabled.
        monkeypatch.setenv(
            "SIFT_GATEWAY_UPSTREAMS__1__COMMAND", "override-beta"
        )

        config = load_gateway_config(data_dir_override=str(tmp_path))

        assert [upstream.prefix for upstream in config.upstreams] == [
            "beta",
            "gamma",
        ]
        assert config.upstreams[0].command == "override-beta"
        assert config.upstreams[1].command == "gcmd"

    def test_registry_preserves_auto_pagination_overrides(
        self, tmp_path: Path
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "gh": {
                            "command": "gh",
                            "_gateway": {
                                "auto_paginate_max_pages": 5,
                                "auto_paginate_max_records": 100,
                                "auto_paginate_timeout_seconds": 3.5,
                            },
                        }
                    }
                }
            )
        )

        config = load_gateway_config(data_dir_override=str(tmp_path))
        gh = config.upstreams[0]
        assert gh.auto_paginate_max_pages == 5
        assert gh.auto_paginate_max_records == 100
        assert gh.auto_paginate_timeout_seconds == 3.5
