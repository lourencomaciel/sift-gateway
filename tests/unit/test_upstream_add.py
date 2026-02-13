"""Tests for sift-mcp upstream add command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from sift_mcp.config.upstream_add import (
    print_add_summary,
    run_upstream_add,
)


def _read_gw_config(data_dir: Path) -> dict:
    config_path = data_dir / "state" / "config.json"
    return json.loads(config_path.read_text(encoding="utf-8"))


class TestRunUpstreamAdd:
    def test_add_single_stdio_server(self, tmp_path: Path) -> None:
        servers = {
            "github": {
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-github"],
                "env": {"GITHUB_TOKEN": "ghp_secret123"},
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["github"]
        assert summary["skipped"] == []

        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]
        entry = gw["mcpServers"]["github"]
        assert entry["command"] == "npx"
        assert entry["args"] == [
            "-y",
            "@modelcontextprotocol/server-github",
        ]
        # Secrets should be externalized
        assert "env" not in entry
        assert entry["_gateway"]["secret_ref"] == "github"

        # Secret file should exist
        secret_path = tmp_path / "state" / "upstream_secrets" / "github.json"
        assert secret_path.exists()
        secret = json.loads(secret_path.read_text(encoding="utf-8"))
        assert secret["env"] == {"GITHUB_TOKEN": "ghp_secret123"}
        assert secret["transport"] == "stdio"

    def test_add_single_http_server(self, tmp_path: Path) -> None:
        servers = {
            "remote": {
                "url": "https://example.com/mcp",
                "headers": {"Authorization": "Bearer tok_123"},
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["remote"]
        gw = _read_gw_config(tmp_path)
        entry = gw["mcpServers"]["remote"]
        assert entry["url"] == "https://example.com/mcp"
        assert "headers" not in entry
        assert entry["_gateway"]["secret_ref"] == "remote"

        secret_path = tmp_path / "state" / "upstream_secrets" / "remote.json"
        secret = json.loads(secret_path.read_text(encoding="utf-8"))
        assert secret["headers"] == {"Authorization": "Bearer tok_123"}
        assert secret["transport"] == "http"

    def test_add_multiple_servers(self, tmp_path: Path) -> None:
        servers = {
            "github": {
                "command": "npx",
                "args": ["-y", "@mcp/github"],
            },
            "filesystem": {
                "command": "npx",
                "args": ["-y", "@mcp/filesystem", "/tmp"],
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["filesystem", "github"]
        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]
        assert "filesystem" in gw["mcpServers"]

    def test_accepts_wrapped_mcp_servers(self, tmp_path: Path) -> None:
        """Full claude_desktop_config.json format is accepted."""
        servers = {
            "mcpServers": {
                "github": {
                    "command": "npx",
                    "args": ["-y", "@mcp/github"],
                },
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["github"]
        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]

    def test_accepts_vscode_wrapped_format(self, tmp_path: Path) -> None:
        """VS Code {"mcp": {"servers": {...}}} format is accepted."""
        servers = {
            "mcp": {
                "servers": {
                    "github": {
                        "command": "npx",
                        "args": ["-y", "@mcp/github"],
                    },
                },
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["github"]
        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]

    def test_rejects_duplicate_prefix(self, tmp_path: Path) -> None:
        # Seed existing config
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        config_path = state_dir / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "github": {"command": "npx", "args": []},
                    },
                }
            ),
            encoding="utf-8",
        )

        servers = {
            "github": {
                "command": "npx",
                "args": ["-y", "@mcp/github-v2"],
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == []
        assert summary["skipped"] == ["github"]
        # Existing config should be unchanged
        gw = _read_gw_config(tmp_path)
        assert gw["mcpServers"]["github"]["args"] == []

    def test_secrets_externalized_and_stripped(self, tmp_path: Path) -> None:
        servers = {
            "myserver": {
                "command": "node",
                "args": ["server.js"],
                "env": {"SECRET_KEY": "s3cret"},
            },
        }

        run_upstream_add(servers, data_dir=tmp_path)

        gw = _read_gw_config(tmp_path)
        entry = gw["mcpServers"]["myserver"]
        assert "env" not in entry
        assert entry["_gateway"]["secret_ref"] == "myserver"

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        servers = {
            "github": {
                "command": "npx",
                "args": ["-y", "@mcp/github"],
                "env": {"TOKEN": "tok"},
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path, dry_run=True)

        assert summary["added"] == ["github"]
        # No files or directories should have been created
        state_dir = tmp_path / "state"
        assert not state_dir.exists()

    def test_honors_sift_mcp_data_dir_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_dir = tmp_path / "from-env"
        monkeypatch.setenv("SIFT_MCP_DATA_DIR", str(env_dir))

        servers = {
            "github": {"command": "npx", "args": ["-y", "@mcp/github"]},
        }

        summary = run_upstream_add(servers)

        assert summary["added"] == ["github"]
        config_path = env_dir / "state" / "config.json"
        assert config_path.exists()
        assert str(env_dir) in summary["config_path"]

    def test_explicit_data_dir_overrides_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_dir = tmp_path / "from-env"
        explicit_dir = tmp_path / "explicit"
        monkeypatch.setenv("SIFT_MCP_DATA_DIR", str(env_dir))

        servers = {
            "github": {"command": "npx", "args": ["-y", "@mcp/github"]},
        }

        summary = run_upstream_add(servers, data_dir=explicit_dir)

        assert str(explicit_dir) in summary["config_path"]
        assert not env_dir.exists()

    def test_invalid_entry_missing_command_and_url(
        self, tmp_path: Path
    ) -> None:
        servers = {"bad": {"args": ["--flag"]}}

        with pytest.raises(ValueError, match="neither 'command' nor 'url'"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_entry_both_command_and_url(self, tmp_path: Path) -> None:
        servers = {
            "bad": {
                "command": "npx",
                "url": "https://example.com",
            },
        }

        with pytest.raises(ValueError, match="both 'command' and 'url'"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_prefix_with_slash(self, tmp_path: Path) -> None:
        servers = {
            "../escape": {"command": "npx", "args": []},
        }

        with pytest.raises(ValueError, match="must not contain"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_prefix_rejected_during_dry_run(
        self, tmp_path: Path
    ) -> None:
        servers = {
            "foo/bar": {
                "command": "npx",
                "args": [],
                "env": {"TOKEN": "secret"},
            },
        }

        with pytest.raises(ValueError, match="path separators"):
            run_upstream_add(servers, data_dir=tmp_path, dry_run=True)

    def test_invalid_prefix_no_partial_writes(self, tmp_path: Path) -> None:
        """Second entry has bad prefix; first entry must not be written."""
        servers = {
            "good": {"command": "npx", "args": []},
            "../../bad": {"command": "npx", "args": []},
        }

        with pytest.raises(ValueError, match="must not contain"):
            run_upstream_add(servers, data_dir=tmp_path)

        # Nothing should have been written
        state_dir = tmp_path / "state"
        assert not state_dir.exists()

    def test_empty_snippet_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no servers provided"):
            run_upstream_add({}, data_dir=tmp_path)

    def test_empty_wrapped_mcp_servers_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no servers provided"):
            run_upstream_add({"mcpServers": {}}, data_dir=tmp_path)

    def test_empty_wrapped_vscode_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no servers provided"):
            run_upstream_add({"mcp": {"servers": {}}}, data_dir=tmp_path)

    def test_non_dict_entry_raises(self, tmp_path: Path) -> None:
        servers = {"bad": "not a dict"}

        with pytest.raises(ValueError, match="must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_gateway_block_string(self, tmp_path: Path) -> None:
        servers = {
            "gh": {
                "command": "npx",
                "args": [],
                "_gateway": "bad",
            },
        }

        with pytest.raises(ValueError, match="_gateway must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_gateway_block_list(self, tmp_path: Path) -> None:
        servers = {
            "gh": {
                "command": "npx",
                "args": [],
                "_gateway": ["bad"],
            },
        }

        with pytest.raises(ValueError, match="_gateway must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_valid_gateway_block_accepted(self, tmp_path: Path) -> None:
        servers = {
            "gh": {
                "command": "npx",
                "args": [],
                "_gateway": {"strict_schema_reuse": False},
            },
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)
        assert summary["added"] == ["gh"]

    def test_null_command_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"command": None}}

        with pytest.raises(ValueError, match="command must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_empty_command_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"command": ""}}

        with pytest.raises(ValueError, match="command must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_non_string_command_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"command": 42}}

        with pytest.raises(ValueError, match="command must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_null_url_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"url": None}}

        with pytest.raises(ValueError, match="url must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_empty_url_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"url": ""}}

        with pytest.raises(ValueError, match="url must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_non_string_url_rejected(self, tmp_path: Path) -> None:
        servers = {"bad": {"url": 123}}

        with pytest.raises(ValueError, match="url must be a non-empty"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_env_type_list(self, tmp_path: Path) -> None:
        servers = {
            "gh": {
                "command": "npx",
                "args": [],
                "env": ["BAD"],
            },
        }

        with pytest.raises(ValueError, match="env must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_headers_type_string(self, tmp_path: Path) -> None:
        servers = {
            "remote": {
                "url": "https://example.com/mcp",
                "headers": "Bearer tok",
            },
        }

        with pytest.raises(ValueError, match="headers must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path)

    def test_invalid_env_rejected_during_dry_run(self, tmp_path: Path) -> None:
        servers = {
            "gh": {
                "command": "npx",
                "args": [],
                "env": 42,
            },
        }

        with pytest.raises(ValueError, match="env must be a JSON object"):
            run_upstream_add(servers, data_dir=tmp_path, dry_run=True)

    def test_works_with_empty_existing_config(self, tmp_path: Path) -> None:
        # Seed an empty config file
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        config_path = state_dir / "config.json"
        config_path.write_text("{}", encoding="utf-8")

        servers = {
            "github": {"command": "npx", "args": ["-y", "@mcp/github"]},
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["github"]
        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]

    def test_server_without_secrets_no_secret_ref(self, tmp_path: Path) -> None:
        servers = {
            "filesystem": {
                "command": "npx",
                "args": ["-y", "@mcp/filesystem", "/tmp"],
            },
        }

        run_upstream_add(servers, data_dir=tmp_path)

        gw = _read_gw_config(tmp_path)
        entry = gw["mcpServers"]["filesystem"]
        assert "_gateway" not in entry

        secret_path = (
            tmp_path / "state" / "upstream_secrets" / "filesystem.json"
        )
        assert not secret_path.exists()

    def test_add_to_existing_servers_preserves_them(
        self, tmp_path: Path
    ) -> None:
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        config_path = state_dir / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "mcpServers": {
                        "existing": {
                            "command": "node",
                            "args": ["old.js"],
                        },
                    },
                    "db_backend": "sqlite",
                }
            ),
            encoding="utf-8",
        )

        servers = {
            "newserver": {"command": "npx", "args": ["-y", "@mcp/new"]},
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["newserver"]
        gw = _read_gw_config(tmp_path)
        # Both old and new should be present
        assert "existing" in gw["mcpServers"]
        assert "newserver" in gw["mcpServers"]
        # Other config keys preserved
        assert gw["db_backend"] == "sqlite"


    def test_legacy_upstreams_key_removed(self, tmp_path: Path) -> None:
        """Adding to a config with legacy 'upstreams' strips it."""
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True)
        config_path = state_dir / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "upstreams": [
                        {"prefix": "old", "command": "node"},
                    ],
                }
            ),
            encoding="utf-8",
        )

        servers = {
            "github": {"command": "npx", "args": ["-y", "@mcp/github"]},
        }

        summary = run_upstream_add(servers, data_dir=tmp_path)

        assert summary["added"] == ["github"]
        gw = _read_gw_config(tmp_path)
        assert "github" in gw["mcpServers"]
        assert "upstreams" not in gw


class TestPrintAddSummary:
    def test_prints_added(self, capsys: pytest.CaptureFixture) -> None:
        summary = {
            "added": ["github", "filesystem"],
            "skipped": [],
            "config_path": "/tmp/state/config.json",
        }
        print_add_summary(summary)
        out = capsys.readouterr().out
        assert "Added 2 upstream(s)" in out
        assert "+ github" in out
        assert "+ filesystem" in out

    def test_prints_skipped(self, capsys: pytest.CaptureFixture) -> None:
        summary = {
            "added": [],
            "skipped": ["github"],
            "config_path": "/tmp/state/config.json",
        }
        print_add_summary(summary)
        out = capsys.readouterr().out
        assert "No new upstreams added" in out
        assert "- github" in out

    def test_dry_run_prefix(self, capsys: pytest.CaptureFixture) -> None:
        summary = {
            "added": ["github"],
            "skipped": [],
            "config_path": "/tmp/state/config.json",
        }
        print_add_summary(summary, dry_run=True)
        out = capsys.readouterr().out
        assert "[dry run]" in out


class TestCLIArgParsing:
    def test_upstream_add_parses(self) -> None:
        from sift_mcp.main import _parse_args

        snippet = '{"gh": {"command": "npx", "args": []}}'
        with patch(
            "sys.argv",
            ["sift-mcp", "upstream", "add", snippet],
        ):
            args = _parse_args()

        assert args.command == "upstream"
        assert args.upstream_command == "add"
        assert args.snippet == snippet
        assert args.dry_run is False

    def test_upstream_add_dry_run(self) -> None:
        from sift_mcp.main import _parse_args

        snippet = '{"gh": {"command": "npx", "args": []}}'
        with patch(
            "sys.argv",
            ["sift-mcp", "upstream", "add", "--dry-run", snippet],
        ):
            args = _parse_args()

        assert args.dry_run is True

    def test_upstream_add_data_dir_global(self) -> None:
        from sift_mcp.main import _parse_args

        snippet = '{"gh": {"command": "npx", "args": []}}'
        with patch(
            "sys.argv",
            [
                "sift-mcp",
                "--data-dir",
                "/custom",
                "upstream",
                "add",
                snippet,
            ],
        ):
            args = _parse_args()

        assert args.data_dir == "/custom"

    def test_upstream_add_data_dir_subcommand(self) -> None:
        from sift_mcp.main import _parse_args

        snippet = '{"gh": {"command": "npx", "args": []}}'
        with patch(
            "sys.argv",
            [
                "sift-mcp",
                "upstream",
                "add",
                "--data-dir",
                "/sub",
                snippet,
            ],
        ):
            args = _parse_args()

        assert args.data_dir == "/sub"

    def test_upstream_add_data_dir_not_shadowed(self) -> None:
        """Global --data-dir is preserved when not repeated."""
        from sift_mcp.main import _parse_args

        snippet = '{"gh": {"command": "npx", "args": []}}'
        with patch(
            "sys.argv",
            [
                "sift-mcp",
                "--data-dir",
                "/global",
                "upstream",
                "add",
                snippet,
            ],
        ):
            args = _parse_args()

        assert args.data_dir == "/global"
