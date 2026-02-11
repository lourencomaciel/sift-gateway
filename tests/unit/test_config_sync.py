"""Tests for startup auto-sync of newly added MCP servers."""

from __future__ import annotations

import json
from pathlib import Path

from sidepouch_mcp.config.sync import run_sync


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2) + "\n",
        encoding="utf-8",
    )


def _setup_gateway_and_source(
    tmp_path: Path,
    *,
    gateway_servers: dict | None = None,
    source_servers: dict | None = None,
    sync_enabled: bool = True,
    extra_gw_keys: dict | None = None,
) -> tuple[Path, Path, Path]:
    """Set up gateway config and source file for sync tests.

    Returns (data_dir, config_path, source_path).
    """
    data_dir = tmp_path / "data"
    config_path = data_dir / "state" / "config.json"
    source_path = tmp_path / "source.json"

    gw_config: dict = {}
    if gateway_servers is not None:
        gw_config["mcpServers"] = gateway_servers
    else:
        gw_config["mcpServers"] = {}

    gw_config["_gateway_sync"] = {
        "enabled": sync_enabled,
        "source_path": str(source_path),
        "gateway_name": "artifact-gateway",
    }
    if extra_gw_keys:
        gw_config.update(extra_gw_keys)

    _write_json(config_path, gw_config)

    if source_servers is not None:
        _write_json(
            source_path,
            {"mcpServers": source_servers},
        )

    return data_dir, config_path, source_path


class TestRunSync:
    def test_sync_imports_new_mcp_from_source(self, tmp_path: Path) -> None:
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sidepouch-mcp",
                },
                "github": {
                    "command": "npx",
                    "args": [
                        "-y",
                        "@modelcontextprotocol/server-github",
                    ],
                },
            },
        )

        result = run_sync(data_dir)

        assert result["synced"] == 1

        gw_config = json.loads(config_path.read_text(encoding="utf-8"))
        assert "github" in gw_config["mcpServers"]
        assert gw_config["mcpServers"]["github"]["command"] == "npx"

    def test_sync_rewrites_source_back_to_gateway_only(
        self, tmp_path: Path
    ) -> None:
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sidepouch-mcp",
                },
                "github": {
                    "command": "npx",
                },
                "filesystem": {
                    "command": "fs-tool",
                },
            },
        )

        run_sync(data_dir)

        source = json.loads(source_path.read_text(encoding="utf-8"))
        assert list(source["mcpServers"].keys()) == ["artifact-gateway"]
        assert (
            source["mcpServers"]["artifact-gateway"]["command"]
            == "sidepouch-mcp"
        )

    def test_sync_idempotent_when_no_new_entries(self, tmp_path: Path) -> None:
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={
                "github": {"command": "npx"},
            },
            source_servers={
                "artifact-gateway": {
                    "command": "sidepouch-mcp",
                },
                "github": {
                    "command": "npx",
                },
            },
        )

        result = run_sync(data_dir)

        assert result["synced"] == 0

    def test_sync_source_missing_produces_warning(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        config_path = data_dir / "state" / "config.json"
        missing_source = tmp_path / "missing.json"

        _write_json(
            config_path,
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(missing_source),
                    "gateway_name": "artifact-gateway",
                },
            },
        )

        result = run_sync(data_dir)

        assert result["synced"] == 0
        assert "warning" in result
        assert "not found" in result["warning"]

    def test_sync_skipped_when_no_metadata(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        config_path = data_dir / "state" / "config.json"
        _write_json(
            config_path,
            {"mcpServers": {"tool": {"command": "tool"}}},
        )

        result = run_sync(data_dir)

        assert result == {"synced": 0}

    def test_sync_externalizes_imported_secrets(self, tmp_path: Path) -> None:
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sidepouch-mcp",
                },
                "github": {
                    "command": "npx",
                    "env": {
                        "GITHUB_TOKEN": "ghp_secret",
                    },
                },
            },
        )

        result = run_sync(data_dir)

        assert result["synced"] == 1

        # Gateway config should have secret_ref, not inline env
        gw_config = json.loads(config_path.read_text(encoding="utf-8"))
        github = gw_config["mcpServers"]["github"]
        assert "env" not in github
        assert github["_gateway"]["secret_ref"] == "github"

        # Secret file should exist
        secret_file = data_dir / "state" / "upstream_secrets" / "github.json"
        assert secret_file.exists()
        secret_data = json.loads(secret_file.read_text(encoding="utf-8"))
        assert secret_data["env"]["GITHUB_TOKEN"] == "ghp_secret"
        assert secret_data["transport"] == "stdio"


class TestSyncDisabled:
    def test_sync_skipped_when_disabled(self, tmp_path: Path) -> None:
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sidepouch-mcp",
                },
                "github": {"command": "npx"},
            },
            sync_enabled=False,
        )

        result = run_sync(data_dir)

        assert result == {"synced": 0}

    def test_sync_no_config_file(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        # No config file at all
        result = run_sync(data_dir)
        assert result == {"synced": 0}


class TestIsGatewayEntry:
    def test_matches_by_name(self) -> None:
        from sidepouch_mcp.config.sync import (
            _is_gateway_entry,
        )

        assert _is_gateway_entry(
            "artifact-gateway",
            {"command": "anything"},
            "artifact-gateway",
        )

    def test_matches_by_command(self) -> None:
        from sidepouch_mcp.config.sync import (
            _is_gateway_entry,
        )

        assert _is_gateway_entry(
            "some-name",
            {"command": "sidepouch-mcp"},
            "artifact-gateway",
        )

    def test_url_with_sidepouch_is_not_gateway(self) -> None:
        """URL substring no longer identifies gateway entries."""
        from sidepouch_mcp.config.sync import (
            _is_gateway_entry,
        )

        assert not _is_gateway_entry(
            "my-gateway",
            {"url": "http://localhost:8080/sidepouch"},
            "artifact-gateway",
        )

    def test_non_gateway_entry(self) -> None:
        from sidepouch_mcp.config.sync import (
            _is_gateway_entry,
        )

        assert not _is_gateway_entry(
            "github",
            {"command": "npx", "args": ["server-github"]},
            "artifact-gateway",
        )
