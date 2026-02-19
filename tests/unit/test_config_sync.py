"""Tests for startup auto-sync of newly added MCP servers."""

from __future__ import annotations

import json
from pathlib import Path

from sift_gateway.config.sync import run_sync


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
        data_dir, config_path, _source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sift-gateway",
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
        data_dir, _config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sift-gateway",
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
        gw_entry = source["mcpServers"]["artifact-gateway"]
        assert gw_entry["command"] == "sift-gateway"
        assert gw_entry["args"] == ["--data-dir", str(data_dir.resolve())]

    def test_sync_idempotent_when_no_new_entries(self, tmp_path: Path) -> None:
        data_dir, _config_path, _source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={
                "github": {"command": "npx"},
            },
            source_servers={
                "artifact-gateway": {
                    "command": "sift-gateway",
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
        data_dir, config_path, _source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sift-gateway",
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

    def test_sync_preserves_zed_source_format(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        config_path = data_dir / "state" / "config.json"
        source_path = tmp_path / "settings.json"

        _write_json(
            config_path,
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source_path),
                    "gateway_name": "artifact-gateway",
                },
            },
        )
        _write_json(
            source_path,
            {
                "context_servers": {
                    "artifact-gateway": {
                        "source": "custom",
                        "command": "sift-gateway",
                    },
                    "github": {
                        "source": "custom",
                        "command": "npx",
                        "args": ["-y", "@modelcontextprotocol/server-github"],
                    },
                }
            },
        )

        result = run_sync(data_dir)

        assert result["synced"] == 1
        source = json.loads(source_path.read_text(encoding="utf-8"))
        assert "context_servers" in source
        assert list(source["context_servers"].keys()) == ["artifact-gateway"]
        assert source["context_servers"]["artifact-gateway"]["command"] == (
            "sift-gateway"
        )

    def test_sync_uses_metadata_data_dir_for_rewrite_and_gateway_updates(
        self, tmp_path: Path
    ) -> None:
        custom_data_dir = (tmp_path / "custom-data").resolve()
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {"command": "sift-gateway"},
                "github": {
                    "command": "npx",
                    "env": {"GITHUB_TOKEN": "ghp_secret"},
                },
            },
            extra_gw_keys={
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str((tmp_path / "source.json").resolve()),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(custom_data_dir),
                }
            },
        )
        _write_json(
            custom_data_dir / "state" / "config.json",
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str((tmp_path / "source.json").resolve()),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(custom_data_dir),
                },
            },
        )

        run_sync(data_dir)

        source = json.loads(source_path.read_text(encoding="utf-8"))
        gw_entry = source["mcpServers"]["artifact-gateway"]
        assert gw_entry["args"] == ["--data-dir", str(custom_data_dir)]

        custom_config_path = custom_data_dir / "state" / "config.json"
        custom_config = json.loads(
            custom_config_path.read_text(encoding="utf-8")
        )
        assert "github" in custom_config["mcpServers"]

        original_config = json.loads(config_path.read_text(encoding="utf-8"))
        assert "github" not in original_config["mcpServers"]

        assert (
            custom_data_dir / "state" / "upstream_secrets" / "github.json"
        ).exists()
        assert not (
            data_dir / "state" / "upstream_secrets" / "github.json"
        ).exists()

    def test_sync_keeps_current_data_dir_when_redirect_config_missing(
        self, tmp_path: Path
    ) -> None:
        missing_redirect_data_dir = (tmp_path / "missing-redirect").resolve()
        data_dir, config_path, source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {"command": "sift-gateway"},
                "github": {
                    "command": "npx",
                    "env": {"GITHUB_TOKEN": "ghp_secret"},
                },
            },
            extra_gw_keys={
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str((tmp_path / "source.json").resolve()),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(missing_redirect_data_dir),
                }
            },
        )

        result = run_sync(data_dir)

        assert result == {"synced": 1}
        source = json.loads(source_path.read_text(encoding="utf-8"))
        gw_entry = source["mcpServers"]["artifact-gateway"]
        assert gw_entry["args"] == ["--data-dir", str(data_dir.resolve())]

        gw_config = json.loads(config_path.read_text(encoding="utf-8"))
        assert "github" in gw_config["mcpServers"]
        assert gw_config["_gateway_sync"]["data_dir"] == str(data_dir.resolve())
        assert (
            data_dir / "state" / "upstream_secrets" / "github.json"
        ).exists()
        assert not (
            missing_redirect_data_dir / "state" / "config.json"
        ).exists()

    def test_sync_uses_redirected_config_metadata_for_source_selection(
        self, tmp_path: Path
    ) -> None:
        custom_data_dir = (tmp_path / "custom-data").resolve()
        source_a = (tmp_path / "source.json").resolve()
        source_b = (tmp_path / "source-b.json").resolve()
        data_dir, _config_path, source_a_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {"command": "sift-gateway"},
                "only-in-source-a": {"command": "a-tool"},
            },
            extra_gw_keys={
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source_a.resolve()),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(custom_data_dir),
                }
            },
        )
        _write_json(
            source_b,
            {
                "mcpServers": {
                    "artifact-gateway": {"command": "sift-gateway"},
                    "only-in-source-b": {"command": "b-tool"},
                }
            },
        )
        _write_json(
            custom_data_dir / "state" / "config.json",
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source_b),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(custom_data_dir),
                },
            },
        )

        result = run_sync(data_dir)

        assert result == {"synced": 1}
        custom_config = json.loads(
            (custom_data_dir / "state" / "config.json").read_text(
                encoding="utf-8"
            )
        )
        assert "only-in-source-b" in custom_config["mcpServers"]
        assert "only-in-source-a" not in custom_config["mcpServers"]
        assert custom_config["_gateway_sync"]["source_path"] == str(source_b)

        source_a_after = json.loads(source_a_path.read_text(encoding="utf-8"))
        assert "only-in-source-a" in source_a_after["mcpServers"]

        source_b_after = json.loads(source_b.read_text(encoding="utf-8"))
        assert list(source_b_after["mcpServers"]) == ["artifact-gateway"]


class TestSyncDisabled:
    def test_sync_skipped_when_disabled(self, tmp_path: Path) -> None:
        data_dir, _config_path, _source_path = _setup_gateway_and_source(
            tmp_path,
            gateway_servers={},
            source_servers={
                "artifact-gateway": {
                    "command": "sift-gateway",
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
        from sift_gateway.config.sync import (
            _is_gateway_entry,
        )

        assert _is_gateway_entry(
            "artifact-gateway",
            {"command": "anything"},
            "artifact-gateway",
        )

    def test_matches_by_command(self) -> None:
        from sift_gateway.config.sync import (
            _is_gateway_entry,
        )

        assert _is_gateway_entry(
            "some-name",
            {"command": "sift-gateway"},
            "artifact-gateway",
        )

    def test_url_with_sift_is_not_gateway(self) -> None:
        """URL substring no longer identifies gateway entries."""
        from sift_gateway.config.sync import (
            _is_gateway_entry,
        )

        assert not _is_gateway_entry(
            "my-gateway",
            {"url": "http://localhost:8080/sift"},
            "artifact-gateway",
        )

    def test_non_gateway_entry(self) -> None:
        from sift_gateway.config.sync import (
            _is_gateway_entry,
        )

        assert not _is_gateway_entry(
            "github",
            {"command": "npx", "args": ["server-github"]},
            "artifact-gateway",
        )
