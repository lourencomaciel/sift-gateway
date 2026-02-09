"""Tests for mcp-gateway init --from migration command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mcp_artifact_gateway.config.init import run_init, run_revert


def _claude_desktop_config() -> dict:
    return {
        "mcpServers": {
            "github": {
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-github"],
                "env": {"GITHUB_TOKEN": "ghp_secret123"},
            },
            "filesystem": {
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
            },
        },
        "someOtherKey": "preserved",
    }


class TestRunInit:
    def test_migrates_servers_to_gateway_config(self, tmp_path: Path) -> None:
        source = tmp_path / "claude_desktop_config.json"
        source.write_text(json.dumps(_claude_desktop_config()), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        summary = run_init(source, data_dir=data_dir)

        assert sorted(summary["servers_migrated"]) == ["filesystem", "github"]

        # Gateway config should have the servers
        gw_config = json.loads(Path(summary["gateway_config_path"]).read_text())
        assert "github" in gw_config["mcpServers"]
        assert "filesystem" in gw_config["mcpServers"]
        assert gw_config["mcpServers"]["github"]["env"]["GITHUB_TOKEN"] == "ghp_secret123"

    def test_rewrites_source_with_gateway_only(self, tmp_path: Path) -> None:
        source = tmp_path / "claude_desktop_config.json"
        source.write_text(json.dumps(_claude_desktop_config()), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        run_init(source, data_dir=data_dir)

        # Source should now have only the gateway
        rewritten = json.loads(source.read_text())
        assert "artifact-gateway" in rewritten["mcpServers"]
        assert len(rewritten["mcpServers"]) == 1
        assert rewritten["mcpServers"]["artifact-gateway"]["command"] == "mcp-gateway"

    def test_preserves_non_mcp_keys_in_source(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        source.write_text(json.dumps(_claude_desktop_config()), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        run_init(source, data_dir=data_dir)

        rewritten = json.loads(source.read_text())
        assert rewritten["someOtherKey"] == "preserved"

    def test_creates_backup(self, tmp_path: Path) -> None:
        source = tmp_path / "claude_desktop_config.json"
        original_content = json.dumps(_claude_desktop_config())
        source.write_text(original_content, encoding="utf-8")
        data_dir = tmp_path / "gateway"

        summary = run_init(source, data_dir=data_dir)

        backup = Path(summary["backup_path"])
        assert backup.exists()
        assert json.loads(backup.read_text()) == _claude_desktop_config()

    def test_custom_gateway_name(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        source.write_text(json.dumps(_claude_desktop_config()), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        run_init(source, data_dir=data_dir, gateway_name="my-gateway")

        rewritten = json.loads(source.read_text())
        assert "my-gateway" in rewritten["mcpServers"]
        assert "artifact-gateway" not in rewritten["mcpServers"]

    def test_dry_run_makes_no_changes(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        original = _claude_desktop_config()
        source.write_text(json.dumps(original), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        summary = run_init(source, data_dir=data_dir, dry_run=True)

        assert sorted(summary["servers_migrated"]) == ["filesystem", "github"]
        # Source unchanged
        assert json.loads(source.read_text()) == original
        # No backup created
        assert not Path(summary["backup_path"]).exists()
        # No gateway config created
        assert not Path(summary["gateway_config_path"]).exists()

    def test_existing_gateway_config_preserved(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        source.write_text(json.dumps({
            "mcpServers": {"new_tool": {"command": "new-tool"}}
        }), encoding="utf-8")

        data_dir = tmp_path / "gateway"
        state_dir = data_dir / "state"
        state_dir.mkdir(parents=True)
        (state_dir / "config.json").write_text(json.dumps({
            "mcpServers": {"existing": {"command": "existing-cmd"}},
            "postgres_dsn": "postgresql://localhost/db",
        }))

        run_init(source, data_dir=data_dir)

        gw_config = json.loads((state_dir / "config.json").read_text())
        # Existing server preserved (wins over import)
        assert gw_config["mcpServers"]["existing"]["command"] == "existing-cmd"
        # New server added
        assert gw_config["mcpServers"]["new_tool"]["command"] == "new-tool"
        # Other config preserved
        assert gw_config["postgres_dsn"] == "postgresql://localhost/db"

    def test_empty_source_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "empty.json"
        source.write_text(json.dumps({"mcpServers": {}}), encoding="utf-8")

        with pytest.raises(ValueError, match="no mcpServers found"):
            run_init(source, data_dir=tmp_path / "gateway")

    def test_missing_source_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            run_init(tmp_path / "nonexistent.json", data_dir=tmp_path / "gateway")

    def test_vscode_format_source(self, tmp_path: Path) -> None:
        source = tmp_path / "mcp.json"
        source.write_text(json.dumps({
            "mcp": {"servers": {"github": {"command": "gh"}}}
        }), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        summary = run_init(source, data_dir=data_dir)
        assert summary["servers_migrated"] == ["github"]

        # Rewritten source should preserve VS Code format
        rewritten = json.loads(source.read_text())
        assert "mcpServers" not in rewritten
        assert "artifact-gateway" in rewritten["mcp"]["servers"]

    def test_tilde_expansion(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))
        source = tmp_path / "config.json"
        source.write_text(json.dumps({
            "mcpServers": {"gh": {"command": "gh"}}
        }), encoding="utf-8")

        summary = run_init(Path("~/config.json"), data_dir=tmp_path / "gateway")
        assert summary["servers_migrated"] == ["gh"]


class TestRunRevert:
    def test_restores_from_backup(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        original = _claude_desktop_config()
        source.write_text(json.dumps(original), encoding="utf-8")
        data_dir = tmp_path / "gateway"

        # Migrate
        run_init(source, data_dir=data_dir)
        assert json.loads(source.read_text()) != original

        # Revert
        result = run_revert(source)
        assert json.loads(source.read_text()) == original
        # Backup should be removed
        assert not Path(result["backup_path"]).exists()

    def test_revert_missing_backup_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "config.json"
        source.write_text("{}", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="no backup found"):
            run_revert(source)
