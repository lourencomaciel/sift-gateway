from __future__ import annotations

import json
from pathlib import Path

import pytest

from sift_mcp.config.init_source import resolve_init_source


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_resolve_init_source_uses_literal_path(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.json"
    _write_json(source, {"mcpServers": {"gh": {"command": "gh"}}})
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    resolved = resolve_init_source(str(source), cwd=tmp_path)

    assert resolved == source.resolve()


def test_resolve_init_source_accepts_claude_shortcut(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    source = (
        home
        / "Library"
        / "Application Support"
        / "Claude"
        / "claude_desktop_config.json"
    )
    _write_json(source, {"mcpServers": {"gh": {"command": "gh"}}})

    resolved = resolve_init_source("claude", cwd=tmp_path)

    assert resolved == source.resolve()


def test_resolve_init_source_skips_non_mcp_candidates(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Linux")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    non_mcp = tmp_path / ".mcp.json"
    _write_json(non_mcp, {"name": "not-an-mcp-config"})
    valid = home / ".mcp.json"
    _write_json(valid, {"mcpServers": {}})

    resolved = resolve_init_source("claude-code", cwd=tmp_path)

    assert resolved == valid.resolve()


def test_resolve_init_source_auto_prefers_claude_order(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    claude = (
        home
        / "Library"
        / "Application Support"
        / "Claude"
        / "claude_desktop_config.json"
    )
    _write_json(claude, {"mcpServers": {"a": {"command": "a"}}})
    cursor = home / ".cursor" / "mcp.json"
    _write_json(cursor, {"mcpServers": {"b": {"command": "b"}}})

    resolved = resolve_init_source("auto", cwd=tmp_path)

    assert resolved == claude.resolve()


def test_resolve_init_source_shortcut_not_found_includes_checked_paths(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Linux")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))

    with pytest.raises(FileNotFoundError) as exc_info:
        resolve_init_source("cursor", cwd=tmp_path)

    message = str(exc_info.value)
    assert "shortcut 'cursor'" in message
    assert "Checked these paths:" in message
    assert str(home / ".cursor" / "mcp.json") in message
    assert "Library/Application Support/Cursor/mcp.json" not in message


def test_resolve_init_source_codex_prefix_is_not_shortcut(
    tmp_path: Path, monkeypatch
) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))

    resolved = resolve_init_source("codex/claude", cwd=tmp_path)

    assert resolved == (tmp_path / "codex" / "claude").resolve()


def test_resolve_init_source_vscode_shortcut_mac(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    source = (
        home
        / "Library"
        / "Application Support"
        / "Code"
        / "User"
        / "mcp.json"
    )
    _write_json(source, {"mcp": {"servers": {"gh": {"command": "gh"}}}})

    resolved = resolve_init_source("vscode", cwd=tmp_path)

    assert resolved == source.resolve()


def test_resolve_init_source_windsurf_shortcut_linux(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Linux")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    source = home / ".codeium" / "windsurf" / "mcp_config.json"
    _write_json(source, {"mcpServers": {"gh": {"command": "gh"}}})

    resolved = resolve_init_source("windsurf", cwd=tmp_path)

    assert resolved == source.resolve()


def test_resolve_init_source_zed_shortcut_windows(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Windows")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    appdata = tmp_path / "appdata"
    monkeypatch.setenv("APPDATA", str(appdata))
    source = appdata / "Zed" / "settings.json"
    _write_json(
        source,
        {
            "context_servers": {
                "gh": {"command": "gh"},
            }
        },
    )

    resolved = resolve_init_source("zed", cwd=tmp_path)

    assert resolved == source.resolve()
