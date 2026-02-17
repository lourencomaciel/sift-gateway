from __future__ import annotations

from pathlib import Path

import pytest

from sift_mcp.config.init_source import (
    find_source_shortcut_matches,
    resolve_init_source,
    resolve_source_arg,
    resolve_source_shortcut,
)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"mcpServers":{"dummy":{"command":"npx"}}}',
        encoding="utf-8",
    )


def test_resolve_source_arg_passthrough_path(tmp_path: Path) -> None:
    source = tmp_path / "config.json"
    _touch(source)

    resolved = resolve_source_arg(str(source))

    assert resolved == source.resolve()


def test_resolve_source_arg_uses_cwd_for_relative_path(tmp_path: Path) -> None:
    source = tmp_path / "config.json"
    _touch(source)

    resolved = resolve_source_arg("config.json", cwd=tmp_path)

    assert resolved == source.resolve()


def test_resolve_source_shortcut_claude_single_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "macos",
    )
    source = (
        tmp_path
        / "Library"
        / "Application Support"
        / "Claude"
        / "claude_desktop_config.json"
    )
    _touch(source)

    resolved = resolve_source_shortcut("claude")

    assert resolved == source.resolve()


def test_resolve_source_shortcut_claude_code_ambiguous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "linux",
    )
    cwd = tmp_path / "workspace" / "repo" / "pkg"
    cwd.mkdir(parents=True)

    first = tmp_path / "workspace" / ".mcp.json"
    second = tmp_path / "workspace" / "repo" / ".mcp.json"
    _touch(first)
    _touch(second)

    with pytest.raises(ValueError, match="matched multiple MCP config files"):
        resolve_source_shortcut("claude-code", cwd=cwd)


def test_resolve_source_shortcut_missing_returns_checked_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "linux",
    )

    with pytest.raises(ValueError, match="did not match any known MCP config"):
        resolve_source_shortcut("claude")


def test_find_source_shortcut_matches_auto_collects_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "linux",
    )
    claude = tmp_path / ".config" / "Claude" / "claude_desktop_config.json"
    zed = tmp_path / ".config" / "zed" / "settings.json"
    _touch(claude)
    _touch(zed)

    matches, _checked = find_source_shortcut_matches("auto", cwd=tmp_path)

    resolved = {str(path) for path in matches}
    assert str(claude.resolve()) in resolved
    assert str(zed.resolve()) in resolved


def test_find_source_shortcut_matches_accepts_zed_context_servers_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "linux",
    )
    zed = tmp_path / ".config" / "zed" / "settings.json"
    zed.parent.mkdir(parents=True, exist_ok=True)
    zed.write_text(
        '{"context_servers":{"gateway":{"command":"sift-mcp"}}}',
        encoding="utf-8",
    )

    matches, _checked = find_source_shortcut_matches("zed", cwd=tmp_path)

    assert matches == [zed.resolve()]


def test_resolve_init_source_missing_shortcut_raises_file_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "linux",
    )

    with pytest.raises(FileNotFoundError, match="did not match any known MCP"):
        resolve_init_source("claude")


def test_resolve_source_shortcut_vscode_ignores_non_mcp_settings_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "sift_mcp.config.init_source._platform_key",
        lambda: "macos",
    )
    base = tmp_path / "Library" / "Application Support" / "Code" / "User"
    settings = base / "settings.json"
    mcp = base / "mcp.json"
    settings.parent.mkdir(parents=True, exist_ok=True)
    settings.write_text('{"editor.tabSize": 2}', encoding="utf-8")
    mcp.write_text(
        '{"mcpServers":{"github":{"command":"npx"}}}',
        encoding="utf-8",
    )

    resolved = resolve_source_shortcut("vscode")

    assert resolved == mcp.resolve()
