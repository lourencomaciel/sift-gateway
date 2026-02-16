"""Resolve ``sift-mcp init --from`` source path shortcuts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform

_SHORTCUT_ALIASES = {
    "all": "auto",
    "auto": "auto",
    "claude": "claude",
    "claude-code": "claude-code",
    "claudecode": "claude-code",
    "cursor": "cursor",
    "vscode": "vscode",
    "windsurf": "windsurf",
    "zed": "zed",
}


def resolve_init_source(
    source: str,
    *,
    cwd: Path | None = None,
) -> Path:
    """Resolve an init source from either a literal path or a shortcut.

    Supported shortcuts:
    - ``claude``
    - ``cursor``
    - ``claude-code``
    - ``vscode``
    - ``windsurf``
    - ``zed``
    - ``auto`` (tries all supported shortcuts)

    Args:
        source: CLI value passed to ``--from``.
        cwd: Optional working directory for resolving relative literal paths.

    Returns:
        Absolute path to the selected config file.

    Raises:
        FileNotFoundError: If a shortcut is used and no matching file is found.
    """
    cwd_path = cwd.resolve() if cwd else Path.cwd().resolve()
    literal = _resolve_literal_path(source, cwd=cwd_path)
    if literal.is_file():
        return literal.resolve()

    shortcut = _normalize_shortcut(source)
    if shortcut is None:
        return literal.resolve()

    candidates = _candidate_paths(shortcut, cwd=cwd_path)
    for candidate in candidates:
        if candidate.is_file() and _looks_like_mcp_config(candidate):
            return candidate.resolve()

    checked = "\n".join(f"  - {path}" for path in candidates)
    msg = (
        f"no config file found for shortcut '{source}'.\n"
        "Checked these paths:\n"
        f"{checked}\n"
        "Pass an explicit file path to --from to override."
    )
    raise FileNotFoundError(msg)


def _normalize_shortcut(source: str) -> str | None:
    token = source.strip().lower()
    token = token.replace("_", "-").replace(" ", "-")
    return _SHORTCUT_ALIASES.get(token)


def _resolve_literal_path(source: str, *, cwd: Path) -> Path:
    path = Path(source).expanduser()
    if path.is_absolute():
        return path
    return cwd / path


def _looks_like_mcp_config(path: Path) -> bool:
    """Return True when file content looks like an MCP client config."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False

    if not isinstance(raw, dict):
        return False

    if "mcpServers" in raw:
        return True

    mcp_block = raw.get("mcp")
    if isinstance(mcp_block, dict) and "servers" in mcp_block:
        return True

    zed_block = raw.get("context_servers")
    return isinstance(zed_block, dict)


def _candidate_paths(shortcut: str, *, cwd: Path) -> list[Path]:
    home = Path.home()
    appdata = os.environ.get("APPDATA")
    os_name = _current_os()

    if shortcut == "auto":
        return _dedupe_paths(
            _candidate_paths("claude", cwd=cwd)
            + _candidate_paths("cursor", cwd=cwd)
            + _candidate_paths("claude-code", cwd=cwd)
            + _candidate_paths("vscode", cwd=cwd)
            + _candidate_paths("windsurf", cwd=cwd)
            + _candidate_paths("zed", cwd=cwd)
        )

    if shortcut == "claude":
        return _paths_for_os(
            os_name=os_name,
            mac=[
                home
                / "Library"
                / "Application Support"
                / "Claude"
                / "claude_desktop_config.json"
            ],
            linux=[
                home / ".config" / "Claude" / "claude_desktop_config.json"
            ],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Claude", "claude_desktop_config.json"),
            ),
        )

    if shortcut == "cursor":
        return _paths_for_os(
            os_name=os_name,
            always=[home / ".cursor" / "mcp.json"],
            mac=[
                home / "Library" / "Application Support" / "Cursor" / "mcp.json"
            ],
            linux=[home / ".config" / "Cursor" / "mcp.json"],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Cursor", "mcp.json"),
            ),
        )

    if shortcut == "claude-code":
        return _paths_for_os(
            os_name=os_name,
            always=[
                cwd / ".mcp.json",
                cwd / ".claude" / "settings.local.json",
                cwd / ".claude" / "settings.json",
                home / ".mcp.json",
                home / ".claude.json",
                home / ".claude" / "settings.local.json",
                home / ".claude" / "settings.json",
            ],
            mac=[
                home
                / "Library"
                / "Application Support"
                / "Claude Code"
                / "mcp.json"
            ],
            linux=[home / ".config" / "claude-code" / "mcp.json"],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Claude Code", "mcp.json"),
            ),
        )

    if shortcut == "vscode":
        return _paths_for_os(
            os_name=os_name,
            always=[cwd / ".vscode" / "mcp.json"],
            mac=[
                home / "Library" / "Application Support" / "Code" / "User"
                / "mcp.json"
            ],
            linux=[home / ".config" / "Code" / "User" / "mcp.json"],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Code", "User", "mcp.json"),
            ),
        )

    if shortcut == "windsurf":
        return _paths_for_os(
            os_name=os_name,
            always=[home / ".codeium" / "windsurf" / "mcp_config.json"],
            mac=[
                home
                / "Library"
                / "Application Support"
                / "Windsurf"
                / "mcp_config.json"
            ],
            linux=[
                home / ".config" / "Windsurf" / "mcp_config.json",
            ],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Codeium", "Windsurf", "mcp_config.json"),
            ),
        )

    if shortcut == "zed":
        return _paths_for_os(
            os_name=os_name,
            always=[cwd / ".zed" / "settings.json"],
            mac=[
                home
                / "Library"
                / "Application Support"
                / "Zed"
                / "settings.json"
            ],
            linux=[home / ".config" / "zed" / "settings.json"],
            windows=_windows_roaming_candidates(
                appdata=appdata,
                home=home,
                parts=("Zed", "settings.json"),
            ),
        )

    return []


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    deduped: list[Path] = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _current_os() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return "mac"
    if system == "linux":
        return "linux"
    if system == "windows":
        return "windows"
    return "other"


def _paths_for_os(
    *,
    os_name: str,
    always: list[Path] | None = None,
    mac: list[Path] | None = None,
    linux: list[Path] | None = None,
    windows: list[Path] | None = None,
) -> list[Path]:
    paths = list(always or [])
    if os_name == "mac":
        paths.extend(mac or [])
    elif os_name == "linux":
        paths.extend(linux or [])
    elif os_name == "windows":
        paths.extend(windows or [])
    return _dedupe_paths(paths)


def _windows_roaming_candidates(
    *,
    appdata: str | None,
    home: Path,
    parts: tuple[str, ...],
) -> list[Path]:
    candidates = [home / "AppData" / "Roaming" / Path(*parts)]
    if appdata:
        candidates.insert(0, Path(appdata) / Path(*parts))
    return candidates
