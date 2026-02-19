"""Resolve ``init --from``/``upstream add --from`` source shortcuts."""

from __future__ import annotations

import os
from pathlib import Path
import platform

SUPPORTED_SOURCE_SHORTCUTS: tuple[str, ...] = (
    "auto",
    "claude",
    "claude-code",
    "cursor",
    "vscode",
    "windsurf",
    "zed",
)

_SOURCE_SHORTCUT_ALIASES: dict[str, str] = {
    "all": "auto",
    "claude_desktop": "claude",
    "claude-desktop": "claude",
    "claudecode": "claude-code",
    "claude_code": "claude-code",
    "vs-code": "vscode",
    "vs_code": "vscode",
    "code": "vscode",
}


def supported_source_shortcuts() -> tuple[str, ...]:
    """Return supported shortcut names for ``--from``."""
    return SUPPORTED_SOURCE_SHORTCUTS


def resolve_source_arg(
    source_arg: str | Path,
    *,
    cwd: Path | None = None,
) -> Path:
    """Resolve a user-provided ``--from`` argument to a concrete path.

    ``source_arg`` can be:
    - explicit path (absolute or relative)
    - a known shortcut (``claude``, ``claude-code``, ``cursor``,
      ``vscode``, ``windsurf``, ``zed``, ``auto``)
    """
    raw = str(source_arg).strip()
    root = cwd.expanduser().resolve() if cwd is not None else None
    if _looks_like_path(raw):
        return _resolve_literal_path(raw, cwd=root)

    shortcut = _normalize_shortcut(raw)
    if shortcut is None:
        return _resolve_literal_path(raw, cwd=root)
    return resolve_source_shortcut(shortcut, cwd=root)


def resolve_init_source(
    source: str,
    *,
    cwd: Path | None = None,
) -> Path:
    """Backward-compatible alias for init source resolution.

    Raises:
        FileNotFoundError: When a known shortcut has no matching config file.
    """
    root = cwd.expanduser().resolve() if cwd is not None else None
    shortcut = _normalize_shortcut(source)
    if shortcut is None:
        return _resolve_literal_path(source, cwd=root)
    try:
        return resolve_source_shortcut(shortcut, cwd=root)
    except ValueError as exc:
        if "did not match any known MCP config file" in str(exc):
            raise FileNotFoundError(str(exc)) from exc
        raise


def resolve_source_shortcut(
    shortcut: str,
    *,
    cwd: Path | None = None,
) -> Path:
    """Resolve a known shortcut to exactly one existing config file path."""
    normalized = _normalize_shortcut(shortcut)
    if normalized is None:
        msg = (
            "unsupported --from shortcut "
            f"'{shortcut}'. Supported: {', '.join(SUPPORTED_SOURCE_SHORTCUTS)}"
        )
        raise ValueError(msg)

    matches, checked = find_source_shortcut_matches(
        normalized,
        cwd=cwd,
    )
    if not matches:
        checked_list = "\n".join(f"  - {p}" for p in checked)
        msg = (
            f"shortcut '{normalized}' did not match any known MCP config file.\n"
            f"Checked:\n{checked_list}\n"
            "Pass an explicit path with --from <path>."
        )
        raise ValueError(msg)

    if len(matches) > 1:
        match_list = "\n".join(f"  - {p}" for p in matches)
        msg = (
            f"shortcut '{normalized}' matched multiple MCP config files:\n"
            f"{match_list}\n"
            "Pass an explicit path with --from <path>."
        )
        raise ValueError(msg)

    return matches[0]


def find_source_shortcut_matches(
    shortcut: str,
    *,
    cwd: Path | None = None,
) -> tuple[list[Path], list[Path]]:
    """Return ``(existing_matches, checked_candidates)`` for a shortcut."""
    normalized = _normalize_shortcut(shortcut)
    if normalized is None:
        msg = (
            "unsupported --from shortcut "
            f"'{shortcut}'. Supported: {', '.join(SUPPORTED_SOURCE_SHORTCUTS)}"
        )
        raise ValueError(msg)

    root = (
        cwd.expanduser().resolve() if cwd is not None else Path.cwd().resolve()
    )
    candidates = _candidate_paths_for_shortcut(normalized, cwd=root)
    checked = _dedupe_paths(candidates)
    matches = [path for path in checked if _is_mcp_config_file(path)]
    return matches, checked


def _candidate_paths_for_shortcut(
    shortcut: str,
    *,
    cwd: Path,
) -> list[Path]:
    if shortcut == "auto":
        all_candidates: list[Path] = []
        for name in SUPPORTED_SOURCE_SHORTCUTS:
            if name == "auto":
                continue
            all_candidates.extend(_candidate_paths_for_shortcut(name, cwd=cwd))
        return all_candidates

    if shortcut == "claude":
        return _claude_desktop_candidates()
    if shortcut == "claude-code":
        return _claude_code_candidates(cwd)
    if shortcut == "cursor":
        return _cursor_candidates(cwd)
    if shortcut == "vscode":
        return _vscode_candidates(cwd)
    if shortcut == "windsurf":
        return _windsurf_candidates(cwd)
    if shortcut == "zed":
        return _zed_candidates(cwd)

    msg = (
        "unsupported --from shortcut "
        f"'{shortcut}'. Supported: {', '.join(SUPPORTED_SOURCE_SHORTCUTS)}"
    )
    raise ValueError(msg)


def _claude_desktop_candidates() -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()
    platform = _platform_key()

    if platform == "macos":
        candidates.append(
            home
            / "Library"
            / "Application Support"
            / "Claude"
            / "claude_desktop_config.json"
        )
    elif platform == "windows":
        appdata = _windows_roaming_dir()
        if appdata is not None:
            candidates.append(appdata / "Claude" / "claude_desktop_config.json")
    else:
        candidates.append(
            home / ".config" / "Claude" / "claude_desktop_config.json"
        )
        candidates.append(
            home / ".config" / "claude" / "claude_desktop_config.json"
        )

    return candidates


def _claude_code_candidates(cwd: Path) -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()
    candidates.extend(
        [
            home / ".claude" / "settings.local.json",
            home / ".claude" / "settings.json",
            home / ".claude.json",
            home / ".mcp.json",
        ]
    )
    for directory in _cwd_and_parents(cwd):
        candidates.extend(
            [
                directory / ".mcp.json",
                directory / ".claude" / "settings.local.json",
                directory / ".claude" / "settings.json",
            ]
        )
    return candidates


def _cursor_candidates(cwd: Path) -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()
    platform = _platform_key()

    candidates.append(home / ".cursor" / "mcp.json")
    candidates.extend(
        directory / ".cursor" / "mcp.json"
        for directory in _cwd_and_parents(cwd)
    )

    if platform == "macos":
        base = home / "Library" / "Application Support" / "Cursor" / "User"
        candidates.extend([base / "mcp.json", base / "settings.json"])
    elif platform == "windows":
        appdata = _windows_roaming_dir()
        if appdata is not None:
            base = appdata / "Cursor" / "User"
            candidates.extend([base / "mcp.json", base / "settings.json"])
    else:
        base = home / ".config" / "Cursor" / "User"
        candidates.extend([base / "mcp.json", base / "settings.json"])

    return candidates


def _vscode_candidates(cwd: Path) -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()
    platform = _platform_key()

    for directory in _cwd_and_parents(cwd):
        candidates.extend(
            [
                directory / ".vscode" / "mcp.json",
                directory / ".vscode" / "settings.json",
            ]
        )

    if platform == "macos":
        base = home / "Library" / "Application Support" / "Code" / "User"
        candidates.extend([base / "mcp.json", base / "settings.json"])
    elif platform == "windows":
        appdata = _windows_roaming_dir()
        if appdata is not None:
            base = appdata / "Code" / "User"
            candidates.extend([base / "mcp.json", base / "settings.json"])
    else:
        base = home / ".config" / "Code" / "User"
        candidates.extend([base / "mcp.json", base / "settings.json"])

    return candidates


def _windsurf_candidates(cwd: Path) -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()
    platform = _platform_key()

    candidates.append(home / ".codeium" / "windsurf" / "mcp_config.json")
    for directory in _cwd_and_parents(cwd):
        candidates.extend(
            [
                directory / ".windsurf" / "mcp.json",
                directory / ".windsurf" / "settings.json",
            ]
        )

    if platform == "macos":
        base = home / "Library" / "Application Support" / "Windsurf"
        candidates.extend(
            [
                base / "mcp_config.json",
                base / "User" / "mcp.json",
                base / "User" / "settings.json",
            ]
        )
    elif platform == "windows":
        appdata = _windows_roaming_dir()
        if appdata is not None:
            candidates.extend(
                [
                    appdata / "Codeium" / "Windsurf" / "mcp_config.json",
                    appdata / "Windsurf" / "User" / "mcp.json",
                    appdata / "Windsurf" / "User" / "settings.json",
                ]
            )
    else:
        base = home / ".config" / "Windsurf"
        candidates.extend(
            [
                base / "mcp_config.json",
                base / "User" / "mcp.json",
                base / "User" / "settings.json",
            ]
        )

    return candidates


def _zed_candidates(cwd: Path) -> list[Path]:
    home = Path.home()
    platform = _platform_key()

    candidates: list[Path] = [
        directory / ".zed" / "settings.json"
        for directory in _cwd_and_parents(cwd)
    ]

    if platform == "macos":
        candidates.append(
            home / "Library" / "Application Support" / "Zed" / "settings.json"
        )
    elif platform == "windows":
        appdata = _windows_roaming_dir()
        if appdata is not None:
            candidates.append(appdata / "Zed" / "settings.json")
    else:
        candidates.append(home / ".config" / "zed" / "settings.json")

    return candidates


def _looks_like_path(value: str) -> bool:
    if value.startswith((".", "~")):
        return True
    if "/" in value or "\\" in value:
        return True
    return value.endswith(".json")


def _resolve_literal_path(
    value: str,
    *,
    cwd: Path | None,
) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if cwd is not None:
        return (cwd / path).resolve()
    return path.resolve()


def _normalize_shortcut(raw: str) -> str | None:
    lowered = raw.lower().strip().replace("_", "-").replace(" ", "-")
    if lowered in SUPPORTED_SOURCE_SHORTCUTS:
        return lowered
    alias = _SOURCE_SHORTCUT_ALIASES.get(lowered)
    if alias is not None:
        return alias
    return None


def _platform_key() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    if system == "windows":
        return "windows"
    return "linux"


def _windows_roaming_dir() -> Path | None:
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata).expanduser().resolve()
    if _platform_key() == "windows":
        return (Path.home() / "AppData" / "Roaming").resolve()
    return None


def _cwd_and_parents(cwd: Path) -> list[Path]:
    return [cwd, *cwd.parents]


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        resolved = path.expanduser().resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _is_mcp_config_file(path: Path) -> bool:
    """Return True when a file contains MCP config shape."""
    if not path.is_file():
        return False

    try:
        from sift_gateway.config.mcp_servers import (
            extract_mcp_servers,
            read_config_file,
        )

        raw = read_config_file(path)
        extract_mcp_servers(raw)
    except (OSError, ValueError):
        return False

    if "mcpServers" in raw:
        return isinstance(raw.get("mcpServers"), dict)

    mcp = raw.get("mcp")
    if isinstance(mcp, dict) and isinstance(mcp.get("servers"), dict):
        return True

    zed = raw.get("context_servers")
    return isinstance(zed, dict)
