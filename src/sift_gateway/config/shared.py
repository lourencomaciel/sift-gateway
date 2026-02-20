"""Shared helpers for gateway config path and command detection."""

from __future__ import annotations

from pathlib import Path
import shutil
import sys

from sift_gateway.constants import CONFIG_FILENAME, STATE_SUBDIR

_SIFT_COMMAND_NAMES = ("sift-gateway", "sift-gateway.exe")


def _is_explicit_command_path(command: str) -> bool:
    """Return whether the command string is an explicit filesystem path."""
    return "/" in command or "\\" in command or command.startswith(".")


def _absolute_command_path(path: Path) -> str:
    """Return an absolute command path without dereferencing symlinks."""
    return str(path.expanduser().absolute())


def gateway_config_path(data_dir: Path) -> Path:
    """Return ``config.json`` path inside the gateway state directory."""
    return data_dir / STATE_SUBDIR / CONFIG_FILENAME


def ensure_gateway_config_path(data_dir: Path) -> Path:
    """Ensure gateway state directory exists and return ``config.json`` path."""
    state_dir = data_dir / STATE_SUBDIR
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / CONFIG_FILENAME


def is_sift_command(command: str) -> bool:
    """Return whether a command string invokes ``sift-gateway``."""
    command_name = Path(command).name.lower()
    return command_name in _SIFT_COMMAND_NAMES


def resolve_sift_command() -> str:
    """Return the best command to launch ``sift-gateway``.

    Prefer an absolute executable path because desktop MCP clients often run
    with a restricted ``PATH`` that omits user-local bin directories. Keep
    shim paths intact so upgrades that retarget symlinks do not stale the
    persisted command path.
    """
    argv0 = sys.argv[0] if sys.argv else ""
    if argv0 and is_sift_command(argv0):
        argv0_path = Path(argv0).expanduser()
        if _is_explicit_command_path(argv0):
            if argv0_path.is_file():
                return _absolute_command_path(argv0_path)
        else:
            found_from_argv = shutil.which(argv0)
            if found_from_argv:
                return _absolute_command_path(Path(found_from_argv))

    for command_name in _SIFT_COMMAND_NAMES:
        sibling = Path(sys.executable).with_name(command_name)
        if sibling.is_file():
            return _absolute_command_path(sibling)

    for command_name in _SIFT_COMMAND_NAMES:
        found = shutil.which(command_name)
        if found:
            return _absolute_command_path(Path(found))

    return "sift-gateway"
