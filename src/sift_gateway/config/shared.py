"""Shared helpers for gateway config path and command detection."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any

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


def write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically write a JSON object with stable formatting.

    Args:
        path: Destination file path.
        data: Dict to serialize.
    """
    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd, tmp_path_raw = tempfile.mkstemp(
        dir=str(path.parent),
        suffix=".tmp",
    )
    tmp_path = Path(tmp_path_raw)
    try:
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        fd = -1
        tmp_path.replace(path)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


def load_gateway_config_dict(config_path: Path) -> dict[str, Any]:
    """Load existing gateway config file as a dict.

    Args:
        config_path: Path to ``config.json``.

    Returns:
        Parsed config dict, or ``{}`` if file is missing or invalid.
    """
    if not config_path.exists():
        return {}
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw
