"""Shared helpers for gateway config path and command detection."""

from __future__ import annotations

from pathlib import Path

from sift_gateway.constants import CONFIG_FILENAME, STATE_SUBDIR


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
    return command_name in {"sift-gateway", "sift-gateway.exe"}
