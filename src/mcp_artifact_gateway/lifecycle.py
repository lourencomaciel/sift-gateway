"""Startup / shutdown lifecycle management for the MCP Artifact Gateway.

Provides helpers to bootstrap the required directory tree on first run and to
probe filesystem health at any point during operation.

File I/O is synchronous because the operations target a local filesystem where
system-call latency is negligible.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from mcp_artifact_gateway.config.settings import GatewayConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _required_dirs(config: GatewayConfig) -> dict[str, Path]:
    """Return a mapping of logical name to directory path for every directory
    the gateway requires at runtime."""
    return {
        "state_dir": config.state_dir,
        "resources_dir": config.resources_dir,
        "blobs_bin_dir": config.blobs_bin_dir,
        "tmp_dir": config.tmp_dir,
        "logs_dir": config.logs_dir,
    }


def _verify_writable(directory: Path) -> None:
    """Create and immediately remove a probe file inside *directory*.

    Raises :class:`OSError` (or a subclass) if the directory is not writable.
    """
    probe = directory / f".gateway_probe_{uuid.uuid4().hex}"
    try:
        probe.write_bytes(b"ok")
    finally:
        # Always attempt cleanup, but don't mask the original error.
        try:
            probe.unlink(missing_ok=True)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def ensure_directories(config: GatewayConfig) -> None:
    """Create all directories required by the gateway and verify each is
    writable.

    This function is idempotent: it can be called on every startup without
    side-effects if the directories already exist.

    Parameters
    ----------
    config:
        The active gateway configuration whose derived path properties
        (``state_dir``, ``resources_dir``, etc.) define the directory layout.

    Raises
    ------
    OSError
        If any directory cannot be created or is not writable.
    """
    for name, directory in _required_dirs(config).items():
        directory.mkdir(parents=True, exist_ok=True)
        _verify_writable(directory)


async def check_filesystem_health(config: GatewayConfig) -> dict[str, str]:
    """Probe every required directory and report its status.

    Returns
    -------
    dict[str, str]
        A mapping of directory logical name to either ``"ok"`` or a
        human-readable error string.  Example::

            {
                "state_dir": "ok",
                "resources_dir": "ok",
                "blobs_bin_dir": "error: [Errno 13] Permission denied: ...",
                "tmp_dir": "ok",
                "logs_dir": "ok",
            }
    """
    results: dict[str, str] = {}
    for name, directory in _required_dirs(config).items():
        try:
            if not directory.exists():
                results[name] = f"error: directory does not exist: {directory}"
                continue
            if not directory.is_dir():
                results[name] = f"error: path is not a directory: {directory}"
                continue
            _verify_writable(directory)
            results[name] = "ok"
        except OSError as exc:
            results[name] = f"error: {exc}"
    return results
