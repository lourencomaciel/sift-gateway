"""Run startup lifecycle checks for gateway bootability.

Verify that the filesystem directories are writable, the database
backend is reachable, and upstream configurations are valid before
the gateway begins serving requests.  Exports ``CheckResult`` and
``run_startup_check``.

Typical usage example::

    config = load_gateway_config()
    result = run_startup_check(config)
    if not result.ok:
        print(result.details)
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from pathlib import Path
import tempfile

from sift_gateway.config.settings import GatewayConfig


@dataclass(frozen=True)
class CheckResult:
    """Aggregate result of all startup health checks.

    Each subsystem is checked independently, and partial failures
    are recorded in ``details`` for diagnostic output.

    Attributes:
        fs_ok: True if all required directories are writable.
        db_ok: True if the database backend is reachable.
        upstream_ok: True if upstream configs are valid.
        details: Human-readable diagnostic messages.
    """

    fs_ok: bool
    db_ok: bool
    upstream_ok: bool
    details: list[str]

    @property
    def ok(self) -> bool:
        """Whether all startup checks passed."""
        return self.fs_ok and self.db_ok and self.upstream_ok


def ensure_data_dirs(config: GatewayConfig) -> list[Path]:
    """Create required data directories if they do not exist.

    Args:
        config: Gateway configuration providing directory paths.

    Returns:
        List of directory paths that were ensured to exist.
    """
    paths = [
        config.state_dir,
        config.resources_dir,
        config.blobs_bin_dir,
        config.blobs_payload_dir,
        config.tmp_dir,
        config.logs_dir,
    ]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _check_writable(paths: list[Path]) -> tuple[bool, list[str]]:
    """Verify that each path is writable via a temp-file probe.

    Args:
        paths: Directories to test for write access.

    Returns:
        A ``(ok, details)`` tuple where *ok* is ``False`` on
        the first unwritable directory and *details* contains
        the diagnostic message.
    """
    details: list[str] = []
    for path in paths:
        probe: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path,
                prefix=".gateway-write-check-",
                delete=False,
            ) as handle:
                probe = Path(handle.name)
                handle.write("ok")
            if probe is not None:
                probe.unlink(missing_ok=True)
        except OSError as exc:
            details.append(f"FS write failed at {path}: {exc}")
            if probe is not None:
                with contextlib.suppress(OSError):
                    probe.unlink(missing_ok=True)
            return False, details
    return True, details


def _validate_upstreams(config: GatewayConfig) -> tuple[bool, list[str]]:
    """Validate upstream configurations for duplicates and completeness.

    Args:
        config: Gateway configuration with upstream definitions.

    Returns:
        A ``(ok, details)`` tuple where *ok* is ``False`` when
        duplicate prefixes or missing transport fields are found.
    """
    details: list[str] = []
    prefixes = [upstream.prefix for upstream in config.upstreams]
    if len(prefixes) != len(set(prefixes)):
        details.append("duplicate upstream prefixes")
        return False, details

    for upstream in config.upstreams:
        if upstream.transport == "stdio" and not upstream.command:
            details.append(
                f"upstream '{upstream.prefix}' missing"
                " command for stdio transport"
            )
            return False, details
        if upstream.transport == "http" and not upstream.url:
            details.append(
                f"upstream '{upstream.prefix}' missing url for http transport"
            )
            return False, details
    return True, details


def _check_db(config: GatewayConfig) -> tuple[bool, list[str]]:
    """Verify SQLite connectivity with a simple probe query.

    Args:
        config: Gateway configuration with ``sqlite_path``.

    Returns:
        A ``(ok, details)`` tuple.
    """
    import sqlite3

    details: list[str] = []
    try:
        with sqlite3.connect(str(config.sqlite_path)) as conn:
            conn.execute("SELECT 1")
            table_rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_names = {
                row[0] for row in table_rows if row and isinstance(row[0], str)
            }
            core_tables = {"artifacts", "payload_blobs"}

            # Fresh bootstrap: migrations haven't run yet.
            if not (table_names & core_tables):
                return True, details

            if not core_tables.issubset(table_names):
                details.append(
                    "Database schema is outdated or incomplete. "
                    "Delete state/gateway.db and re-run 'sift-gateway init'."
                )
                return False, details

            required_v2_tables = {"artifacts_fts", "artifact_lineage_edges"}
            missing_v2_tables = sorted(required_v2_tables - table_names)
            if missing_v2_tables:
                details.append(
                    "Database schema is outdated. "
                    "Delete state/gateway.db and re-run 'sift-gateway init'."
                )
                return False, details

            try:
                conn.execute("SELECT kind, derivation FROM artifacts LIMIT 0")
                conn.execute(
                    "SELECT payload_fs_path FROM payload_blobs LIMIT 0"
                )
            except sqlite3.OperationalError:
                details.append(
                    "Database schema is outdated. "
                    "Delete state/gateway.db and re-run 'sift-gateway init'."
                )
                return False, details
    except Exception as exc:
        details.append(f"SQLite check failed: {exc}")
        return False, details
    return True, details


def run_startup_check(config: GatewayConfig) -> CheckResult:
    """Execute all startup health checks and aggregate results.

    Checks filesystem writability, database connectivity, and
    upstream configuration validity.

    Args:
        config: Gateway configuration to validate.

    Returns:
        A ``CheckResult`` summarising each subsystem's health.
    """
    details: list[str] = []

    try:
        dirs = ensure_data_dirs(config)
        fs_ok, fs_details = _check_writable(dirs)
        details.extend(fs_details)
    except OSError as exc:
        fs_ok = False
        details.append(f"FS check failed: {exc}")

    db_ok, db_details = _check_db(config)
    details.extend(db_details)

    upstream_ok, upstream_details = _validate_upstreams(config)
    details.extend(upstream_details)

    return CheckResult(
        fs_ok=fs_ok,
        db_ok=db_ok,
        upstream_ok=upstream_ok,
        details=details,
    )
