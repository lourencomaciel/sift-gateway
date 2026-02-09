"""Startup lifecycle checks for gateway bootability."""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.db.conn import connect

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    fs_ok: bool
    db_ok: bool
    upstream_ok: bool
    details: list[str]

    @property
    def ok(self) -> bool:
        return self.fs_ok and self.db_ok and self.upstream_ok


def ensure_data_dirs(config: GatewayConfig) -> list[Path]:
    paths = [
        config.state_dir,
        config.resources_dir,
        config.blobs_bin_dir,
        config.tmp_dir,
        config.logs_dir,
    ]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _check_writable(paths: list[Path]) -> tuple[bool, list[str]]:
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
                try:
                    probe.unlink(missing_ok=True)
                except OSError:
                    pass
            return False, details
    return True, details


def _validate_upstreams(config: GatewayConfig) -> tuple[bool, list[str]]:
    details: list[str] = []
    prefixes = [upstream.prefix for upstream in config.upstreams]
    if len(prefixes) != len(set(prefixes)):
        details.append("duplicate upstream prefixes")
        return False, details

    for upstream in config.upstreams:
        if upstream.transport == "stdio" and not upstream.command:
            details.append(f"upstream '{upstream.prefix}' missing command for stdio transport")
            return False, details
        if upstream.transport == "http" and not upstream.url:
            details.append(f"upstream '{upstream.prefix}' missing url for http transport")
            return False, details
    return True, details


def _check_db(config: GatewayConfig) -> tuple[bool, list[str]]:
    if config.db_backend == "sqlite":
        return _check_sqlite(config)
    return _check_postgres(config)


def _check_sqlite(config: GatewayConfig) -> tuple[bool, list[str]]:
    import sqlite3

    details: list[str] = []
    try:
        conn = sqlite3.connect(str(config.sqlite_path))
        conn.execute("SELECT 1")
        conn.close()
    except Exception as exc:
        details.append(f"SQLite check failed: {exc}")
        return False, details
    return True, details


def _check_postgres(config: GatewayConfig) -> tuple[bool, list[str]]:
    details: list[str] = []
    if not config.postgres_dsn.strip():
        details.append("postgres_dsn is empty")
        return False, details

    try:
        connection = connect(config)
    except Exception as exc:
        details.append(f"DB connect failed: {exc}")
        return False, details

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except Exception as exc:
        details.append(f"DB probe query failed: {exc}")
        return False, details
    finally:
        try:
            connection.close()
        except Exception:
            _logger.warning("failed to close DB connection during startup check", exc_info=True)

    return True, details


def run_startup_check(config: GatewayConfig) -> CheckResult:
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
