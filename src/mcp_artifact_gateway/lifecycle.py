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

from dataclasses import dataclass
import logging
from pathlib import Path
import tempfile

from mcp_artifact_gateway.config.settings import GatewayConfig

_logger = logging.getLogger(__name__)


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
                try:
                    probe.unlink(missing_ok=True)
                except OSError:
                    pass
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


def _check_migrations(connection: object, details: list[str]) -> None:
    """Append migration status info to *details*.

    This is purely informational and never causes ``db_ok`` to
    become ``False``.

    Args:
        connection: Open database connection with cursor support.
        details: Mutable list to which diagnostic strings are
            appended.
    """
    try:
        migrations_dir = Path(__file__).resolve().parent / "db" / "migrations"
        if not migrations_dir.is_dir():
            return

        from mcp_artifact_gateway.db.migrate import list_migrations

        available_names = {p.name for p in list_migrations(migrations_dir)}

        with connection.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(
                "SELECT EXISTS ("
                "SELECT FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_name = 'schema_migrations')"
            )
            row = cur.fetchone()
            if not row or not row[0]:
                pending_count = len(available_names)
                details.append(
                    f"migrations: {pending_count}"
                    " pending (table not initialized)"
                )
                return
            cur.execute("SELECT migration_name FROM schema_migrations")
            applied = {str(r[0]) for r in cur.fetchall()}

        pending = sorted(available_names - applied)
        if pending:
            names = ", ".join(pending[:3])
            suffix = ", ..." if len(pending) > 3 else ""
            details.append(
                f"migrations: {len(pending)} pending ({names}{suffix})"
            )
    except Exception:
        _logger.warning("migration check failed", exc_info=True)


def _check_db(config: GatewayConfig) -> tuple[bool, list[str]]:
    """Dispatch database connectivity check to the correct backend.

    Args:
        config: Gateway configuration specifying the backend.

    Returns:
        A ``(ok, details)`` tuple from the backend-specific check.
    """
    if config.db_backend == "sqlite":
        return _check_sqlite(config)
    return _check_postgres(config)


def _check_sqlite(config: GatewayConfig) -> tuple[bool, list[str]]:
    """Verify SQLite connectivity with a simple probe query.

    Args:
        config: Gateway configuration with ``sqlite_path``.

    Returns:
        A ``(ok, details)`` tuple.
    """
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
    """Verify Postgres connectivity and optionally check migrations.

    Args:
        config: Gateway configuration with ``postgres_dsn``.

    Returns:
        A ``(ok, details)`` tuple.
    """
    from mcp_artifact_gateway.db.conn import connect

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
        _check_migrations(connection, details)
    except Exception as exc:
        details.append(f"DB probe query failed: {exc}")
        return False, details
    finally:
        try:
            connection.close()
        except Exception:
            _logger.warning(
                "failed to close DB connection during startup check",
                exc_info=True,
            )

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
