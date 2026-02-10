"""Compose the MCP Artifact Gateway application stack.

Build the full runtime from configuration through database backend,
filesystem blob store, upstream connections, and the MCP server
instance.  Exports ``build_app`` as the primary entry point.

Typical usage example::

    server, backend = build_app(data_dir_override="./data")
    try:
        app = server.build_fastmcp_app()
        app.run()
    finally:
        backend.close()
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from mcp_artifact_gateway.config import load_gateway_config
from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.db.backend import (
    DatabaseBackend,
    PostgresBackend,
    SqliteBackend,
)
from mcp_artifact_gateway.db.migrate import apply_migrations
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.lifecycle import CheckResult, run_startup_check
from mcp_artifact_gateway.mcp.server import GatewayServer, bootstrap_server


def _migrations_dir(db_backend_name: str) -> Path:
    """Return the migrations directory for the given backend.

    Args:
        db_backend_name: Database backend identifier
            (``"sqlite"`` or ``"postgres"``).

    Returns:
        Path to the backend-specific migrations directory.
    """
    base = Path(__file__).resolve().parent / "db"
    if db_backend_name == "sqlite":
        return base / "migrations_sqlite"
    return base / "migrations"


def _create_backend(config: GatewayConfig) -> DatabaseBackend:
    """Create the database backend matching the configuration.

    Args:
        config: Gateway configuration specifying which backend
            to use and its connection parameters.

    Returns:
        A ``PostgresBackend`` or ``SqliteBackend`` instance.
    """
    if config.db_backend == "postgres":
        from mcp_artifact_gateway.db.conn import create_pool

        pool = create_pool(config)
        return PostgresBackend(pool=pool)
    return SqliteBackend(
        db_path=config.sqlite_path,
        busy_timeout_ms=config.sqlite_busy_timeout_ms,
    )


def build_app(
    *,
    data_dir_override: str | None = None,
    config: GatewayConfig | None = None,
    startup_report: CheckResult | None = None,
) -> tuple[GatewayServer, Any]:
    """Wire all components and return a ready-to-run server.

    Either provide *config* directly or let it be loaded via
    *data_dir_override*.  If *startup_report* is provided it is
    reused; otherwise ``run_startup_check`` is called.

    Args:
        data_dir_override: Optional filesystem path overriding
            the default data directory.
        config: Pre-loaded gateway configuration. When ``None``,
            configuration is loaded using *data_dir_override*.
        startup_report: Pre-computed startup check result.
            When ``None``, ``run_startup_check`` is called.

    Returns:
        A ``(server, backend)`` tuple. The caller must call
        ``backend.close()`` on shutdown.

    Raises:
        RuntimeError: If startup checks fail.
    """
    if config is None:
        config = load_gateway_config(data_dir_override=data_dir_override)

    report = startup_report or run_startup_check(config)
    if not report.ok:
        raise RuntimeError(
            "Startup checks failed: " + "; ".join(report.details)
        )

    backend = _create_backend(config)
    try:
        with backend.connection() as conn:
            apply_migrations(
                conn,
                _migrations_dir(config.db_backend),
                param_marker=backend.dialect.param_marker,
            )

        server = asyncio.run(
            bootstrap_server(
                config,
                db_pool=backend,
                blob_store=BlobStore(
                    config.blobs_bin_dir,
                    probe_bytes=config.binary_probe_bytes,
                ),
                fs_ok=report.fs_ok,
                db_ok=report.db_ok,
            )
        )
    except BaseException:
        backend.close()
        raise

    return server, backend
