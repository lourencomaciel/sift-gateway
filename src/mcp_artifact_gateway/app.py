"""Composition root: config -> db -> fs -> upstreams -> MCP server."""

from __future__ import annotations

import asyncio
from pathlib import Path

from mcp_artifact_gateway.config import load_gateway_config
from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.db.conn import create_pool
from mcp_artifact_gateway.db.migrate import apply_migrations
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.lifecycle import CheckResult, run_startup_check
from mcp_artifact_gateway.mcp.server import GatewayServer, bootstrap_server


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "db" / "migrations"


def build_app(
    *,
    data_dir_override: str | None = None,
    config: GatewayConfig | None = None,
    startup_report: CheckResult | None = None,
) -> tuple[GatewayServer, "ConnectionPool"]:  # type: ignore[name-defined]
    """Wire all components and return a ready-to-run server + pool.

    Either provide *config* directly or let it be loaded via
    *data_dir_override*.  If *startup_report* is provided it is reused;
    otherwise ``run_startup_check`` is called.  Returns ``(server, pool)``
    — caller is responsible for calling ``pool.close()`` on shutdown.
    """
    from psycopg_pool import ConnectionPool  # noqa: F811 — deferred for type stub

    if config is None:
        config = load_gateway_config(data_dir_override=data_dir_override)

    report = startup_report or run_startup_check(config)
    if not report.ok:
        raise RuntimeError(
            "Startup checks failed: " + "; ".join(report.details)
        )

    pool: ConnectionPool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        server = asyncio.run(
            bootstrap_server(
                config,
                db_pool=pool,
                blob_store=BlobStore(
                    config.blobs_bin_dir,
                    probe_bytes=config.binary_probe_bytes,
                ),
                fs_ok=report.fs_ok,
                db_ok=report.db_ok,
            )
        )
    except BaseException:
        pool.close()
        raise

    return server, pool
