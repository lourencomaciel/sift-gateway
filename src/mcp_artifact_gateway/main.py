"""CLI entrypoint for MCP Artifact Gateway."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from mcp_artifact_gateway.config import load_gateway_config
from mcp_artifact_gateway.db.backend import PostgresBackend
from mcp_artifact_gateway.db.conn import create_pool
from mcp_artifact_gateway.db.migrate import apply_migrations
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.lifecycle import run_startup_check
from mcp_artifact_gateway.mcp.server import bootstrap_server


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mcp-gateway",
        description="MCP Artifact Gateway — local single-tenant MCP proxy",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate config, DB, FS, upstreams and exit",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .mcp_gateway/)",
    )
    return parser.parse_args()


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "db" / "migrations"


def serve() -> int:
    args = _parse_args()
    config = load_gateway_config(data_dir_override=args.data_dir)
    report = run_startup_check(config)

    if args.check:
        print(f"fs_ok={report.fs_ok}")
        print(f"db_ok={report.db_ok}")
        print(f"upstream_ok={report.upstream_ok}")
        if report.details:
            for item in report.details:
                print(f"- {item}")
        return 0 if report.ok else 1

    if not report.ok:
        for item in report.details:
            print(f"- {item}", file=sys.stderr)
        return 1

    pool = create_pool(config)
    backend = PostgresBackend(pool=pool)
    try:
        with backend.connection() as connection:
            apply_migrations(connection, _migrations_dir())

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
        app = server.build_fastmcp_app()
        app.run(show_banner=False)
    finally:
        backend.close()

    return 0


def cli() -> None:
    """Main CLI entrypoint."""
    try:
        exit_code = serve()
    except Exception as exc:
        print(f"mcp-gateway serve failed: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(exit_code)
