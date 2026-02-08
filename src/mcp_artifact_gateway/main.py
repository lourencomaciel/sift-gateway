"""CLI entrypoint for MCP Artifact Gateway."""

from __future__ import annotations

import argparse
import sys

from mcp_artifact_gateway.config import load_gateway_config
from mcp_artifact_gateway.lifecycle import run_startup_check


def cli() -> None:
    """Main CLI entrypoint."""
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
    args = parser.parse_args()

    config = load_gateway_config(data_dir_override=args.data_dir)

    if args.check:
        report = run_startup_check(config)
        print(f"fs_ok={report.fs_ok}")
        print(f"db_ok={report.db_ok}")
        print(f"upstream_ok={report.upstream_ok}")
        if report.details:
            for item in report.details:
                print(f"- {item}")
        sys.exit(0 if report.ok else 1)

    print("mcp-gateway serve: not yet implemented")
    sys.exit(0)
