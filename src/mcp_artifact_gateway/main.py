"""CLI entrypoint for MCP Artifact Gateway."""

from __future__ import annotations

import argparse
import sys


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

    if args.check:
        print("mcp-gateway --check: not yet implemented")
        sys.exit(0)

    print("mcp-gateway serve: not yet implemented")
    sys.exit(0)
