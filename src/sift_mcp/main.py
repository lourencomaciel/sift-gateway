"""Provide the CLI entrypoint for the Sift.

Parse command-line arguments, dispatch to subcommands (``init``,
``--check``), and launch the MCP server.  Exports ``serve`` and
``cli`` as the primary entry points.

Typical usage example::

    # From the command line:
    sift-mcp --check
    sift-mcp init --from claude
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys
from typing import Any

from sift_mcp import __version__
from sift_mcp.config import load_gateway_config
from sift_mcp.config.init_source import resolve_init_source
from sift_mcp.lifecycle import run_startup_check


def _add_init_mode_group(
    init_parser: argparse.ArgumentParser,
) -> None:
    """Add mutually exclusive mode flags and init options.

    Args:
        init_parser: The ``init`` subcommand argument parser
            to which mode flags are added.
    """
    init_mode = init_parser.add_mutually_exclusive_group()
    init_mode.add_argument(
        "--revert",
        action="store_true",
        help="Restore the source file from its backup",
    )
    init_mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without changes",
    )
    init_parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .sift-mcp/)",
    )
    init_parser.add_argument(
        "--gateway-name",
        default="artifact-gateway",
        help="Name for the gateway entry in the rewritten source file",
    )
    init_parser.add_argument(
        "--db-backend",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help=(
            "Database backend for generated gateway config (default: sqlite)"
        ),
    )
    init_parser.add_argument(
        "--postgres-dsn",
        default=None,
        help=(
            "Postgres connection string (used when --db-backend=postgres; "
            "skips Docker auto-provisioning)"
        ),
    )
    init_parser.add_argument(
        "--gateway-url",
        default=None,
        help=(
            "URL for the gateway entry in the rewritten "
            "source file (uses URL transport instead of "
            "command)"
        ),
    )


def _add_upstream_subcommand(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the ``upstream`` subcommand tree.

    Args:
        sub: Subparser action from the root argument parser.
    """
    upstream_parser = sub.add_parser(
        "upstream",
        help="Manage upstream MCP servers",
    )
    upstream_sub = upstream_parser.add_subparsers(
        dest="upstream_command",
    )

    add_parser = upstream_sub.add_parser(
        "add",
        help="Add upstream(s) from a JSON mcpServers snippet",
    )
    add_parser.add_argument(
        "snippet",
        help=(
            "JSON mcpServers snippet, e.g. "
            '\'{"name": {"command": "npx", "args": [...]}}\''
        ),
    )
    add_parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override DATA_DIR (default: .sift-mcp/)",
    )
    add_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without changes",
    )


def _add_init_subcommand(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the ``init`` subcommand and its arguments.

    Args:
        sub: Subparser action from the root argument parser.
    """
    init_parser = sub.add_parser(
        "init",
        help=(
            "Migrate MCP server config from an external tool into the gateway"
        ),
    )
    init_parser.add_argument(
        "--from",
        dest="source",
        required=True,
        help=(
            "Path or shortcut to source config file "
            "(e.g., claude, claude-code, cursor, vscode, windsurf, zed, auto, "
            "/path/to/claude_desktop_config.json)"
        ),
    )
    _add_init_mode_group(init_parser)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the gateway CLI.

    Returns:
        Parsed argument namespace with command, flags, and
        subcommand-specific options.
    """
    parser = argparse.ArgumentParser(
        prog="sift-mcp",
        description=("Sift — local single-tenant MCP proxy"),
    )
    sub = parser.add_subparsers(dest="command")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate config, DB, FS, upstreams and exit",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the installed version and exit",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .sift-mcp/)",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Transport mode (default: stdio)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind for HTTP transports",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to bind for HTTP transports",
    )
    parser.add_argument(
        "--path",
        default="/mcp",
        help="URL path for HTTP transports",
    )
    parser.add_argument(
        "--auth-token",
        default=None,
        help=(
            "Bearer token for non-local HTTP access. "
            "Also reads SIFT_MCP_AUTH_TOKEN env var"
        ),
    )
    _add_init_subcommand(sub)
    _add_upstream_subcommand(sub)
    return parser.parse_args()


def _run_upstream_add(args: argparse.Namespace) -> int:
    """Handle the ``upstream add`` subcommand.

    Args:
        args: Parsed CLI arguments containing the JSON snippet
            and optional overrides.

    Returns:
        Exit code (``0`` on success).
    """
    import json as json_mod

    from sift_mcp.config.upstream_add import (
        print_add_summary,
        run_upstream_add,
    )

    try:
        raw = json_mod.loads(args.snippet)
    except json_mod.JSONDecodeError as exc:
        msg = f"invalid JSON snippet: {exc}"
        raise ValueError(msg) from exc

    if not isinstance(raw, dict):
        msg = "snippet must be a JSON object mapping server names to configs"
        raise ValueError(msg)

    data_dir = Path(args.data_dir).resolve() if args.data_dir else None
    summary = run_upstream_add(
        raw,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )
    print_add_summary(summary, dry_run=args.dry_run)
    return 0


def _run_init(args: argparse.Namespace) -> int:
    """Handle the ``init`` subcommand.

    Args:
        args: Parsed CLI arguments containing source path,
            mode flags, and optional overrides.

    Returns:
        Exit code (``0`` on success).
    """
    from sift_mcp.config.init import (
        print_init_summary,
        run_init,
        run_revert,
    )

    source_path = resolve_init_source(args.source)
    data_dir = Path(args.data_dir).resolve() if args.data_dir else None

    if args.revert:
        result = run_revert(source_path)
        print(f"Restored: {result['restored_path']}")
        print(f"Backup removed: {result['backup_path']}")
        return 0

    summary = run_init(
        source_path,
        data_dir=data_dir,
        gateway_name=args.gateway_name,
        gateway_url=args.gateway_url,
        dry_run=args.dry_run,
        db_backend=args.db_backend,
        postgres_dsn=args.postgres_dsn,
    )
    print_init_summary(summary, dry_run=args.dry_run)
    return 0


def _print_check_report(
    config: Any,
    report: Any,
) -> int:
    """Print ``--check`` diagnostic output and return exit code.

    Args:
        config: Gateway configuration for budget display.
        report: Startup check result with health flags.

    Returns:
        ``0`` when all checks pass, ``1`` otherwise.
    """
    from sift_mcp.constants import (
        CANONICALIZER_VERSION,
        CURSOR_VERSION,
        MAPPER_VERSION,
        PRNG_VERSION,
        TRAVERSAL_CONTRACT_VERSION,
    )

    print(f"fs_ok={report.fs_ok}")
    print(f"db_ok={report.db_ok}")
    print(f"upstream_ok={report.upstream_ok}")
    print(
        f"versions: canonicalizer={CANONICALIZER_VERSION}, "
        f"mapper={MAPPER_VERSION}, "
        f"traversal={TRAVERSAL_CONTRACT_VERSION}, "
        f"cursor={CURSOR_VERSION}, "
        f"prng={PRNG_VERSION}"
    )
    print(
        f"budgets: max_items={config.max_items}, "
        f"max_bytes_out={config.max_bytes_out}, "
        f"max_total_storage_bytes="
        f"{config.max_total_storage_bytes}"
    )
    if report.details:
        for item in report.details:
            print(f"- {item}")
    return 0 if report.ok else 1


def _run_server(
    config: Any,
    report: Any,
    args: argparse.Namespace,
) -> int:
    """Build the MCP server and run until shutdown.

    Args:
        config: Gateway configuration.
        report: Startup check result confirming readiness.
        args: Parsed CLI arguments with transport options.

    Returns:
        Exit code (``0`` on clean shutdown).
    """
    from sift_mcp.app import build_app

    transport = args.transport

    if transport == "stdio" and sys.stdin.isatty():
        print(
            "sift-mcp is an MCP server and expects "
            "JSON-RPC input on stdin.\n"
            "It should be launched by an MCP client "
            "(e.g. Claude Desktop), not run directly.\n"
            "\n"
            "Useful commands:\n"
            "  sift-mcp --check          "
            "Validate config and exit\n"
            "  sift-mcp --transport sse   "
            "Run with HTTP transport\n"
            "  sift-mcp init --from FILE  "
            "Import MCP config",
            file=sys.stderr,
        )
        return 1

    auth_token = None
    if transport in ("sse", "streamable-http"):
        from sift_mcp.mcp.http_auth import (
            bearer_auth_middleware,
            validate_http_bind,
        )

        auth_token = args.auth_token or os.environ.get("SIFT_MCP_AUTH_TOKEN")
        validate_http_bind(args.host, auth_token)

    server, pool = build_app(
        config=config,
        startup_report=report,
    )
    try:
        app = server.build_fastmcp_app()
        if transport == "stdio":
            from sift_mcp.mcp.stdio_compat import (
                run_fastmcp_stdio_compat,
            )

            run_fastmcp_stdio_compat(app, show_banner=False)
        else:
            # Wrap with bearer auth middleware when token set
            if auth_token:
                asgi: Any = app.http_app(
                    transport=transport,
                    path=args.path,
                )
                asgi = bearer_auth_middleware(asgi, auth_token)
                import uvicorn

                uvicorn.run(
                    asgi,
                    host=args.host,
                    port=args.port,
                )
            else:
                app.run(
                    transport=transport,
                    host=args.host,
                    port=args.port,
                    path=args.path,
                )
    finally:
        try:
            asyncio.run(server.drain_mapping_tasks(timeout=5.0))
        except Exception:
            print(
                "drain_mapping_tasks failed during shutdown",
                file=sys.stderr,
            )
        pool.close()
    return 0


def serve() -> int:
    """Dispatch CLI command and return an exit code.

    Handles ``init``, ``--check``, and the default server mode.

    Returns:
        ``0`` on success, ``1`` on failure.
    """
    args = _parse_args()

    if args.command == "init":
        return _run_init(args)

    if args.command == "upstream":
        if getattr(args, "upstream_command", None) == "add":
            return _run_upstream_add(args)
        # No subcommand given — print help
        print(
            "usage: sift-mcp upstream {add} ...",
            file=sys.stderr,
        )
        return 1

    # Auto-sync newly added MCPs from source config
    if not args.check:
        from sift_mcp.config.sync import run_sync
        from sift_mcp.constants import DEFAULT_DATA_DIR

        sync_data_dir = (
            args.data_dir
            or os.environ.get("SIFT_MCP_DATA_DIR")
            or DEFAULT_DATA_DIR
        )
        sync_result = run_sync(sync_data_dir)
        if sync_result.get("synced", 0) > 0:
            print(
                f"Auto-synced {sync_result['synced']} new upstream(s)",
                file=sys.stderr,
            )

    config = load_gateway_config(
        data_dir_override=args.data_dir,
    )
    report = run_startup_check(config)

    if args.check:
        return _print_check_report(config, report)

    if not report.ok:
        for item in report.details:
            print(f"- {item}", file=sys.stderr)
        return 1

    return _run_server(config, report, args)


def cli() -> None:
    """Run the gateway CLI and exit with the appropriate code."""
    from sift_mcp.obs.logging import configure_logging

    configure_logging()

    try:
        exit_code = serve()
    except Exception as exc:
        print(f"sift-mcp failed: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(exit_code)
