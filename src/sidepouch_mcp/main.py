"""Provide the CLI entrypoint for the SidePouch.

Parse command-line arguments, dispatch to subcommands (``init``,
``--check``), and launch the MCP server.  Exports ``serve`` and
``cli`` as the primary entry points.

Typical usage example::

    # From the command line:
    sidepouch-mcp --check
    sidepouch-mcp init --from claude_desktop_config.json
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys
from typing import Any

from sidepouch_mcp.config import load_gateway_config
from sidepouch_mcp.lifecycle import run_startup_check


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
        help="Override DATA_DIR (default: .sidepouch-mcp/)",
    )
    init_parser.add_argument(
        "--gateway-name",
        default="artifact-gateway",
        help="Name for the gateway entry in the rewritten source file",
    )
    init_parser.add_argument(
        "--postgres-dsn",
        default=None,
        help="Postgres connection string (skips Docker auto-provisioning)",
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
        help="Path to source config file (e.g., claude_desktop_config.json)",
    )
    _add_init_mode_group(init_parser)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the gateway CLI.

    Returns:
        Parsed argument namespace with command, flags, and
        subcommand-specific options.
    """
    parser = argparse.ArgumentParser(
        prog="sidepouch-mcp",
        description=("SidePouch — local single-tenant MCP proxy"),
    )
    sub = parser.add_subparsers(dest="command")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate config, DB, FS, upstreams and exit",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .sidepouch-mcp/)",
    )
    _add_init_subcommand(sub)
    return parser.parse_args()


def _run_init(args: argparse.Namespace) -> int:
    """Handle the ``init`` subcommand.

    Args:
        args: Parsed CLI arguments containing source path,
            mode flags, and optional overrides.

    Returns:
        Exit code (``0`` on success).
    """
    from sidepouch_mcp.config.init import (
        print_init_summary,
        run_init,
        run_revert,
    )

    source_path = Path(args.source)
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
        dry_run=args.dry_run,
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
    from sidepouch_mcp.constants import (
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


def _run_server(config: Any, report: Any) -> int:
    """Build the MCP server and run until shutdown.

    Args:
        config: Gateway configuration.
        report: Startup check result confirming readiness.

    Returns:
        Exit code (``0`` on clean shutdown).
    """
    from sidepouch_mcp.app import build_app

    server, pool = build_app(
        config=config,
        startup_report=report,
    )
    try:
        app = server.build_fastmcp_app()
        app.run(show_banner=False)
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

    return _run_server(config, report)


def cli() -> None:
    """Run the gateway CLI and exit with the appropriate code."""
    try:
        exit_code = serve()
    except Exception as exc:
        print(f"sidepouch-mcp failed: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(exit_code)
