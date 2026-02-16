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
import json
import os
from pathlib import Path
import sys
from typing import Any

from sift_mcp import __version__
from sift_mcp.config import load_gateway_config
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
        help=(
            "Override DATA_DIR (default: managed per-source "
            "instance under ~/.sift-mcp/instances/)"
        ),
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
    add_target = add_parser.add_mutually_exclusive_group()
    add_target.add_argument(
        "--from",
        dest="source",
        default=None,
        help=(
            "Target by source config path or shortcut "
            "(claude, claude-code, cursor, vscode, windsurf, zed, auto)"
        ),
    )
    add_target.add_argument(
        "--instance",
        dest="instance_id",
        default=None,
        help="Target a managed instance by id (see: sift-mcp instances list)",
    )
    add_parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override DATA_DIR directly (legacy/manual mode)",
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
            "Source config path or shortcut "
            "(claude, claude-code, cursor, vscode, windsurf, zed, auto)"
        ),
    )
    _add_init_mode_group(init_parser)


def _add_instances_subcommand(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the ``instances`` management subcommand tree."""
    instances_parser = sub.add_parser(
        "instances",
        help="Inspect managed Sift instances",
    )
    instances_sub = instances_parser.add_subparsers(
        dest="instances_command",
    )
    list_parser = instances_sub.add_parser(
        "list",
        help="List managed Sift instances",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output",
    )


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
    _add_instances_subcommand(sub)
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

    data_dir: Path | None
    data_dir = None
    source_arg = getattr(args, "source", None)
    instance_id = getattr(args, "instance_id", None)
    raw_data_dir = getattr(args, "data_dir", None)
    resolved_source_path: Path | None = None

    if instance_id and raw_data_dir is not None:
        msg = "--instance cannot be combined with --data-dir"
        raise ValueError(msg)

    if source_arg:
        from sift_mcp.config.init_source import resolve_source_arg
        from sift_mcp.config.instances import resolve_instance_data_dir

        resolved_source_path = resolve_source_arg(source_arg)
        if raw_data_dir is not None:
            data_dir = Path(raw_data_dir).expanduser().resolve()
        else:
            source_data_dir = _resolve_data_dir_from_source_config(
                resolved_source_path
            )
            if source_data_dir is not None:
                data_dir = source_data_dir
            else:
                data_dir = resolve_instance_data_dir(
                    resolved_source_path,
                    require_existing=True,
                )
    elif instance_id:
        from sift_mcp.config.instances import get_instance_data_dir

        data_dir = get_instance_data_dir(instance_id)
    else:
        effective_data_dir = _resolve_effective_data_dir_arg(raw_data_dir)
        if effective_data_dir is not None:
            data_dir = Path(effective_data_dir).expanduser().resolve()

    summary = run_upstream_add(
        raw,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )

    registry_warning: str | None = None
    if not args.dry_run:
        try:
            if resolved_source_path is not None and data_dir is not None:
                from sift_mcp.config.instances import upsert_instance

                upsert_instance(
                    source_path=resolved_source_path,
                    data_dir=data_dir,
                )
            elif instance_id:
                from sift_mcp.config.instances import touch_instance_by_id

                touch_instance_by_id(instance_id)
        except OSError as exc:
            registry_warning = (
                "upstream add completed but failed to update instance "
                f"registry: {exc}"
            )

    print_add_summary(summary, dry_run=args.dry_run)
    if registry_warning is not None:
        print(f"Warning: {registry_warning}", file=sys.stderr)
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
    from sift_mcp.config.init_source import resolve_source_arg

    source_path = resolve_source_arg(args.source)
    data_dir = (
        Path(args.data_dir).expanduser().resolve()
        if args.data_dir
        else None
    )

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


def _run_instances_list(args: argparse.Namespace) -> int:
    """Handle ``instances list``."""
    import json as json_mod

    from sift_mcp.config.instances import list_instances

    rows = list_instances()
    if args.json:
        print(
            json_mod.dumps(
                rows,
                indent=2,
                ensure_ascii=False,
            )
        )
        return 0

    if not rows:
        print("No managed instances found.")
        return 0

    for row in rows:
        instance_id = str(row.get("id", ""))
        client = str(row.get("client", ""))
        label = str(row.get("label", ""))
        source_path = str(row.get("source_path", ""))
        data_dir = str(row.get("data_dir", ""))
        print(f"{instance_id} [{client}] {label}")
        print(f"  source:   {source_path}")
        print(f"  data_dir: {data_dir}")
    return 0


def _resolve_managed_default_data_dir() -> Path | None:
    """Return the most recently used managed instance data dir."""
    from sift_mcp.config.instances import list_instances
    from sift_mcp.constants import CONFIG_FILENAME, STATE_SUBDIR

    for row in list_instances():
        raw_data_dir = row.get("data_dir")
        if not isinstance(raw_data_dir, str):
            continue
        candidate = Path(raw_data_dir).expanduser().resolve()
        if (candidate / STATE_SUBDIR / CONFIG_FILENAME).is_file():
            return candidate
    return None


def _is_sift_command(command: str) -> bool:
    """Return whether a command string invokes ``sift-mcp``."""
    command_name = Path(command).name.lower()
    return command_name in {"sift-mcp", "sift-mcp.exe"}


def _resolve_data_dir_from_source_config(source_path: Path) -> Path | None:
    """Extract gateway ``--data-dir`` from a migrated source config."""
    from sift_mcp.config.mcp_servers import (
        extract_mcp_servers,
        read_config_file,
    )

    try:
        source_raw = read_config_file(source_path)
        source_servers = extract_mcp_servers(source_raw)
    except (OSError, ValueError):
        return None

    for entry in source_servers.values():
        if not isinstance(entry, dict):
            continue
        command = entry.get("command")
        if not isinstance(command, str) or not _is_sift_command(command):
            continue
        raw_args = entry.get("args")
        if not isinstance(raw_args, list):
            continue
        args = [str(value) for value in raw_args]
        if "--data-dir" not in args:
            continue
        idx = args.index("--data-dir")
        if idx + 1 >= len(args):
            continue
        candidate = Path(args[idx + 1]).expanduser().resolve()
        return candidate

    return None


def _resolve_legacy_initialized_data_dir() -> Path | None:
    """Resolve an initialized legacy data dir when no source mapping exists."""
    from sift_mcp.constants import (
        CONFIG_FILENAME,
        DEFAULT_DATA_DIR,
        STATE_SUBDIR,
    )

    env_data_dir = os.environ.get("SIFT_MCP_DATA_DIR")
    candidates: list[Path] = []
    if env_data_dir:
        candidates.append(Path(env_data_dir).expanduser().resolve())
    candidates.append(Path(DEFAULT_DATA_DIR).expanduser().resolve())

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        config_path = candidate / STATE_SUBDIR / CONFIG_FILENAME
        if config_path.is_file():
            return candidate

    return None


def _resolve_effective_data_dir_arg(
    explicit_data_dir: str | None,
) -> str | None:
    """Resolve the data dir used by sync and runtime config loading."""
    if explicit_data_dir:
        return str(Path(explicit_data_dir).expanduser().resolve())

    env_data_dir = os.environ.get("SIFT_MCP_DATA_DIR")
    if env_data_dir:
        return str(Path(env_data_dir).expanduser().resolve())

    managed_default = _resolve_managed_default_data_dir()
    if managed_default is not None:
        return str(managed_default)
    return None


def _resolve_data_dir_from_sync_metadata(
    data_dir: str | Path,
) -> str:
    """Follow ``_gateway_sync.data_dir`` redirects to a final data dir."""
    from sift_mcp.constants import CONFIG_FILENAME, STATE_SUBDIR

    current = Path(data_dir).expanduser().resolve()
    seen: set[Path] = set()

    while current not in seen:
        seen.add(current)
        config_path = current / STATE_SUBDIR / CONFIG_FILENAME
        if not config_path.is_file():
            break
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            break
        if not isinstance(raw, dict):
            break
        sync_meta = raw.get("_gateway_sync")
        if not isinstance(sync_meta, dict):
            break
        redirected = sync_meta.get("data_dir")
        if not isinstance(redirected, str):
            break
        redirected_path = Path(redirected).expanduser().resolve()
        if redirected_path == current:
            break
        redirected_config_path = (
            redirected_path / STATE_SUBDIR / CONFIG_FILENAME
        )
        if not redirected_config_path.is_file():
            break
        current = redirected_path

    return str(current)


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
            "  sift-mcp init --from claude  "
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

    if args.command == "instances":
        if getattr(args, "instances_command", None) == "list":
            return _run_instances_list(args)
        print(
            "usage: sift-mcp instances {list} ...",
            file=sys.stderr,
        )
        return 1

    from sift_mcp.constants import DEFAULT_DATA_DIR

    effective_data_dir = _resolve_effective_data_dir_arg(args.data_dir)
    sync_data_dir = effective_data_dir or DEFAULT_DATA_DIR
    runtime_data_dir = _resolve_data_dir_from_sync_metadata(sync_data_dir)

    # Auto-sync newly added MCPs from source config
    if not args.check:
        from sift_mcp.config.sync import run_sync
        sync_result = run_sync(sync_data_dir)
        runtime_data_dir = _resolve_data_dir_from_sync_metadata(sync_data_dir)
        if sync_result.get("synced", 0) > 0:
            print(
                f"Auto-synced {sync_result['synced']} new upstream(s)",
                file=sys.stderr,
            )

    config = load_gateway_config(
        data_dir_override=runtime_data_dir,
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
