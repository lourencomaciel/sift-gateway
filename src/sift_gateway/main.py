"""Provide the CLI entrypoint for the Sift.

Parse command-line arguments, dispatch to subcommands (``init``,
``--check``), and launch the MCP server.  Exports ``serve`` and
``cli`` as the primary entry points.

Typical usage example::

    # From the command line:
    sift-gateway --check
    sift-gateway init --from claude
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

from sift_gateway import __version__
from sift_gateway.config import load_gateway_config
from sift_gateway.config.shared import is_sift_command
from sift_gateway.constants import DEFAULT_GATEWAY_NAME
from sift_gateway.lifecycle import run_startup_check

_ARTIFACT_COMMANDS = {
    "code",
    "run",
}
_SHARED_FLAGS_WITH_VALUE = {
    "--data-dir",
}
_SHARED_FLAGS = {
    "--version",
}
_SERVER_FLAGS_WITH_VALUE = {
    "--transport",
    "--host",
    "--port",
    "--path",
    "--auth-token",
}
_SERVER_FLAGS = {
    "--check",
}
_GLOBAL_FLAGS_WITH_VALUE = _SHARED_FLAGS_WITH_VALUE | _SERVER_FLAGS_WITH_VALUE
_GLOBAL_FLAGS = _SHARED_FLAGS | _SERVER_FLAGS
_LOGS_FLAG = "--logs"


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
            "Override DATA_DIR (default: .sift-gateway in current directory)"
        ),
    )
    init_parser.add_argument(
        "--gateway-name",
        default=DEFAULT_GATEWAY_NAME,
        help="Name for the gateway entry in the rewritten source file",
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
        "--from",
        dest="source",
        default=None,
        help=(
            "Target by source config path or shortcut "
            "(claude, claude-code, cursor, vscode, windsurf, zed, auto)"
        ),
    )
    add_parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override DATA_DIR directly",
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


def _add_install_subcommand(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register ``install`` and ``uninstall`` subcommands."""
    install_parser = sub.add_parser(
        "install",
        help="Install Python packages for code queries",
    )
    install_parser.add_argument(
        "packages",
        nargs="+",
        help="Package names to install (e.g. pandas scipy)",
    )
    # Use SUPPRESS so a subcommand-level --data-dir doesn't
    # overwrite the global one with None when omitted.
    install_parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override instance DATA_DIR for allowlist update",
    )

    uninstall_parser = sub.add_parser(
        "uninstall",
        help="Uninstall Python packages from code queries",
    )
    uninstall_parser.add_argument(
        "packages",
        nargs="+",
        help="Package names to uninstall",
    )
    uninstall_parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override instance DATA_DIR for allowlist update",
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the gateway CLI.

    Returns:
        Parsed argument namespace with command, flags, and
        subcommand-specific options.
    """
    parser = argparse.ArgumentParser(
        prog="sift-gateway",
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
        "--logs",
        action="store_true",
        help="Emit structured logs to stderr",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .sift-gateway/)",
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
            "Also reads SIFT_GATEWAY_AUTH_TOKEN env var"
        ),
    )
    _add_init_subcommand(sub)
    _add_upstream_subcommand(sub)
    _add_install_subcommand(sub)
    return parser.parse_args(argv)


def _first_positional_command(raw_argv: list[str]) -> str | None:
    """Return the first positional token from raw CLI args."""
    idx = 0
    while idx < len(raw_argv):
        token = raw_argv[idx]
        if token in _GLOBAL_FLAGS_WITH_VALUE:
            idx += 2
            continue
        if token in _GLOBAL_FLAGS:
            idx += 1
            continue
        if token.startswith("-"):
            return None
        return token
    return None


def _has_server_only_flags(raw_argv: list[str]) -> bool:
    """Return whether argv includes flags reserved for server mode."""
    for token in raw_argv:
        if token in _SERVER_FLAGS or token in _SERVER_FLAGS_WITH_VALUE:
            return True
    return False


def _is_artifact_cli_invocation(raw_argv: list[str]) -> bool:
    """Return whether argv targets artifact CLI mode."""
    if _has_server_only_flags(raw_argv):
        return False
    command = _first_positional_command(raw_argv)
    return isinstance(command, str) and command in _ARTIFACT_COMMANDS


def _extract_logs_flag(raw_argv: list[str]) -> tuple[bool, list[str]]:
    """Extract top-level ``--logs`` flag and return sanitized argv."""
    logs_enabled = False
    sanitized: list[str] = []
    idx = 0
    while idx < len(raw_argv):
        token = raw_argv[idx]
        if token == "--":
            sanitized.extend(raw_argv[idx:])
            break
        if token in _GLOBAL_FLAGS_WITH_VALUE:
            sanitized.append(token)
            idx += 1
            if idx < len(raw_argv):
                sanitized.append(raw_argv[idx])
            idx += 1
            continue
        if token == _LOGS_FLAG:
            logs_enabled = True
            idx += 1
            continue
        if token in _GLOBAL_FLAGS:
            sanitized.append(token)
            idx += 1
            continue
        if token.startswith("-"):
            sanitized.append(token)
            idx += 1
            continue
        sanitized.extend(raw_argv[idx:])
        break
    return logs_enabled, sanitized


def _run_upstream_add(args: argparse.Namespace) -> int:
    """Handle the ``upstream add`` subcommand.

    Args:
        args: Parsed CLI arguments containing the JSON snippet
            and optional overrides.

    Returns:
        Exit code (``0`` on success).
    """
    import json as json_mod

    from sift_gateway.config.upstream_add import (
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
    raw_data_dir = getattr(args, "data_dir", None)

    if source_arg:
        from sift_gateway.config.init_source import resolve_source_arg

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
                effective_data_dir = _resolve_effective_data_dir_arg(None)
                data_dir = Path(effective_data_dir).expanduser().resolve()
    else:
        effective_data_dir = _resolve_effective_data_dir_arg(raw_data_dir)
        data_dir = Path(effective_data_dir).expanduser().resolve()

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
    from sift_gateway.config.init import (
        print_init_summary,
        run_init,
        run_revert,
    )
    from sift_gateway.config.init_source import resolve_source_arg

    source_path = resolve_source_arg(args.source)
    data_dir = (
        Path(args.data_dir).expanduser().resolve() if args.data_dir else None
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
    )
    print_init_summary(summary, dry_run=args.dry_run)
    return 0


def _run_install(args: argparse.Namespace) -> int:
    """Handle ``install`` and ``uninstall`` subcommands.

    Args:
        args: Parsed CLI arguments with package names and
            optional data-dir override.

    Returns:
        Exit code (``0`` on success).
    """
    from sift_gateway.config.package_install import (
        install_packages,
        uninstall_packages,
    )

    raw_data_dir = getattr(args, "data_dir", None)
    effective_data_dir = _resolve_effective_data_dir_arg(raw_data_dir)
    data_dir: Path = Path(effective_data_dir).expanduser().resolve()

    if args.command == "uninstall":
        return uninstall_packages(args.packages, data_dir=data_dir)
    return install_packages(args.packages, data_dir=data_dir)


def _resolve_data_dir_from_source_config(source_path: Path) -> Path | None:
    """Extract gateway ``--data-dir`` from a migrated source config."""
    from sift_gateway.config.mcp_servers import (
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
        if not isinstance(command, str) or not is_sift_command(command):
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
        return Path(args[idx + 1]).expanduser().resolve()

    return None


def _resolve_effective_data_dir_arg(
    explicit_data_dir: str | None,
) -> str:
    """Resolve the data dir used by sync and runtime config loading."""
    from sift_gateway.constants import DEFAULT_DATA_DIR

    if explicit_data_dir:
        return str(Path(explicit_data_dir).expanduser().resolve())

    env_data_dir = os.environ.get("SIFT_GATEWAY_DATA_DIR")
    if env_data_dir:
        return str(Path(env_data_dir).expanduser().resolve())

    return str(Path(DEFAULT_DATA_DIR).expanduser().resolve())


def _resolve_data_dir_from_sync_metadata(
    data_dir: str | Path,
) -> str:
    """Follow ``_gateway_sync.data_dir`` redirects to a final data dir."""
    from sift_gateway.constants import CONFIG_FILENAME, STATE_SUBDIR

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
    from sift_gateway.constants import (
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
    from sift_gateway.app import build_app

    transport = args.transport

    if transport == "stdio" and sys.stdin.isatty():
        print(
            "sift-gateway is an MCP server and expects "
            "JSON-RPC input on stdin.\n"
            "It should be launched by an MCP client "
            "(e.g. Claude Desktop), not run directly.\n"
            "\n"
            "Useful commands:\n"
            "  sift-gateway --check          "
            "Validate config and exit\n"
            "  sift-gateway --transport sse   "
            "Run with HTTP transport\n"
            "  sift-gateway init --from claude  "
            "Import MCP config\n"
            "  sift-gateway run -- <command>  "
            "Capture CLI artifacts (then use code or continue-from)",
            file=sys.stderr,
        )
        return 1

    auth_token = None
    if transport in ("sse", "streamable-http"):
        from sift_gateway.mcp.http_auth import (
            bearer_auth_middleware,
            validate_http_bind,
        )

        auth_token = args.auth_token or os.environ.get(
            "SIFT_GATEWAY_AUTH_TOKEN"
        )
        validate_http_bind(args.host, auth_token)

    server, pool = build_app(
        config=config,
        startup_report=report,
    )
    try:
        app = server.build_fastmcp_app()
        if transport == "stdio":
            from sift_gateway.mcp.stdio_compat import (
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
        pool.close()
    return 0


def serve(argv: list[str] | None = None) -> int:
    """Dispatch CLI command and return an exit code.

    Handles ``init``, ``--check``, and the default server mode.

    Returns:
        ``0`` on success, ``1`` on failure.
    """
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    _, dispatch_argv = _extract_logs_flag(raw_argv)
    if _is_artifact_cli_invocation(dispatch_argv):
        from sift_gateway import cli_main as artifact_cli

        return artifact_cli.serve(dispatch_argv)

    if argv is None and dispatch_argv == raw_argv:
        args = _parse_args()
    else:
        args = _parse_args(dispatch_argv)

    if args.command == "init":
        return _run_init(args)

    if args.command == "upstream":
        if getattr(args, "upstream_command", None) == "add":
            return _run_upstream_add(args)
        # No subcommand given — print help
        print(
            "usage: sift-gateway upstream {add} ...",
            file=sys.stderr,
        )
        return 1

    if args.command in ("install", "uninstall"):
        return _run_install(args)

    effective_data_dir = _resolve_effective_data_dir_arg(args.data_dir)
    sync_data_dir = effective_data_dir
    runtime_data_dir = _resolve_data_dir_from_sync_metadata(sync_data_dir)

    # Auto-sync newly added MCPs from source config
    if not args.check:
        from sift_gateway.config.sync import run_sync

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
    from sift_gateway.obs.logging import configure_logging

    raw_argv = sys.argv[1:]
    logs_enabled, dispatch_argv = _extract_logs_flag(raw_argv)
    configure_logging(enabled=logs_enabled)

    try:
        exit_code = serve(dispatch_argv)
    except Exception as exc:
        print(f"sift-gateway failed: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(exit_code)
