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
_UPSTREAM_ADD_FLAG_MODE_ATTRS = (
    ("transport", "--transport"),
    ("stdio_command", "--command"),
    ("url", "--url"),
    ("command_args", "--arg"),
    ("env_pairs", "--env"),
    ("header_pairs", "--header"),
    ("external_user_id", "--external-user-id"),
    ("inherit_parent_env", "--inherit-parent-env"),
)


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


def _add_upstream_common_flags(
    parser: argparse.ArgumentParser,
    *,
    json_flag: bool = True,
    dry_run_flag: bool = False,
) -> None:
    """Add common upstream subcommand flags.

    Args:
        parser: The subcommand parser to add flags to.
        json_flag: Whether to add ``--json`` output flag.
        dry_run_flag: Whether to add ``--dry-run`` flag.
    """
    if dry_run_flag:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would happen without changes",
        )
    if json_flag:
        parser.add_argument(
            "--json",
            action="store_true",
            help="Print machine-readable JSON output",
        )
    parser.add_argument(
        "--data-dir",
        default=argparse.SUPPRESS,
        help="Override DATA_DIR directly",
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
        help="Add an upstream from flags or JSON mcpServers snippet",
    )
    add_parser.add_argument(
        "snippet",
        nargs="?",
        default=None,
        help=(
            "JSON mcpServers snippet (legacy mode), e.g. "
            '\'{"name": {"command": "npx", "args": [...]}}\''
        ),
    )
    add_parser.add_argument(
        "--name",
        default=None,
        help="Upstream prefix for flag-based add mode",
    )
    add_parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=None,
        help="Transport for flag-based add mode",
    )
    add_parser.add_argument(
        "--command",
        dest="stdio_command",
        default=None,
        help="Command for stdio upstream",
    )
    add_parser.add_argument(
        "--url",
        default=None,
        help="URL for http upstream",
    )
    add_parser.add_argument(
        "--arg",
        dest="command_args",
        action="append",
        default=None,
        help="Repeatable stdio argument",
    )
    add_parser.add_argument(
        "--env",
        dest="env_pairs",
        action="append",
        default=None,
        help="Repeatable KEY=VALUE env entry (stdio only)",
    )
    add_parser.add_argument(
        "--header",
        dest="header_pairs",
        action="append",
        default=None,
        help="Repeatable KEY=VALUE header entry (http only)",
    )
    add_parser.add_argument(
        "--external-user-id",
        default=None,
        help="Set _gateway.external_user_id for the upstream",
    )
    add_parser.add_argument(
        "--inherit-parent-env",
        action="store_true",
        help="Set _gateway.inherit_parent_env for stdio upstreams",
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
    _add_upstream_common_flags(add_parser, json_flag=False, dry_run_flag=True)

    list_parser = upstream_sub.add_parser(
        "list",
        help="List configured upstream MCP servers",
    )
    _add_upstream_common_flags(list_parser)

    inspect_parser = upstream_sub.add_parser(
        "inspect",
        help="Inspect one configured upstream",
    )
    inspect_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to inspect",
    )
    _add_upstream_common_flags(inspect_parser)

    test_parser = upstream_sub.add_parser(
        "test",
        help="Probe upstream connectivity and tools/list",
    )
    test_scope = test_parser.add_mutually_exclusive_group(required=True)
    test_scope.add_argument(
        "--server",
        default=None,
        help="Upstream prefix to test",
    )
    test_scope.add_argument(
        "--all",
        action="store_true",
        help="Test all enabled upstreams",
    )
    _add_upstream_common_flags(test_parser)

    remove_parser = upstream_sub.add_parser(
        "remove",
        help="Remove one upstream",
    )
    remove_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to remove",
    )
    _add_upstream_common_flags(remove_parser, dry_run_flag=True)

    enable_parser = upstream_sub.add_parser(
        "enable",
        help="Enable one upstream",
    )
    enable_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to enable",
    )
    _add_upstream_common_flags(enable_parser, dry_run_flag=True)

    disable_parser = upstream_sub.add_parser(
        "disable",
        help="Disable one upstream",
    )
    disable_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to disable",
    )
    _add_upstream_common_flags(disable_parser, dry_run_flag=True)

    login_parser = upstream_sub.add_parser(
        "login",
        help="Run OAuth login for one HTTP upstream and persist auth header",
    )
    login_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to login",
    )
    login_parser.add_argument(
        "--headless",
        action="store_true",
        help="Run OAuth login without opening a browser (CI/testing)",
    )
    login_parser.add_argument(
        "--oauth-client-id",
        default=None,
        help="Use a pre-registered OAuth client ID",
    )
    login_parser.add_argument(
        "--oauth-client-secret",
        default=None,
        help="Use a pre-registered OAuth client secret",
    )
    login_parser.add_argument(
        "--oauth-auth-method",
        choices=[
            "none",
            "client_secret_post",
            "client_secret_basic",
        ],
        default=None,
        help="Token endpoint auth method for the pre-registered client",
    )
    login_parser.add_argument(
        "--oauth-registration",
        choices=["dynamic", "preregistered"],
        default=None,
        help="OAuth client registration strategy override",
    )
    login_parser.add_argument(
        "--oauth-scope",
        dest="oauth_scopes",
        action="append",
        default=None,
        help="Repeatable OAuth scope override",
    )
    login_parser.add_argument(
        "--oauth-callback-port",
        type=int,
        default=None,
        help="Fixed localhost callback port for OAuth login",
    )
    _add_upstream_common_flags(login_parser, dry_run_flag=True)

    auth_parser = upstream_sub.add_parser(
        "auth",
        help="Manage upstream auth material",
    )
    auth_sub = auth_parser.add_subparsers(dest="auth_command")

    auth_set_parser = auth_sub.add_parser(
        "set",
        help="Set auth material and externalize to secret storage",
    )
    auth_set_parser.add_argument(
        "--server",
        required=True,
        help="Upstream prefix to update",
    )
    auth_set_parser.add_argument(
        "--env",
        dest="env_pairs",
        action="append",
        default=None,
        help="Repeatable KEY=VALUE env entry (stdio only)",
    )
    auth_set_parser.add_argument(
        "--header",
        dest="header_pairs",
        action="append",
        default=None,
        help="Repeatable KEY=VALUE header entry (http only)",
    )
    auth_set_parser.add_argument(
        "--auth-mode",
        "--oauth-provider",
        dest="auth_mode",
        choices=["google-adc"],
        default=None,
        help="Enable a runtime auth mode for an HTTP upstream",
    )
    auth_set_parser.add_argument(
        "--scope",
        "--oauth-scope",
        dest="auth_scopes",
        action="append",
        default=None,
        help="Repeatable scope for auth-mode based runtime auth",
    )
    auth_set_parser.add_argument(
        "--preserve-auth-mode",
        action="store_true",
        help=(
            "Keep the existing runtime auth mode enabled while updating "
            "auxiliary HTTP headers"
        ),
    )
    _add_upstream_common_flags(auth_set_parser, dry_run_flag=True)

    auth_check_parser = auth_sub.add_parser(
        "check",
        help="Probe OAuth upstream sessions with forced refresh preflight",
    )
    auth_check_scope = auth_check_parser.add_mutually_exclusive_group(
        required=True
    )
    auth_check_scope.add_argument(
        "--server",
        default=None,
        help="OAuth-enabled upstream prefix to check",
    )
    auth_check_scope.add_argument(
        "--all",
        action="store_true",
        help="Check all OAuth-enabled upstreams",
    )
    _add_upstream_common_flags(auth_check_parser)


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

    raw_argv = list(sys.argv[1:] if argv is None else argv)
    normalized_argv = _normalize_upstream_add_argv(raw_argv)
    return parser.parse_args(normalized_argv)


_DASH_VALUE_FLAGS = frozenset({"--arg", "--env", "--header"})


def _normalize_upstream_add_argv(raw_argv: list[str]) -> list[str]:
    """Normalize upstream add argv to handle dash-prefixed values.

    ``argparse`` treats tokens that start with ``-`` as potential flags, so
    values such as ``-y`` or ``-DTOKEN=value`` may fail when provided as
    ``--arg <value>``, ``--env <value>``, or ``--header <value>``.  Rewrite
    dash-prefixed values to ``--flag=<value>`` specifically for the
    ``upstream add`` command.
    """
    if "upstream" not in raw_argv:
        return raw_argv
    try:
        upstream_index = raw_argv.index("upstream")
    except ValueError:
        return raw_argv
    if upstream_index + 1 >= len(raw_argv):
        return raw_argv
    if raw_argv[upstream_index + 1] != "add":
        return raw_argv

    normalized: list[str] = []
    idx = 0
    while idx < len(raw_argv):
        token = raw_argv[idx]
        if token == "--":
            normalized.extend(raw_argv[idx:])
            break
        if token in _DASH_VALUE_FLAGS and idx + 1 < len(raw_argv):
            value = raw_argv[idx + 1]
            if value.startswith("-"):
                normalized.append(f"{token}={value}")
                idx += 2
                continue
        normalized.append(token)
        idx += 1
    return normalized


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


def _build_upstream_add_input(
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Build raw mcpServers input from CLI args.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Dict mapping server names to config entries.
    """
    from sift_gateway.config.upstream_admin import parse_kv_pairs

    snippet = getattr(args, "snippet", None)
    name = getattr(args, "name", None)

    if snippet is not None and name is not None:
        msg = "provide either legacy snippet JSON or --name, not both"
        raise ValueError(msg)
    if snippet is None and name is None:
        msg = "upstream add requires either snippet JSON or --name"
        raise ValueError(msg)

    if name is not None:
        transport = getattr(args, "transport", None)
        if transport is None:
            msg = "--transport is required when using --name"
            raise ValueError(msg)

        entry: dict[str, Any] = {}
        if transport == "stdio":
            if not getattr(args, "stdio_command", None):
                msg = "--command is required for stdio transport"
                raise ValueError(msg)
            if getattr(args, "url", None) is not None:
                msg = "--url is only valid for http transport"
                raise ValueError(msg)
            entry["command"] = args.stdio_command
            if getattr(args, "command_args", None):
                entry["args"] = list(args.command_args)
            env_pairs = parse_kv_pairs(args.env_pairs, option_name="--env")
            if env_pairs:
                entry["env"] = env_pairs
            if getattr(args, "header_pairs", None):
                msg = "--header is only valid for http transport"
                raise ValueError(msg)
        else:
            if not getattr(args, "url", None):
                msg = "--url is required for http transport"
                raise ValueError(msg)
            if getattr(args, "stdio_command", None) is not None:
                msg = "--command is only valid for stdio transport"
                raise ValueError(msg)
            if getattr(args, "command_args", None):
                msg = "--arg is only valid for stdio transport"
                raise ValueError(msg)
            if getattr(args, "env_pairs", None):
                msg = "--env is only valid for stdio transport"
                raise ValueError(msg)
            entry["url"] = args.url
            header_pairs = parse_kv_pairs(
                args.header_pairs, option_name="--header"
            )
            if header_pairs:
                entry["headers"] = header_pairs

        if transport == "http" and getattr(args, "inherit_parent_env", False):
            msg = "--inherit-parent-env is only valid for stdio transport"
            raise ValueError(msg)

        gateway_ext: dict[str, Any] = {}
        if getattr(args, "inherit_parent_env", False):
            gateway_ext["inherit_parent_env"] = True
        if getattr(args, "external_user_id", None) is not None:
            gateway_ext["external_user_id"] = args.external_user_id
        if gateway_ext:
            entry["_gateway"] = gateway_ext

        return {name: entry}

    # Legacy snippet mode
    invalid_snippet_flags = [
        flag
        for attr, flag in _UPSTREAM_ADD_FLAG_MODE_ATTRS
        if (
            (
                isinstance(getattr(args, attr, None), bool)
                and getattr(args, attr, None)
            )
            or (
                not isinstance(getattr(args, attr, None), bool)
                and getattr(args, attr, None) is not None
            )
        )
    ]
    if invalid_snippet_flags:
        flags = ", ".join(sorted(invalid_snippet_flags))
        msg = (
            "legacy snippet mode cannot be combined with "
            f"flag-based options: {flags}"
        )
        raise ValueError(msg)
    try:
        raw = json.loads(str(snippet))
    except json.JSONDecodeError as exc:
        msg = f"invalid JSON snippet: {exc}"
        raise ValueError(msg) from exc

    if not isinstance(raw, dict):
        msg = "snippet must be a JSON object mapping server names to configs"
        raise ValueError(msg)
    return raw


def _run_upstream_add(args: argparse.Namespace) -> int:
    """Handle the ``upstream add`` subcommand.

    Args:
        args: Parsed CLI arguments for legacy snippet mode
            or flag-based mode.

    Returns:
        Exit code (``0`` on success).
    """
    from sift_gateway.config.upstream_add import (
        print_add_summary,
        run_upstream_add,
    )
    from sift_gateway.config.upstream_admin import (
        reconcile_after_add,
    )

    raw = _build_upstream_add_input(args)

    data_dir: Path
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

    runtime_data_dir = _resolve_data_dir_from_sync_metadata(data_dir)
    data_dir = Path(runtime_data_dir).expanduser().resolve()

    summary = run_upstream_add(
        raw,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        added_names = {
            str(name)
            for name in summary.get("added", [])
            if isinstance(name, str)
        }
        warnings: list[str] = []
        reconcile_after_add(
            data_dir=data_dir,
            raw_input=raw,
            added_names=added_names,
            warnings=warnings,
        )
        for warning in warnings:
            print(f"warning: {warning}", file=sys.stderr)

    print_add_summary(summary, dry_run=args.dry_run)
    return 0


def _resolve_upstream_data_dir_arg(raw_data_dir: str | None) -> Path:
    """Resolve upstream command data directory from CLI/env/default."""
    effective_data_dir = _resolve_effective_data_dir_arg(raw_data_dir)
    runtime_data_dir = _resolve_data_dir_from_sync_metadata(effective_data_dir)
    return Path(runtime_data_dir).expanduser().resolve()


def _print_json_output(payload: Any) -> None:
    """Print machine-readable JSON output."""
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _run_upstream_list(args: argparse.Namespace) -> int:
    """Handle the ``upstream list`` subcommand."""
    from sift_gateway.config.upstream_admin import list_upstreams

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    rows = list_upstreams(data_dir=data_dir)

    if getattr(args, "json", False):
        _print_json_output(rows)
        return 0

    if not rows:
        print("No upstreams configured.")
        return 0

    for row in rows:
        status = "enabled" if row.get("enabled") else "disabled"
        name = str(row.get("name", ""))
        transport = str(row.get("transport", "unknown"))
        target = row.get("command") if transport == "stdio" else row.get("url")
        print(f"{name}: {status} ({transport})")
        if isinstance(target, str) and target:
            print(f"  target: {target}")
        secret_ref = row.get("secret_ref")
        if isinstance(secret_ref, str) and secret_ref:
            print(f"  secret_ref: {secret_ref}")
    return 0


def _run_upstream_inspect(args: argparse.Namespace) -> int:
    """Handle the ``upstream inspect`` subcommand."""
    from sift_gateway.config.upstream_admin import inspect_upstream

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    item = inspect_upstream(
        server=args.server,
        data_dir=data_dir,
    )

    if getattr(args, "json", False):
        _print_json_output(item)
        return 0

    print(f"name: {item['name']}")
    print(f"enabled: {item['enabled']}")
    print(f"transport: {item['transport']}")
    if item.get("command"):
        print(f"command: {item['command']}")
    if item.get("url"):
        print(f"url: {item['url']}")
    if item.get("args"):
        print(f"args: {item['args']}")

    secret = item.get("secret")
    if isinstance(secret, dict):
        if "error" in secret:
            print(f"secret: error ({secret['error']})")
        else:
            print(f"secret_ref: {secret.get('ref')}")
            print(f"secret_transport: {secret.get('transport')}")
            print(f"secret_env_keys: {secret.get('env_keys', [])}")
            print(f"secret_header_keys: {secret.get('header_keys', [])}")
    return 0


def _run_upstream_test(args: argparse.Namespace) -> int:
    """Handle the ``upstream test`` subcommand."""
    from sift_gateway.config.upstream_admin import probe_upstreams

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    report = probe_upstreams(
        server=getattr(args, "server", None),
        all_servers=bool(getattr(args, "all", False)),
        data_dir=data_dir,
    )

    if getattr(args, "json", False):
        _print_json_output(report)
        return 0 if report.get("ok") else 1

    for row in report.get("results", []):
        name = row.get("name")
        if row.get("ok"):
            print(f"ok {name} tool_count={row.get('tool_count', 0)}")
            continue
        print(
            "fail "
            f"{name} "
            f"error_code={row.get('error_code', 'UNKNOWN')} "
            f"error={row.get('error', '')}"
        )
    print(f"summary: {report.get('ok_count', 0)}/{report.get('total', 0)} ok")
    return 0 if report.get("ok") else 1


def _run_upstream_remove(args: argparse.Namespace) -> int:
    """Handle the ``upstream remove`` subcommand."""
    from sift_gateway.config.upstream_admin import remove_upstream

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    result = remove_upstream(
        server=args.server,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )

    if getattr(args, "json", False):
        _print_json_output(result)
    elif args.dry_run:
        print(f"[dry run] would remove upstream: {args.server}")
    else:
        print(f"Removed upstream: {args.server}")
    return 0


def _run_upstream_set_enabled(
    args: argparse.Namespace,
    *,
    enabled: bool,
) -> int:
    """Handle the ``upstream enable`` and ``upstream disable`` commands."""
    from sift_gateway.config.upstream_admin import set_upstream_enabled

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    result = set_upstream_enabled(
        server=args.server,
        enabled=enabled,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )

    if getattr(args, "json", False):
        _print_json_output(result)
        return 0

    if args.dry_run:
        action = "enable" if enabled else "disable"
        print(f"[dry run] would {action} upstream: {args.server}")
    else:
        status = "enabled" if enabled else "disabled"
        print(f"Upstream {args.server} {status}.")
    return 0


def _run_upstream_auth_set(args: argparse.Namespace) -> int:
    """Handle the ``upstream auth set`` subcommand."""
    from sift_gateway.config.upstream_admin import (
        parse_kv_pairs,
        set_upstream_auth,
    )

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    env_updates = parse_kv_pairs(
        getattr(args, "env_pairs", None),
        option_name="--env",
    )
    header_updates = parse_kv_pairs(
        getattr(args, "header_pairs", None),
        option_name="--header",
    )
    auth_mode = getattr(
        args,
        "auth_mode",
        getattr(args, "oauth_provider", None),
    )
    auth_scopes = getattr(
        args,
        "auth_scopes",
        getattr(args, "oauth_scopes", None),
    )
    oauth = None
    clear_oauth = True
    preserve_auth_mode = bool(getattr(args, "preserve_auth_mode", False))
    updates_authorization_header = any(
        key.lower() == "authorization" for key in header_updates
    )
    scopes = [
        scope.strip()
        for scope in (auth_scopes or [])
        if isinstance(scope, str) and scope.strip()
    ]
    if scopes and not auth_mode:
        msg = "--scope requires --auth-mode"
        raise ValueError(msg)
    if isinstance(auth_mode, str) and auth_mode:
        if auth_mode in {"oauth", "fastmcp"}:
            msg = (
                "Use `sift-gateway upstream login --server ...` to enable "
                "interactive OAuth login."
            )
            raise ValueError(msg)
        oauth = {"enabled": True, "mode": auth_mode}
        if scopes:
            oauth["google_scopes"] = scopes
        clear_oauth = False
    elif preserve_auth_mode:
        if updates_authorization_header:
            msg = (
                "--preserve-auth-mode cannot be combined with an "
                "Authorization header override"
            )
            raise ValueError(msg)
        clear_oauth = False
    result = set_upstream_auth(
        server=args.server,
        env_updates=env_updates,
        header_updates=header_updates,
        oauth=oauth,
        merge_oauth=bool(isinstance(oauth, dict) and "google_scopes" in oauth),
        clear_oauth=clear_oauth,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )

    if getattr(args, "json", False):
        _print_json_output(result)
        return 0

    if args.dry_run:
        print(
            f"[dry run] would update auth material for upstream: {args.server}"
        )
    else:
        print(f"Updated auth material for upstream: {args.server}")
    return 0


def _run_upstream_login(args: argparse.Namespace) -> int:
    """Handle the ``upstream login`` subcommand."""
    from sift_gateway.config.upstream_admin import login_upstream

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    result = login_upstream(
        server=args.server,
        data_dir=data_dir,
        dry_run=args.dry_run,
        headless=bool(getattr(args, "headless", False)),
        oauth_client_id=getattr(args, "oauth_client_id", None),
        oauth_client_secret=getattr(args, "oauth_client_secret", None),
        oauth_auth_method=getattr(args, "oauth_auth_method", None),
        oauth_registration=getattr(args, "oauth_registration", None),
        oauth_scopes=getattr(args, "oauth_scopes", None),
        oauth_callback_port=getattr(args, "oauth_callback_port", None),
    )

    if getattr(args, "json", False):
        _print_json_output(result)
        return 0

    if args.dry_run:
        print(f"[dry run] would run OAuth login for upstream: {args.server}")
    else:
        print(f"OAuth login completed for upstream: {args.server}")
    return 0


def _run_upstream_auth_check(args: argparse.Namespace) -> int:
    """Handle the ``upstream auth check`` subcommand."""
    from sift_gateway.config.upstream_admin import probe_oauth_upstreams

    data_dir = _resolve_upstream_data_dir_arg(getattr(args, "data_dir", None))
    report = probe_oauth_upstreams(
        server=getattr(args, "server", None),
        all_servers=bool(getattr(args, "all", False)),
        data_dir=data_dir,
    )

    if getattr(args, "json", False):
        _print_json_output(report)
        return 0 if report.get("ok") else 1

    for row in report.get("results", []):
        name = row.get("name")
        if row.get("ok"):
            forced_refresh = bool(row.get("forced_refresh"))
            print(
                f"ok {name} "
                f"tool_count={row.get('tool_count', 0)} "
                f"forced_refresh={forced_refresh}"
            )
            continue
        print(
            "fail "
            f"{name} "
            f"error_code={row.get('error_code', 'UNKNOWN')} "
            f"error={row.get('error', '')}"
        )
    print(f"summary: {report.get('ok_count', 0)}/{report.get('total', 0)} ok")
    return 0 if report.get("ok") else 1


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
        upstream_command = getattr(args, "upstream_command", None)
        if upstream_command == "add":
            return _run_upstream_add(args)
        if upstream_command == "list":
            return _run_upstream_list(args)
        if upstream_command == "inspect":
            return _run_upstream_inspect(args)
        if upstream_command == "test":
            return _run_upstream_test(args)
        if upstream_command == "remove":
            return _run_upstream_remove(args)
        if upstream_command == "enable":
            return _run_upstream_set_enabled(args, enabled=True)
        if upstream_command == "disable":
            return _run_upstream_set_enabled(args, enabled=False)
        if upstream_command == "login":
            return _run_upstream_login(args)
        if upstream_command == "auth":
            if getattr(args, "auth_command", None) == "set":
                return _run_upstream_auth_set(args)
            if getattr(args, "auth_command", None) == "check":
                return _run_upstream_auth_check(args)
            print(
                "usage: sift-gateway upstream auth {set,check} ...",
                file=sys.stderr,
            )
            return 1

        print(
            (
                "usage: sift-gateway upstream "
                "{add,list,inspect,test,remove,enable,disable,login,auth} ..."
            ),
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
