"""CLI entrypoint for protocol-agnostic artifact retrieval commands."""

from __future__ import annotations

import argparse
from collections.abc import Generator
from contextlib import contextmanager
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import textwrap
from typing import Any

from sift_mcp import __version__
from sift_mcp.config import load_gateway_config
from sift_mcp.constants import (
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_STDIN_PIPE,
    WORKSPACE_ID,
)
from sift_mcp.core.artifact_capture import (
    execute_artifact_capture,
)
from sift_mcp.core.artifact_code import execute_artifact_code
from sift_mcp.core.artifact_describe import execute_artifact_describe
from sift_mcp.core.artifact_get import execute_artifact_get
from sift_mcp.core.artifact_search import execute_artifact_search
from sift_mcp.core.artifact_select import execute_artifact_select
from sift_mcp.db.backend import SqliteBackend
from sift_mcp.db.migrate import apply_migrations
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)
from sift_mcp.mcp.server import GatewayServer
from sift_mcp.request_identity import compute_request_identity
from sift_mcp.storage.payload_store import reconstruct_envelope

_CLI_SESSION_ID = "cli"
_CLI_PREFIX = "cli"
_CLI_UPSTREAM_INSTANCE_ID = "cli_local"
_DEFAULT_TTL_RAW = "24h"
_TTL_PATTERN = re.compile(r"^([1-9][0-9]*)([smhd]?)$")
_DIFF_DEFAULT_MAX_LINES = 200
_CODE_EXPR_TEMPLATE = textwrap.dedent(
    """
    def run(data, schema, params):
        import pandas as pd
        rows = data if isinstance(data, list) else [data]
        df = pd.DataFrame(rows)
        return {expr}
    """
).strip()


def _migrations_dir() -> Path:
    """Return the SQLite migrations directory path."""
    return Path(__file__).resolve().parent / "db" / "migrations_sqlite"


def _split_select_paths(raw_values: list[str] | None) -> list[str]:
    """Split ``--select`` values supporting repeated and comma-separated flags."""
    values = raw_values or []
    out: list[str] = []
    for value in values:
        for segment in value.split(","):
            stripped = segment.strip()
            if stripped:
                out.append(stripped)
    return out


def _parse_where_json(raw_where: str | None) -> dict[str, Any] | None:
    """Parse ``--where`` JSON into a filter object."""
    if raw_where is None:
        return None
    return _parse_json_object(raw_where, flag="--where")


def _parse_json_object(raw_value: str, *, flag: str) -> dict[str, Any]:
    """Parse a JSON object flag payload."""
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        msg = f"invalid {flag} JSON: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(parsed, dict):
        msg = f"{flag} must decode to a JSON object"
        raise ValueError(msg)
    return dict(parsed)


def _parse_params_json(raw_params: str | None) -> dict[str, Any]:
    """Parse optional ``--params`` JSON object."""
    if raw_params is None:
        return {}
    return _parse_json_object(raw_params, flag="--params")


def _load_code_source(args: argparse.Namespace) -> str:
    """Load Python source from ``--code``, ``--file``, or ``--expr``."""
    if args.code_expr is not None:
        return _CODE_EXPR_TEMPLATE.format(expr=args.code_expr)
    if args.code_file is not None:
        code_path = Path(args.code_file)
        if not code_path.exists():
            msg = f"code file not found: {args.code_file}"
            raise ValueError(msg)
        try:
            return code_path.read_text(encoding="utf-8")
        except OSError as exc:
            msg = f"unable to read code file: {args.code_file}"
            raise ValueError(msg) from exc
    code_inline = args.code_inline
    if isinstance(code_inline, str) and code_inline.strip():
        return code_inline
    msg = "missing code source; provide --code, --file, or --expr"
    raise ValueError(msg)


def _parse_ttl_seconds(raw_ttl: str | None) -> int | None:
    """Parse CLI TTL values (e.g., ``30m``, ``24h``, ``7d``)."""
    candidate = (
        raw_ttl.strip().lower()
        if isinstance(raw_ttl, str) and raw_ttl.strip()
        else os.environ.get("SIFT_TTL", _DEFAULT_TTL_RAW).strip().lower()
    )
    if candidate in {"none", "off", "0"}:
        return None
    match = _TTL_PATTERN.fullmatch(candidate)
    if match is None:
        msg = f"invalid --ttl value: {candidate}"
        raise ValueError(msg)
    value = int(match.group(1))
    suffix = match.group(2) or "s"
    multiplier = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }[suffix]
    return value * multiplier


def _parse_json_or_text_payload(text: str) -> tuple[Any, bool]:
    """Return parsed JSON when possible, otherwise the raw text."""
    if not text.strip():
        return "", False
    try:
        return json.loads(text), True
    except (json.JSONDecodeError, ValueError):
        return text, False


def _normalize_tags(raw_tags: list[str] | None) -> list[str]:
    """Normalize repeated/comma-delimited tag values."""
    tags = raw_tags or []
    out: list[str] = []
    seen: set[str] = set()
    for raw in tags:
        for segment in raw.split(","):
            tag = segment.strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
    return out


def _environment_fingerprint() -> str:
    """Return stable hash of visible environment keys."""
    keys = sorted(os.environ.keys())
    payload = "\n".join(keys).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalize_command_argv(raw_argv: list[str]) -> list[str]:
    """Normalize remainder argv for ``sift run -- <cmd>``."""
    argv = list(raw_argv)
    if argv and argv[0] == "--":
        return argv[1:]
    return argv


def _estimate_records(payload: Any) -> int | None:
    """Best-effort row estimate for captured payload summaries."""
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return len(items)
        stdout = payload.get("stdout")
        if isinstance(stdout, list):
            return len(stdout)
        return 1
    return None


def _build_diff_lines(
    *,
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
    left_label: str,
    right_label: str,
    max_lines: int,
) -> tuple[list[str], bool]:
    """Build bounded unified diff lines for two payload objects."""
    left_lines = json.dumps(
        left_payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    ).splitlines()
    right_lines = json.dumps(
        right_payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    ).splitlines()
    all_lines = list(
        difflib.unified_diff(
            left_lines,
            right_lines,
            fromfile=left_label,
            tofile=right_label,
            lineterm="",
        )
    )
    if len(all_lines) <= max_lines:
        return all_lines, False
    return all_lines[:max_lines], True


def _fetch_artifact_for_diff(
    runtime: GatewayArtifactQueryRuntime,
    *,
    artifact_id: str,
) -> dict[str, Any]:
    """Load one artifact envelope payload for diffing."""
    if runtime.db_pool is None:
        msg = "artifact diff requires database backend"
        raise ValueError(msg)
    with runtime.db_pool.connection() as connection:
        row = connection.execute(
            """
            SELECT a.artifact_id, a.payload_hash_full, a.payload_total_bytes,
                   pb.envelope, pb.envelope_canonical_encoding,
                   pb.payload_fs_path
            FROM artifacts a
            JOIN payload_blobs pb
              ON pb.workspace_id = a.workspace_id
             AND pb.payload_hash_full = a.payload_hash_full
            WHERE a.workspace_id = %s
              AND a.artifact_id = %s
              AND a.deleted_at IS NULL
            """,
            (WORKSPACE_ID, artifact_id),
        ).fetchone()
        if row is None:
            msg = f"artifact not found: {artifact_id}"
            raise ValueError(msg)
        loaded = {
            "artifact_id": row[0],
            "payload_hash_full": row[1],
            "payload_total_bytes": row[2],
            "envelope": row[3],
            "envelope_canonical_encoding": row[4],
            "payload_fs_path": row[5],
        }
        raw_envelope = loaded["envelope"]
        if isinstance(raw_envelope, dict):
            envelope = raw_envelope
        else:
            payload_fs_path = loaded["payload_fs_path"]
            if not isinstance(payload_fs_path, str) or not payload_fs_path:
                msg = f"payload path unavailable for artifact: {artifact_id}"
                raise ValueError(msg)
            envelope = reconstruct_envelope(
                payload_fs_path=payload_fs_path,
                blobs_payload_dir=runtime.config.blobs_payload_dir,
                encoding=str(
                    loaded.get("envelope_canonical_encoding", "none")
                ),
                expected_hash=str(loaded["payload_hash_full"]),
            )
        return {
            "artifact_id": str(loaded["artifact_id"]),
            "payload_hash_full": str(loaded["payload_hash_full"]),
            "payload_total_bytes": int(loaded["payload_total_bytes"]),
            "envelope": envelope,
        }


@contextmanager
def _runtime_context(
    *,
    data_dir_override: str | None,
) -> Generator[GatewayArtifactQueryRuntime, None, None]:
    """Build and yield a query runtime for CLI retrieval commands."""
    config = load_gateway_config(data_dir_override=data_dir_override)
    backend = SqliteBackend(
        db_path=config.sqlite_path,
        busy_timeout_ms=config.sqlite_busy_timeout_ms,
    )
    try:
        with backend.connection() as conn:
            apply_migrations(conn, _migrations_dir())
        server = GatewayServer(
            config=config,
            db_pool=backend,
            blob_store=None,
            upstreams=[],
            fs_ok=True,
            db_ok=True,
        )
        yield GatewayArtifactQueryRuntime(gateway=server)
    finally:
        backend.close()


def _build_gateway_context() -> dict[str, Any]:
    """Return the synthetic gateway context used for CLI retrieval calls."""
    return {"session_id": _CLI_SESSION_ID}


def _collect_list_filters(args: argparse.Namespace) -> dict[str, Any]:
    """Build search filters from parsed list-command arguments."""
    filters: dict[str, Any] = {}
    if args.include_deleted:
        filters["include_deleted"] = True
    if args.status is not None:
        filters["status"] = args.status
    if args.kind is not None:
        filters["kind"] = args.kind
    if args.source_tool is not None:
        filters["source_tool"] = args.source_tool
    if args.source_tool_prefix is not None:
        filters["source_tool_prefix"] = args.source_tool_prefix
    if args.upstream_instance_id is not None:
        filters["upstream_instance_id"] = args.upstream_instance_id
    if args.request_key is not None:
        filters["request_key"] = args.request_key
    if args.capture_kind is not None:
        filters["capture_kind"] = args.capture_kind
    if args.capture_key is not None:
        filters["capture_key"] = args.capture_key
    if args.parent_artifact_id is not None:
        filters["parent_artifact_id"] = args.parent_artifact_id
    return filters


def _is_error_response(payload: dict[str, Any]) -> bool:
    """Return whether a payload appears to be a gateway error response."""
    return (
        isinstance(payload.get("code"), str)
        and isinstance(payload.get("message"), str)
        and "items" not in payload
        and "count" not in payload
        and "artifact_id" not in payload
    )


def _write_line(text: str, *, stream: Any | None = None) -> None:
    """Write one line to the given stream."""
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


def _emit_json(payload: dict[str, Any]) -> None:
    """Emit payload as machine-readable JSON."""
    _write_line(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _emit_human_list(payload: dict[str, Any]) -> None:
    """Emit compact human-readable output for ``sift list``."""
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        _write_line("no artifacts")
    else:
        for item in items:
            if not isinstance(item, dict):
                continue
            _write_line(
                " ".join(
                    [
                        str(item.get("artifact_id", "")),
                        f"seq={item.get('created_seq')}",
                        f"kind={item.get('kind')}",
                        f"status={item.get('status')}",
                        f"source={item.get('source_tool')}",
                        f"capture={item.get('capture_kind')}",
                        f"bytes={item.get('payload_total_bytes')}",
                    ]
                )
            )
    cursor = payload.get("cursor")
    if isinstance(cursor, str) and cursor:
        _write_line(f"next_cursor: {cursor}")


def _emit_human_schema(payload: dict[str, Any]) -> None:
    """Emit compact human-readable output for ``sift schema``."""
    _write_line(f"artifact: {payload.get('artifact_id')}")
    _write_line(f"scope: {payload.get('scope')}")
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list):
        _write_line(f"artifacts: {len(artifacts)}")
    roots = payload.get("roots")
    if isinstance(roots, list):
        _write_line(f"roots: {len(roots)}")
        for root in roots:
            if not isinstance(root, dict):
                continue
            root_path = root.get("root_path")
            count = root.get("count_estimate")
            _write_line(f"- {root_path} count={count}")


def _emit_human_generic(payload: dict[str, Any]) -> None:
    """Emit default human-readable output for ``get`` and ``query``."""
    items = payload.get("items")
    if isinstance(items, list):
        _write_line(f"items: {len(items)}")
    if "count" in payload:
        _write_line(f"count: {payload.get('count')}")
    if payload.get("truncated") is True and isinstance(payload.get("cursor"), str):
        _write_line(f"next_cursor: {payload['cursor']}")
    _write_line(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _emit_human_run(payload: dict[str, Any]) -> None:
    """Emit compact run-capture summary."""
    artifact_id = payload.get("artifact_id")
    if isinstance(artifact_id, str):
        _write_line(f"artifact: {artifact_id}")
    records = payload.get("records")
    if isinstance(records, int):
        _write_line(f"records:  {records}")
    else:
        _write_line("records:  unknown")
    _write_line(f"bytes:    {payload.get('payload_total_bytes')}")
    capture_kind = payload.get("capture_kind")
    if isinstance(capture_kind, str):
        _write_line(f"capture:  {capture_kind}")
    expires_at = payload.get("expires_at")
    if isinstance(expires_at, str) and expires_at:
        _write_line(f"expires:  {expires_at}")
    tags = payload.get("tags")
    if isinstance(tags, list) and tags:
        _write_line(f"tags:     {', '.join(str(tag) for tag in tags)}")
    command_exit_code = payload.get("command_exit_code")
    if isinstance(command_exit_code, int):
        _write_line(f"exit:     {command_exit_code}")
    if isinstance(artifact_id, str):
        _write_line(f"hint:     use `sift query {artifact_id} '$'` to explore")


def _emit_human_diff(payload: dict[str, Any]) -> None:
    """Emit concise artifact comparison output."""
    _write_line(f"left:    {payload.get('left_artifact_id')}")
    _write_line(f"right:   {payload.get('right_artifact_id')}")
    _write_line(f"equal:   {payload.get('equal')}")
    _write_line(
        f"hashes:  {payload.get('left_payload_hash')} / "
        f"{payload.get('right_payload_hash')}"
    )
    _write_line(
        f"bytes:   {payload.get('left_payload_bytes')} / "
        f"{payload.get('right_payload_bytes')}"
    )
    if payload.get("equal") is True:
        return
    diff_lines = payload.get("diff_lines")
    if isinstance(diff_lines, list):
        for line in diff_lines:
            _write_line(str(line))
    if payload.get("diff_truncated") is True:
        _write_line("diff truncated; use --max-lines to increase output")


def _execute_list(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift list`` against core search service."""
    return execute_artifact_search(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "filters": _collect_list_filters(args),
            "order_by": args.order_by,
            "limit": args.limit,
            "cursor": args.cursor,
            "query": args.query,
        },
    )


def _execute_schema(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift schema`` against core describe service."""
    return execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "artifact_id": args.artifact_id,
            "scope": args.scope,
        },
    )


def _execute_get(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift get`` against core get service."""
    return execute_artifact_get(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "artifact_id": args.artifact_id,
            "scope": args.scope,
            "target": args.target,
            "jsonpath": args.jsonpath,
            "limit": args.limit,
            "cursor": args.cursor,
        },
    )


def _execute_query(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift query`` against core select service."""
    where_expr = _parse_where_json(args.where)
    payload: dict[str, Any] = {
        "_gateway_context": _build_gateway_context(),
        "artifact_id": args.artifact_id,
        "scope": args.scope,
        "root_path": args.root_path,
        "select_paths": _split_select_paths(args.select),
        "limit": args.limit,
        "cursor": args.cursor,
        "order_by": args.order_by,
        "distinct": args.distinct,
        "count_only": args.count_only,
    }
    if where_expr is not None:
        payload["where"] = where_expr
    return execute_artifact_select(runtime, arguments=payload)


def _execute_code(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift code`` against the core code-query service."""
    code_source = _load_code_source(args)
    params_obj = _parse_params_json(args.params)
    return execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "artifact_id": args.artifact_id,
            "root_path": args.root_path,
            "code": code_source,
            "params": params_obj,
        },
    )


def _execute_run(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift run`` by capturing command output or stdin."""
    tags = _normalize_tags(args.tag)
    ttl_seconds = _parse_ttl_seconds(args.ttl)
    cwd = str(Path.cwd())
    env_fingerprint = _environment_fingerprint()
    command_argv = _normalize_command_argv(args.command_argv)
    if args.stdin and command_argv:
        msg = "--stdin cannot be combined with a command"
        raise ValueError(msg)

    if args.stdin:
        raw_stdin = sys.stdin.buffer.read()
        stdin_text = raw_stdin.decode("utf-8", errors="replace")
        payload, _is_json = _parse_json_or_text_payload(stdin_text)
        if not _is_json:
            payload = {"stdin": stdin_text}
        stdin_hash = hashlib.sha256(raw_stdin).hexdigest()
        identity = compute_request_identity(
            upstream_instance_id=_CLI_UPSTREAM_INSTANCE_ID,
            prefix=_CLI_PREFIX,
            tool_name="stdin",
            forwarded_args={
                "cwd": cwd,
                "env_fingerprint": env_fingerprint,
                "stdin_hash": stdin_hash,
            },
        )
        capture_kind = CAPTURE_KIND_STDIN_PIPE
        capture_origin: dict[str, Any] = {
            "cwd": cwd,
            "env_fingerprint": env_fingerprint,
            "stdin_hash": stdin_hash,
        }
        command_exit_code = 0
        status = "ok"
        error_block = None
    else:
        if not command_argv:
            msg = "run requires a command or --stdin"
            raise ValueError(msg)

        identity = compute_request_identity(
            upstream_instance_id=_CLI_UPSTREAM_INSTANCE_ID,
            prefix=_CLI_PREFIX,
            tool_name="run",
            forwarded_args={
                "command_argv": command_argv,
                "cwd": cwd,
                "env_fingerprint": env_fingerprint,
            },
        )
        capture_kind = CAPTURE_KIND_CLI_COMMAND
        capture_origin = {
            "command_argv": command_argv,
            "cwd": cwd,
            "env_fingerprint": env_fingerprint,
        }

        try:
            completed = subprocess.run(
                command_argv,
                capture_output=True,
                check=False,
                text=False,
            )
        except FileNotFoundError as exc:
            msg = f"command not found: {command_argv[0]}"
            raise ValueError(msg) from exc
        except OSError as exc:
            msg = f"failed to execute command: {exc}"
            raise ValueError(msg) from exc

        stdout_text = completed.stdout.decode("utf-8", errors="replace")
        stderr_text = completed.stderr.decode("utf-8", errors="replace")
        stdout_payload, stdout_is_json = _parse_json_or_text_payload(stdout_text)
        command_exit_code = completed.returncode
        status = "ok" if command_exit_code == 0 else "error"
        error_block = None
        if command_exit_code != 0:
            error_block = {
                "code": "COMMAND_EXIT_NONZERO",
                "message": f"command exited with code {command_exit_code}",
                "details": {"exit_code": command_exit_code},
            }
        if stdout_is_json and not stderr_text and command_exit_code == 0:
            payload = stdout_payload
        else:
            payload = {
                "stdout": stdout_payload if stdout_is_json else stdout_text,
                "stderr": stderr_text,
                "exit_code": command_exit_code,
                "stdout_is_json": stdout_is_json,
            }

    if tags:
        capture_origin["tags"] = tags

    capture_payload = execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "capture_kind": capture_kind,
            "capture_origin": capture_origin,
            "capture_key": identity.request_key,
            "prefix": _CLI_PREFIX,
            "tool_name": "stdin" if args.stdin else "run",
            "upstream_instance_id": _CLI_UPSTREAM_INSTANCE_ID,
            "request_key": identity.request_key,
            "request_args_hash": identity.request_args_hash,
            "request_args_prefix": identity.request_args_prefix,
            "payload": payload,
            "status": status,
            "error": error_block,
            "ttl_seconds": ttl_seconds,
            "tags": tags,
            "meta": {
                "capture_mode": "stdin" if args.stdin else "command",
            },
        },
    )
    if isinstance(capture_payload, dict):
        capture_payload["records"] = _estimate_records(payload)
        capture_payload["command_exit_code"] = command_exit_code
        capture_payload["tags"] = tags
    return capture_payload


def _execute_diff(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift diff`` by comparing reconstructed envelopes."""
    max_lines = args.max_lines
    if max_lines <= 0:
        msg = "--max-lines must be > 0"
        raise ValueError(msg)

    left = _fetch_artifact_for_diff(
        runtime,
        artifact_id=args.left_artifact_id,
    )
    right = _fetch_artifact_for_diff(
        runtime,
        artifact_id=args.right_artifact_id,
    )
    left_hash = left["payload_hash_full"]
    right_hash = right["payload_hash_full"]
    equal = left_hash == right_hash
    diff_lines: list[str] = []
    diff_truncated = False
    if not equal:
        diff_lines, diff_truncated = _build_diff_lines(
            left_payload=left["envelope"],
            right_payload=right["envelope"],
            left_label=str(left["artifact_id"]),
            right_label=str(right["artifact_id"]),
            max_lines=max_lines,
        )
    return {
        "left_artifact_id": left["artifact_id"],
        "right_artifact_id": right["artifact_id"],
        "left_payload_hash": left_hash,
        "right_payload_hash": right_hash,
        "left_payload_bytes": left["payload_total_bytes"],
        "right_payload_bytes": right["payload_total_bytes"],
        "equal": equal,
        "diff_lines": diff_lines,
        "diff_truncated": diff_truncated,
    }


def _dispatch_command(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], str]:
    """Run the selected command and return payload plus formatter kind."""
    if args.command == "list":
        return _execute_list(runtime, args), "list"
    if args.command == "schema":
        return _execute_schema(runtime, args), "schema"
    if args.command == "get":
        return _execute_get(runtime, args), "generic"
    if args.command == "query":
        return _execute_query(runtime, args), "generic"
    if args.command == "code":
        return _execute_code(runtime, args), "generic"
    if args.command == "run":
        return _execute_run(runtime, args), "run"
    if args.command == "diff":
        return _execute_diff(runtime, args), "diff"
    msg = f"unsupported command: {args.command}"
    raise ValueError(msg)


def _add_common_json_flag(parser: argparse.ArgumentParser) -> None:
    """Add ``--json`` output mode flag to a subparser."""
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )


def _build_parser() -> argparse.ArgumentParser:
    """Build the ``sift`` CLI parser."""
    parser = argparse.ArgumentParser(
        prog="sift",
        description="Protocol-agnostic artifact retrieval CLI",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override DATA_DIR (default: .sift-mcp/)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list", help="List recent artifacts")
    list_parser.add_argument("--limit", type=int, default=25)
    list_parser.add_argument("--cursor", default=None)
    list_parser.add_argument(
        "--order-by",
        choices=["created_seq_desc", "last_seen_desc", "chain_seq_asc"],
        default="created_seq_desc",
    )
    list_parser.add_argument("--query", default=None)
    list_parser.add_argument("--status", choices=["ok", "error"], default=None)
    list_parser.add_argument(
        "--kind",
        choices=["data", "derived_query", "derived_codegen"],
        default=None,
    )
    list_parser.add_argument("--source-tool", default=None)
    list_parser.add_argument("--source-tool-prefix", default=None)
    list_parser.add_argument("--upstream-instance-id", default=None)
    list_parser.add_argument("--request-key", default=None)
    list_parser.add_argument(
        "--capture-kind",
        choices=[
            "mcp_tool",
            "cli_command",
            "stdin_pipe",
            "file_ingest",
            "derived_query",
            "derived_codegen",
        ],
        default=None,
    )
    list_parser.add_argument("--capture-key", default=None)
    list_parser.add_argument("--parent-artifact-id", default=None)
    list_parser.add_argument("--include-deleted", action="store_true")
    _add_common_json_flag(list_parser)

    schema_parser = sub.add_parser("schema", help="Describe artifact schema")
    schema_parser.add_argument("artifact_id")
    schema_parser.add_argument(
        "--scope",
        choices=["all_related", "single"],
        default="all_related",
    )
    _add_common_json_flag(schema_parser)

    get_parser = sub.add_parser("get", help="Retrieve stored artifact payload")
    get_parser.add_argument("artifact_id")
    get_parser.add_argument(
        "--scope",
        choices=["all_related", "single"],
        default="all_related",
    )
    get_parser.add_argument(
        "--target",
        choices=["envelope", "mapped"],
        default="envelope",
    )
    get_parser.add_argument("--jsonpath", default=None)
    get_parser.add_argument("--limit", type=int, default=50)
    get_parser.add_argument("--cursor", default=None)
    _add_common_json_flag(get_parser)

    query_parser = sub.add_parser("query", help="Select/query artifact rows")
    query_parser.add_argument("artifact_id")
    query_parser.add_argument("root_path")
    query_parser.add_argument(
        "--scope",
        choices=["all_related", "single"],
        default="all_related",
    )
    query_parser.add_argument(
        "--select",
        action="append",
        default=[],
        help="Projection path(s); repeat or comma-separate",
    )
    query_parser.add_argument(
        "--where",
        default=None,
        help="Structured filter JSON object",
    )
    query_parser.add_argument("--limit", type=int, default=50)
    query_parser.add_argument("--cursor", default=None)
    query_parser.add_argument("--order-by", default=None)
    query_parser.add_argument("--distinct", action="store_true")
    query_parser.add_argument("--count-only", action="store_true")
    _add_common_json_flag(query_parser)

    code_parser = sub.add_parser(
        "code",
        help="Run sandboxed Python over artifact root data",
    )
    code_parser.add_argument("artifact_id")
    code_parser.add_argument("root_path")
    code_source_group = code_parser.add_mutually_exclusive_group(required=True)
    code_source_group.add_argument(
        "--code",
        dest="code_inline",
        default=None,
        help="Inline Python source defining run(data, schema, params)",
    )
    code_source_group.add_argument(
        "--file",
        dest="code_file",
        default=None,
        help="Path to Python source defining run(...)",
    )
    code_source_group.add_argument(
        "--expr",
        dest="code_expr",
        default=None,
        help="Python expression evaluated with pandas DataFrame `df`",
    )
    code_parser.add_argument(
        "--params",
        default=None,
        help="JSON object passed to run(..., params)",
    )
    _add_common_json_flag(code_parser)

    run_parser = sub.add_parser("run", help="Capture command output")
    run_parser.add_argument(
        "--stdin",
        action="store_true",
        help="Capture payload from stdin instead of running a command",
    )
    run_parser.add_argument(
        "--ttl",
        default=None,
        help="Artifact TTL (e.g. 30m, 24h, 7d, none)",
    )
    run_parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Tag(s) to store with capture metadata (repeat or comma-separate)",
    )
    run_parser.add_argument(
        "command_argv",
        nargs=argparse.REMAINDER,
        help="Command to execute (use -- to separate command args)",
    )
    _add_common_json_flag(run_parser)

    diff_parser = sub.add_parser(
        "diff",
        help="Compare two stored artifact payloads",
    )
    diff_parser.add_argument("left_artifact_id")
    diff_parser.add_argument("right_artifact_id")
    diff_parser.add_argument(
        "--max-lines",
        type=int,
        default=_DIFF_DEFAULT_MAX_LINES,
        help="Maximum unified diff lines to print when payloads differ",
    )
    _add_common_json_flag(diff_parser)

    return parser


def serve(argv: list[str] | None = None) -> int:
    """Run the ``sift`` CLI and return an exit code."""
    args = _build_parser().parse_args(argv)
    try:
        with _runtime_context(data_dir_override=args.data_dir) as runtime:
            payload, mode = _dispatch_command(runtime, args)
    except ValueError as exc:
        _write_line(str(exc), stream=sys.stderr)
        return 1
    except Exception as exc:
        _write_line(f"sift failed: {exc}", stream=sys.stderr)
        return 1

    if _is_error_response(payload):
        if args.json:
            _emit_json(payload)
        else:
            _write_line(
                f"{payload['code']}: {payload['message']}",
                stream=sys.stderr,
            )
        return 1

    if args.json:
        _emit_json(payload)
        if mode == "run":
            command_exit_code = payload.get("command_exit_code")
            if isinstance(command_exit_code, int):
                return command_exit_code
        return 0

    if mode == "list":
        _emit_human_list(payload)
    elif mode == "schema":
        _emit_human_schema(payload)
    elif mode == "run":
        _emit_human_run(payload)
    elif mode == "diff":
        _emit_human_diff(payload)
    else:
        _emit_human_generic(payload)

    if mode == "run":
        command_exit_code = payload.get("command_exit_code")
        if isinstance(command_exit_code, int):
            return command_exit_code
    return 0


def cli() -> None:
    """Entrypoint used by the ``sift`` console script."""
    raise SystemExit(serve())


__all__ = ["cli", "serve"]
