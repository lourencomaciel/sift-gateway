"""CLI entrypoint for protocol-agnostic artifact retrieval commands."""

from __future__ import annotations

import argparse
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, cast

from sift_gateway import __version__
from sift_gateway.config import load_gateway_config
from sift_gateway.constants import (
    CAPTURE_KIND_CLI_COMMAND,
    RESPONSE_TYPE_ERROR,
    WORKSPACE_ID,
)
from sift_gateway.core.artifact_capture import (
    execute_artifact_capture,
)
from sift_gateway.core.artifact_code import execute_artifact_code
from sift_gateway.core.artifact_describe import execute_artifact_describe
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.envelope.responses import (
    gateway_error,
    gateway_tool_result,
    select_response_mode,
)
from sift_gateway.lifecycle import ensure_data_dirs
from sift_gateway.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)
from sift_gateway.mcp.server import GatewayServer
from sift_gateway.pagination.contract import build_upstream_pagination_meta
from sift_gateway.pagination.extract import (
    PaginationAssessment,
    assess_pagination,
)
from sift_gateway.request_identity import compute_request_identity
from sift_gateway.response_sample import (
    build_representative_item_sample,
    resolve_item_sequence_with_path,
)
from sift_gateway.tools.usage_hint import (
    build_code_query_usage,
    render_code_query_usage_hint,
    schema_primary_root_path,
)

_CLI_SESSION_ID = "cli"
_CLI_PREFIX = "cli"
_CLI_UPSTREAM_INSTANCE_ID = "cli_local"
_DEFAULT_TTL_RAW = "24h"
_TTL_PATTERN = re.compile(r"^([1-9][0-9]*)([smhd]?)$")
_INT_PATTERN = re.compile(r"^[+-]?[0-9]+$")

_FETCH_CLI_CONTINUE_PARENT_SQL = """
SELECT artifact_id, deleted_at, capture_kind, chain_seq
FROM artifacts
WHERE workspace_id = %s
  AND artifact_id = %s
"""

_CLI_CONTINUE_PARENT_COLUMNS = [
    "artifact_id",
    "deleted_at",
    "capture_kind",
    "chain_seq",
]


@dataclass
class _RunCaptureExecution:
    """Execution payload for one ``sift-gateway run`` invocation mode."""

    payload: Any
    identity: Any
    capture_kind: str
    capture_origin: dict[str, Any]
    command_exit_code: int
    status: str
    error_block: dict[str, Any] | None
    pagination_meta: dict[str, Any]
    pagination_assessment: PaginationAssessment | None


def _migrations_dir() -> Path:
    """Return the SQLite migrations directory path."""
    return Path(__file__).resolve().parent / "db" / "migrations_sqlite"


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


def _normalize_code_flag_values(
    raw_values: list[str] | None,
    *,
    flag: str,
) -> list[str]:
    """Normalize repeatable ``code`` flags and enforce non-empty values."""
    values = raw_values or []
    normalized: list[str] = []
    for raw_value in values:
        value = raw_value.strip()
        if not value:
            msg = f"{flag} values must be non-empty strings"
            raise ValueError(msg)
        normalized.append(value)
    return normalized


def _resolve_code_target_arguments(
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Resolve code-target args into single-artifact or multi-artifact shape."""
    raw_positional_artifact_id = getattr(args, "artifact_id", None)
    positional_artifact_id = (
        raw_positional_artifact_id.strip()
        if isinstance(raw_positional_artifact_id, str)
        and raw_positional_artifact_id.strip()
        else None
    )
    raw_positional_root_path = getattr(args, "root_path", None)
    positional_root_path = (
        raw_positional_root_path.strip()
        if isinstance(raw_positional_root_path, str)
        and raw_positional_root_path.strip()
        else None
    )
    raw_flag_artifact_ids = getattr(args, "artifact_ids", None)
    raw_flag_root_paths = getattr(args, "root_paths", None)
    flag_artifact_ids = _normalize_code_flag_values(
        raw_flag_artifact_ids
        if isinstance(raw_flag_artifact_ids, list)
        else None,
        flag="--artifact-id",
    )
    flag_root_paths = _normalize_code_flag_values(
        raw_flag_root_paths if isinstance(raw_flag_root_paths, list) else None,
        flag="--root-path",
    )

    has_positional_artifact = positional_artifact_id is not None
    has_positional_root = positional_root_path is not None
    uses_positionals = has_positional_artifact or has_positional_root
    uses_flags = bool(flag_artifact_ids or flag_root_paths)

    if uses_positionals and uses_flags:
        msg = (
            "cannot mix positional artifact_id/root_path with "
            "--artifact-id/--root-path"
        )
        raise ValueError(msg)

    if uses_positionals:
        if not (has_positional_artifact and has_positional_root):
            msg = "code positional mode requires both artifact_id and root_path"
            raise ValueError(msg)
        return {
            "artifact_id": positional_artifact_id,
            "root_path": positional_root_path,
        }

    if not flag_artifact_ids:
        msg = (
            "missing artifact target; provide positional artifact_id/root_path "
            "or --artifact-id/--root-path"
        )
        raise ValueError(msg)
    if not flag_root_paths:
        msg = (
            "missing root path; provide positional artifact_id/root_path "
            "or --root-path"
        )
        raise ValueError(msg)
    if len(set(flag_artifact_ids)) != len(flag_artifact_ids):
        msg = "duplicate --artifact-id values are not supported"
        raise ValueError(msg)

    root_paths: dict[str, str]
    if len(flag_root_paths) == 1:
        root_paths = dict.fromkeys(flag_artifact_ids, flag_root_paths[0])
    elif len(flag_root_paths) == len(flag_artifact_ids):
        root_paths = dict(
            zip(
                flag_artifact_ids,
                flag_root_paths,
                strict=True,
            )
        )
    else:
        msg = (
            "when using multiple --artifact-id values, provide one --root-path "
            "or repeat --root-path once per --artifact-id"
        )
        raise ValueError(msg)

    return {
        "artifact_ids": flag_artifact_ids,
        "root_paths": root_paths,
    }


def _load_code_source(args: argparse.Namespace) -> str:
    """Load Python source from ``--code`` or ``--file``."""
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
    msg = "missing code source; provide --code or --file"
    raise ValueError(msg)


def _parse_ttl_seconds(raw_ttl: str | None) -> int | None:
    """Parse CLI TTL values (e.g., ``30m``, ``24h``, ``7d``)."""
    env_ttl = os.environ.get("SIFT_GATEWAY_TTL")
    if env_ttl is None:
        env_ttl = os.environ.get("SIFT_TTL", _DEFAULT_TTL_RAW)
    candidate = (
        raw_ttl.strip().lower()
        if isinstance(raw_ttl, str) and raw_ttl.strip()
        else env_ttl.strip().lower()
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
    """Normalize remainder argv for ``sift-gateway run -- <cmd>``."""
    argv = list(raw_argv)
    if argv and argv[0] == "--":
        return argv[1:]
    return argv


def _coerce_cli_flag_value(raw_value: str) -> Any:
    """Coerce a CLI flag value into stable JSON-friendly scalar types."""
    value = raw_value.strip()
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if _INT_PATTERN.fullmatch(value):
        unsigned = value[1:] if value and value[0] in {"+", "-"} else value
        # Preserve string cursor/token values that rely on leading zeroes.
        if len(unsigned) > 1 and unsigned.startswith("0"):
            return value
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _is_cli_flag_token(token: str) -> bool:
    """Return whether token should be interpreted as a CLI flag token."""
    return bool(token) and token != "-" and token.startswith("-")


def _raw_cli_flag_key(token: str) -> str | None:
    """Extract raw key segment from one short/long option token."""
    raw_key = token[2:] if token.startswith("--") else token[1:]
    return raw_key if raw_key else None


def _apply_inline_cli_flag_assignment(
    raw_key: str,
    parsed: dict[str, Any],
) -> bool:
    """Apply ``--key=value`` assignment when present."""
    if "=" not in raw_key:
        return False
    key, raw_value = raw_key.split("=", 1)
    key = key.strip()
    if key:
        parsed[key] = _coerce_cli_flag_value(raw_value)
    return True


def _is_cli_flag_value_token(token: str | None) -> bool:
    """Return whether token can be consumed as a positional flag value."""
    if not isinstance(token, str):
        return False
    return bool(token) and token != "--" and not token.startswith("-")


def _consume_cli_flag_token(
    *,
    tokens: list[str],
    index: int,
    parsed: dict[str, Any],
) -> int:
    """Consume one flag token and return number of consumed argv entries."""
    raw_key = _raw_cli_flag_key(tokens[index])
    if raw_key is None:
        return 1
    if _apply_inline_cli_flag_assignment(raw_key, parsed):
        return 1

    key = raw_key.strip()
    if not key:
        return 1
    if key.startswith("no-") and len(key) > 3:
        parsed[key[3:]] = False
        return 1

    next_token = tokens[index + 1] if index + 1 < len(tokens) else None
    if _is_cli_flag_value_token(next_token):
        assert next_token is not None
        parsed[key] = _coerce_cli_flag_value(next_token)
        return 2

    parsed[key] = True
    return 1


def _extract_cli_flag_args(command_argv: list[str]) -> dict[str, Any]:
    """Best-effort parse of CLI-style flags from command argv."""
    if len(command_argv) <= 1:
        return {}

    parsed: dict[str, Any] = {}
    tokens = command_argv[1:]
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            break
        if not _is_cli_flag_token(token):
            index += 1
            continue
        index += _consume_cli_flag_token(
            tokens=tokens,
            index=index,
            parsed=parsed,
        )
    return parsed


def _assess_cli_pagination(
    *,
    json_value: Any,
    command_argv: list[str],
    page_number: int = 0,
) -> tuple[dict[str, Any], PaginationAssessment | None]:
    """Build capture meta pagination state for CLI command output."""
    if not command_argv:
        return {}, None

    original_args = _extract_cli_flag_args(command_argv)
    original_args["command_argv"] = list(command_argv)
    assessment = assess_pagination(
        json_value=json_value,
        pagination_config=None,
        original_args=original_args,
        upstream_prefix=_CLI_PREFIX,
        tool_name="run",
        page_number=page_number,
    )
    if assessment is None:
        return {}, None
    if assessment.state is None:
        return {}, assessment
    return {"_gateway_pagination": assessment.state.to_dict()}, assessment


def _build_cli_pagination_output(
    *,
    assessment: PaginationAssessment,
    artifact_id: str,
) -> dict[str, Any]:
    """Build model-facing pagination metadata for CLI command captures."""
    return build_upstream_pagination_meta(
        artifact_id=artifact_id,
        page_number=assessment.page_number,
        retrieval_status=assessment.retrieval_status,
        has_more=assessment.has_more,
        partial_reason=assessment.partial_reason,
        warning=assessment.warning,
        next_kind=(
            "command"
            if assessment.has_more and assessment.state is not None
            else None
        ),
        next_params=(
            assessment.state.next_params
            if assessment.state is not None
            else None
        ),
        original_args=(
            assessment.state.original_args
            if assessment.state is not None
            else None
        ),
    )


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


def _load_cli_continue_chain_seq(
    runtime: GatewayArtifactQueryRuntime,
    *,
    artifact_id: str,
) -> int:
    """Load and validate the parent chain sequence for ``run --continue-from``."""
    if runtime.db_pool is None:
        msg = "run --continue-from requires database backend"
        raise ValueError(msg)

    with runtime.db_pool.connection() as connection:
        row = connection.execute(
            _FETCH_CLI_CONTINUE_PARENT_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchone()

    if row is None:
        msg = f"artifact not found: {artifact_id}"
        raise ValueError(msg)

    loaded = dict(zip(_CLI_CONTINUE_PARENT_COLUMNS, row, strict=False))
    if loaded.get("deleted_at") is not None:
        msg = f"artifact has been deleted: {artifact_id}"
        raise ValueError(msg)
    if loaded.get("capture_kind") != CAPTURE_KIND_CLI_COMMAND:
        msg = (
            "run --continue-from requires a cli command parent artifact: "
            f"{artifact_id}"
        )
        raise ValueError(msg)

    raw_chain_seq = loaded.get("chain_seq")
    chain_seq = raw_chain_seq if isinstance(raw_chain_seq, int) else 0
    if chain_seq < 0:
        chain_seq = 0
    return chain_seq + 1


@contextmanager
def _runtime_context(
    *,
    data_dir_override: str | None,
) -> Generator[GatewayArtifactQueryRuntime, None, None]:
    """Build and yield a query runtime for CLI retrieval commands."""
    config = load_gateway_config(data_dir_override=data_dir_override)
    # Allow CLI-only workflows to bootstrap from an empty data dir.
    ensure_data_dirs(config)
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


def _is_error_response(payload: dict[str, Any]) -> bool:
    """Return whether a payload appears to be a gateway error response."""
    code = payload.get("code")
    message = payload.get("message")
    if not isinstance(code, str) or not isinstance(message, str):
        return False
    payload_type = payload.get("type")
    if payload_type == RESPONSE_TYPE_ERROR:
        return True
    # Legacy CLI/core errors still return {"code","message"} without
    # the typed gateway_error envelope.
    return payload_type is None and "response_mode" not in payload


def _write_line(text: str, *, stream: Any | None = None) -> None:
    """Write one line to the given stream."""
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


def _emit_json(payload: dict[str, Any]) -> None:
    """Emit payload as machine-readable JSON."""
    _write_line(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _emit_human_code(payload: dict[str, Any]) -> None:
    """Emit compact code-query summary."""
    artifact_id = payload.get("artifact_id")
    if isinstance(artifact_id, str):
        _write_line(f"artifact: {artifact_id}")
    _write_line(f"mode:     {payload.get('response_mode')}")
    if payload.get("response_mode") == "schema_ref":
        schemas = payload.get("schemas")
        if isinstance(schemas, list):
            _write_line(f"schema_roots: {len(schemas)}")
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        stats = metadata.get("stats")
        if isinstance(stats, dict):
            output_records = stats.get("output_records")
            if isinstance(output_records, int):
                _write_line(f"records:  {output_records}")
            bytes_out = stats.get("bytes_out")
            if isinstance(bytes_out, int):
                _write_line(f"bytes:    {bytes_out}")
    _write_line(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    )


def _run_payload_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Return run metadata, synthesizing from top-level fields if needed."""
    metadata = payload.get("metadata")
    merged: dict[str, Any]
    if isinstance(metadata, dict):
        usage = metadata.get("usage")
        merged = {"usage": usage} if isinstance(usage, dict) else {}
    else:
        merged = {}
    for key in (
        "records",
        "command_exit_code",
        "payload_total_bytes",
        "capture_kind",
        "expires_at",
        "status",
        "tags",
    ):
        if key in payload:
            merged[key] = payload[key]
    if merged:
        return merged
    return {}


def _emit_human_run_metadata(meta: dict[str, Any]) -> int | None:
    """Emit run metadata lines and return command exit code when available."""
    records = meta.get("records")
    if isinstance(records, int):
        _write_line(f"records:  {records}")
    else:
        _write_line("records:  unknown")

    _write_line(f"bytes:    {meta.get('payload_total_bytes')}")
    capture_kind = meta.get("capture_kind")
    if isinstance(capture_kind, str):
        _write_line(f"capture:  {capture_kind}")
    expires_at = meta.get("expires_at")
    if isinstance(expires_at, str) and expires_at:
        _write_line(f"expires:  {expires_at}")
    tags = meta.get("tags")
    if isinstance(tags, list) and tags:
        _write_line(f"tags:     {', '.join(str(tag) for tag in tags)}")
    command_exit_code = meta.get("command_exit_code")
    if isinstance(command_exit_code, int):
        _write_line(f"exit:     {command_exit_code}")
        return command_exit_code
    return None


def _emit_human_run_continuation(
    payload: dict[str, Any],
    *,
    artifact_id: str | None,
    command_exit_code: int | None,
) -> None:
    """Emit run continuation, schema, and follow-up hint lines."""
    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        next_payload = pagination.get("next")
        if isinstance(next_payload, dict) and (
            next_payload.get("kind") == "command"
        ):
            command_line = next_payload.get("command_line")
            if isinstance(command_line, str) and command_line:
                _write_line(f"next:     {command_line}")

    if payload.get("response_mode") == "schema_ref":
        schemas = payload.get("schemas")
        if isinstance(schemas, list):
            _write_line(f"schema_roots: {len(schemas)}")

    if artifact_id is not None and command_exit_code == 0:
        usage_hint = ""
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            raw_usage = metadata.get("usage")
            if isinstance(raw_usage, dict):
                usage_hint = render_code_query_usage_hint(raw_usage)
        if not usage_hint:
            usage_hint = render_code_query_usage_hint(
                build_code_query_usage(
                    interface="cli",
                    artifact_id=artifact_id,
                    root_path="$",
                    configured_roots=None,
                )
            )
        _write_line(f"hint:     {usage_hint}")


def _emit_human_run(payload: dict[str, Any]) -> None:
    """Emit compact run-capture summary."""
    raw_artifact_id = payload.get("artifact_id")
    artifact_id = raw_artifact_id if isinstance(raw_artifact_id, str) else None
    if artifact_id is not None:
        _write_line(f"artifact: {artifact_id}")
    _write_line(f"mode:     {payload.get('response_mode')}")
    command_exit_code = _emit_human_run_metadata(_run_payload_metadata(payload))
    _emit_human_run_continuation(
        payload,
        artifact_id=artifact_id,
        command_exit_code=command_exit_code,
    )


def _execute_code(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift-gateway code`` against the core code-query service."""
    code_source = _load_code_source(args)
    params_obj = _parse_params_json(args.params)
    target_args = _resolve_code_target_arguments(args)
    return execute_artifact_code(
        runtime,
        arguments={
            "_gateway_context": _build_gateway_context(),
            "scope": args.scope,
            **target_args,
            "code": code_source,
            "params": params_obj,
        },
    )


def _validate_run_mode_inputs(
    *,
    command_argv: list[str],
) -> None:
    """Validate required run-mode inputs before execution."""
    if not command_argv:
        msg = "run requires a command"
        raise ValueError(msg)


def _build_run_identity_args(
    *,
    command_argv: list[str],
    cwd: str,
    env_fingerprint: str,
    parent_artifact_id: str | None,
) -> dict[str, Any]:
    """Build request-identity args for command run captures."""
    identity_args: dict[str, Any] = {
        "command_argv": command_argv,
        "cwd": cwd,
        "env_fingerprint": env_fingerprint,
    }
    if parent_artifact_id is not None:
        identity_args["continue_from_artifact_id"] = parent_artifact_id
    return identity_args


def _build_run_capture_origin(
    *,
    command_argv: list[str],
    cwd: str,
    env_fingerprint: str,
    parent_artifact_id: str | None,
) -> dict[str, Any]:
    """Build capture_origin metadata for command run captures."""
    capture_origin: dict[str, Any] = {
        "command_argv": command_argv,
        "cwd": cwd,
        "env_fingerprint": env_fingerprint,
    }
    if parent_artifact_id is not None:
        capture_origin["continue_from_artifact_id"] = parent_artifact_id
    return capture_origin


def _execute_run_subprocess(
    command_argv: list[str],
) -> subprocess.CompletedProcess[bytes]:
    """Execute command and normalize process-level execution failures."""
    try:
        return subprocess.run(
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


def _run_error_block(command_exit_code: int) -> dict[str, Any] | None:
    """Build run error block for non-zero command exit codes."""
    if command_exit_code == 0:
        return None
    return {
        "code": "COMMAND_EXIT_NONZERO",
        "message": f"command exited with code {command_exit_code}",
        "details": {"exit_code": command_exit_code},
    }


def _execute_run_command_capture(
    *,
    command_argv: list[str],
    cwd: str,
    env_fingerprint: str,
    parent_artifact_id: str | None,
    chain_seq: int | None,
) -> _RunCaptureExecution:
    """Execute command-backed run capture and derive pagination metadata."""
    if not command_argv:
        msg = "run requires a command"
        raise ValueError(msg)

    identity = compute_request_identity(
        upstream_instance_id=_CLI_UPSTREAM_INSTANCE_ID,
        prefix=_CLI_PREFIX,
        tool_name="run",
        forwarded_args=_build_run_identity_args(
            command_argv=command_argv,
            cwd=cwd,
            env_fingerprint=env_fingerprint,
            parent_artifact_id=parent_artifact_id,
        ),
    )
    capture_origin = _build_run_capture_origin(
        command_argv=command_argv,
        cwd=cwd,
        env_fingerprint=env_fingerprint,
        parent_artifact_id=parent_artifact_id,
    )

    completed = _execute_run_subprocess(command_argv)
    stdout_text = completed.stdout.decode("utf-8", errors="replace")
    stderr_text = completed.stderr.decode("utf-8", errors="replace")
    stdout_payload, stdout_is_json = _parse_json_or_text_payload(stdout_text)
    command_exit_code = completed.returncode

    pagination_meta: dict[str, Any] = {}
    pagination_assessment: PaginationAssessment | None = None
    if stdout_is_json and command_exit_code == 0:
        pagination_meta, pagination_assessment = _assess_cli_pagination(
            json_value=stdout_payload,
            command_argv=command_argv,
            page_number=chain_seq if chain_seq is not None else 0,
        )

    payload: Any
    if stdout_is_json and not stderr_text and command_exit_code == 0:
        payload = stdout_payload
    else:
        payload = {
            "stdout": stdout_payload if stdout_is_json else stdout_text,
            "stderr": stderr_text,
            "exit_code": command_exit_code,
            "stdout_is_json": stdout_is_json,
        }

    return _RunCaptureExecution(
        payload=payload,
        identity=identity,
        capture_kind=CAPTURE_KIND_CLI_COMMAND,
        capture_origin=capture_origin,
        command_exit_code=command_exit_code,
        status="ok" if command_exit_code == 0 else "error",
        error_block=_run_error_block(command_exit_code),
        pagination_meta=pagination_meta,
        pagination_assessment=pagination_assessment,
    )


def _build_run_capture_arguments(
    *,
    execution: _RunCaptureExecution,
    ttl_seconds: int | None,
    tags: list[str],
    parent_artifact_id: str | None,
    chain_seq: int | None,
) -> dict[str, Any]:
    """Build artifact.capture argument payload for run invocations."""
    capture_meta: dict[str, Any] = {
        "capture_mode": "command",
    }
    if parent_artifact_id is not None:
        capture_meta["continue_from_artifact_id"] = parent_artifact_id
    capture_meta.update(execution.pagination_meta)

    capture_arguments: dict[str, Any] = {
        "_gateway_context": _build_gateway_context(),
        "capture_kind": execution.capture_kind,
        "capture_origin": execution.capture_origin,
        "capture_key": execution.identity.request_key,
        "prefix": _CLI_PREFIX,
        "tool_name": "run",
        "upstream_instance_id": _CLI_UPSTREAM_INSTANCE_ID,
        "request_key": execution.identity.request_key,
        "request_args_hash": execution.identity.request_args_hash,
        "request_args_prefix": execution.identity.request_args_prefix,
        "payload": execution.payload,
        "status": execution.status,
        "error": execution.error_block,
        "ttl_seconds": ttl_seconds,
        "tags": tags,
        "meta": capture_meta,
    }
    if parent_artifact_id is not None:
        capture_arguments["parent_artifact_id"] = parent_artifact_id
        if chain_seq is not None:
            capture_arguments["chain_seq"] = chain_seq
    return capture_arguments


def _decorate_run_capture_payload(
    capture_payload: dict[str, Any],
    *,
    execution: _RunCaptureExecution,
    tags: list[str],
    parent_artifact_id: str | None,
) -> None:
    """Attach CLI run summary fields onto artifact.capture output payload."""
    capture_payload["records"] = _estimate_records(execution.payload)
    capture_payload["command_exit_code"] = execution.command_exit_code
    capture_payload["tags"] = tags
    if parent_artifact_id is not None:
        capture_payload["source_artifact_id"] = parent_artifact_id
    artifact_id = capture_payload.get("artifact_id")
    if (
        execution.pagination_assessment is not None
        and isinstance(artifact_id, str)
        and artifact_id
    ):
        capture_payload["pagination"] = _build_cli_pagination_output(
            assessment=execution.pagination_assessment,
            artifact_id=artifact_id,
        )


def _sanitize_run_payload_for_storage(
    runtime: GatewayArtifactQueryRuntime,
    payload: Any,
) -> Any:
    """Apply gateway redaction to persisted run payloads when available."""
    gateway = getattr(runtime, "gateway", None)
    sanitize = getattr(gateway, "_sanitize_tool_result", None)
    if not callable(sanitize):
        return payload
    sanitized = sanitize({"payload": payload})
    if (
        not isinstance(sanitized, dict)
        or sanitized.get("type") == RESPONSE_TYPE_ERROR
    ):
        raise ValueError("response redaction failed")
    if "payload" in sanitized:
        return sanitized["payload"]
    raise ValueError("response redaction failed")


def _resolve_run_schema_ref(
    runtime: GatewayArtifactQueryRuntime,
    *,
    artifact_id: str,
) -> list[dict[str, Any]]:
    """Load verbose schema payload for one run artifact."""
    try:
        describe = execute_artifact_describe(
            runtime,
            arguments={
                "_gateway_context": _build_gateway_context(),
                "artifact_id": artifact_id,
                "scope": "single",
            },
        )
    except Exception:
        return []
    if not isinstance(describe, dict):
        return []

    raw_schemas = describe.get("schemas")
    if not isinstance(raw_schemas, list):
        return []
    return [schema for schema in raw_schemas if isinstance(schema, dict)]


def _runtime_jsonpath_limit(runtime: Any, field: str, default: int) -> int:
    """Return a positive integer JSONPath limit from runtime or fallback."""
    raw_value = getattr(runtime, field, default)
    if isinstance(raw_value, int) and raw_value > 0:
        return raw_value
    return default


def _resolve_run_sample_ref(
    runtime: GatewayArtifactQueryRuntime,
    *,
    payload: Any,
    root_path: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Build representative sample payload for run schema-ref responses."""
    items, resolved_root_path = resolve_item_sequence_with_path(
        payload,
        root_path=root_path,
        max_jsonpath_length=_runtime_jsonpath_limit(
            runtime, "max_jsonpath_length", 4096
        ),
        max_path_segments=_runtime_jsonpath_limit(
            runtime, "max_path_segments", 64
        ),
        max_wildcard_expansion_total=_runtime_jsonpath_limit(
            runtime, "max_wildcard_expansion_total", 10_000
        ),
    )
    if items is None:
        return None, resolved_root_path
    return build_representative_item_sample(items), resolved_root_path


def _build_run_lineage(
    *,
    artifact_id: str,
    parent_artifact_id: str | None,
    chain_seq: int | None,
) -> dict[str, Any]:
    """Build lineage metadata for CLI run responses."""
    lineage: dict[str, Any] = {
        "scope": "single",
        "artifact_ids": [artifact_id],
    }
    if parent_artifact_id is not None:
        lineage["parent_artifact_id"] = parent_artifact_id
    if chain_seq is not None:
        lineage["chain_seq"] = chain_seq
    return lineage


def _execute_run(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Execute ``sift-gateway run`` by capturing command output or stdin."""
    tags = _normalize_tags(args.tag)
    ttl_seconds = _parse_ttl_seconds(args.ttl)
    cwd = str(Path.cwd())
    env_fingerprint = _environment_fingerprint()
    parent_artifact_id: str | None = args.continue_from
    command_argv = _normalize_command_argv(args.command_argv)
    _validate_run_mode_inputs(
        command_argv=command_argv,
    )

    chain_seq: int | None = None
    if parent_artifact_id is not None:
        chain_seq = _load_cli_continue_chain_seq(
            runtime,
            artifact_id=parent_artifact_id,
        )

    execution = _execute_run_command_capture(
        command_argv=command_argv,
        cwd=cwd,
        env_fingerprint=env_fingerprint,
        parent_artifact_id=parent_artifact_id,
        chain_seq=chain_seq,
    )
    if tags:
        execution.capture_origin["tags"] = tags
    try:
        execution.payload = _sanitize_run_payload_for_storage(
            runtime,
            execution.payload,
        )
    except ValueError:
        return gateway_error("INTERNAL", "response redaction failed")

    capture_arguments = _build_run_capture_arguments(
        execution=execution,
        ttl_seconds=ttl_seconds,
        tags=tags,
        parent_artifact_id=parent_artifact_id,
        chain_seq=chain_seq,
    )

    capture_payload = execute_artifact_capture(
        runtime,
        arguments=capture_arguments,
    )
    if isinstance(capture_payload, dict):
        _decorate_run_capture_payload(
            capture_payload,
            execution=execution,
            tags=tags,
            parent_artifact_id=parent_artifact_id,
        )
        artifact_id = capture_payload.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id:
            return capture_payload

        pagination_payload = capture_payload.get("pagination")
        pagination = (
            pagination_payload if isinstance(pagination_payload, dict) else None
        )
        lineage = _build_run_lineage(
            artifact_id=artifact_id,
            parent_artifact_id=parent_artifact_id,
            chain_seq=chain_seq,
        )
        records = capture_payload.get("records")
        payload_total_bytes = capture_payload.get("payload_total_bytes")
        capture_kind = capture_payload.get("capture_kind")
        expires_at = capture_payload.get("expires_at")
        status = capture_payload.get("status")
        metadata: dict[str, Any] = {}

        representative_sample, sample_root_path = _resolve_run_sample_ref(
            runtime,
            payload=execution.payload,
        )
        schemas: list[dict[str, Any]] = []
        if representative_sample is None:
            schemas = _resolve_run_schema_ref(
                runtime,
                artifact_id=artifact_id,
            )
            representative_sample, sample_root_path = _resolve_run_sample_ref(
                runtime,
                payload=execution.payload,
                root_path=schema_primary_root_path(schemas),
            )
        usage_root_path = (
            sample_root_path
            if isinstance(sample_root_path, str) and sample_root_path
            else schema_primary_root_path(schemas)
        )
        configured_roots = getattr(
            runtime, "code_query_allowed_import_roots", None
        )
        metadata["usage"] = build_code_query_usage(
            interface="cli",
            artifact_id=artifact_id,
            root_path=usage_root_path,
            configured_roots=configured_roots,
        )
        full_payload = gateway_tool_result(
            response_mode="full",
            artifact_id=artifact_id,
            payload=execution.payload,
            lineage=lineage,
            pagination=pagination,
            metadata=metadata,
        )
        schema_ref_payload = gateway_tool_result(
            response_mode="schema_ref",
            artifact_id=artifact_id,
            schemas=schemas,
            lineage=lineage,
            pagination=pagination,
            metadata=metadata,
        )
        if representative_sample is not None:
            schema_ref_payload.pop("schemas", None)
            schema_ref_payload.update(representative_sample)
        for response_payload in (full_payload, schema_ref_payload):
            response_payload["records"] = records
            response_payload["command_exit_code"] = execution.command_exit_code
            response_payload["payload_total_bytes"] = payload_total_bytes
            response_payload["capture_kind"] = capture_kind
            response_payload["expires_at"] = expires_at
            response_payload["status"] = status
            response_payload["tags"] = tags
            if parent_artifact_id is not None:
                response_payload["source_artifact_id"] = parent_artifact_id
        has_pagination = (
            pagination is not None or parent_artifact_id is not None
        )
        raw_max_bytes = getattr(runtime, "max_bytes_out", 5_000_000)
        max_bytes = (
            raw_max_bytes
            if isinstance(raw_max_bytes, int) and raw_max_bytes > 0
            else 5_000_000
        )
        response_mode = select_response_mode(
            has_pagination=has_pagination,
            full_payload=full_payload,
            schema_ref_payload=schema_ref_payload,
            max_bytes=max_bytes,
        )
        if response_mode == "schema_ref":
            return schema_ref_payload
        return full_payload
    return capture_payload


def _dispatch_command(
    runtime: GatewayArtifactQueryRuntime,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], str]:
    """Run the selected command and return payload plus formatter kind."""
    if args.command == "code":
        return _execute_code(runtime, args), "code"
    if args.command == "run":
        return _execute_run(runtime, args), "run"
    msg = f"unsupported command: {args.command}"
    raise ValueError(msg)


def _sanitize_cli_payload(runtime: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Apply outbound redaction when runtime exposes the gateway sanitizer."""
    gateway = getattr(runtime, "gateway", None)
    sanitize = getattr(gateway, "_sanitize_tool_result", None)
    if callable(sanitize):
        sanitized = sanitize(payload)
        if isinstance(sanitized, dict):
            return cast(dict[str, Any], sanitized)
    return payload


def _add_common_json_flag(parser: argparse.ArgumentParser) -> None:
    """Add ``--json`` output mode flag to a subparser."""
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )


def _build_parser() -> argparse.ArgumentParser:
    """Build the artifact-mode parser for ``sift-gateway``."""
    parser = argparse.ArgumentParser(
        prog="sift-gateway",
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
        help="Override DATA_DIR (default: .sift-gateway/)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    code_parser = sub.add_parser(
        "code",
        help="Run sandboxed Python over artifact root data",
    )
    code_parser.add_argument(
        "artifact_id",
        nargs="?",
        help=(
            "Artifact id in single-artifact mode. "
            "Omit when using repeated --artifact-id."
        ),
    )
    code_parser.add_argument(
        "root_path",
        nargs="?",
        help=(
            "Root path in single-artifact mode. "
            "Omit when using --root-path flag mode."
        ),
    )
    code_parser.add_argument(
        "--artifact-id",
        dest="artifact_ids",
        action="append",
        default=[],
        help=(
            "Artifact id for multi-artifact code queries. "
            "Repeat to include multiple artifacts."
        ),
    )
    code_parser.add_argument(
        "--root-path",
        dest="root_paths",
        action="append",
        default=[],
        help=(
            "Root path for --artifact-id mode. "
            "Provide once for all artifacts, or repeat once per artifact "
            "in order."
        ),
    )
    code_parser.add_argument(
        "--scope",
        choices=["all_related", "single"],
        default="all_related",
        help="Lineage scope (default: all_related)",
    )
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
    code_parser.add_argument(
        "--params",
        default=None,
        help="JSON object passed to run(..., params)",
    )
    _add_common_json_flag(code_parser)

    run_parser = sub.add_parser("run", help="Capture command output")
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
        "--continue-from",
        default=None,
        help=(
            "Parent artifact_id for manual pagination chaining. "
            "Use with an explicit upstream continuation command after --."
        ),
    )
    run_parser.add_argument(
        "command_argv",
        nargs=argparse.REMAINDER,
        help="Command to execute (use -- to separate command args)",
    )
    _add_common_json_flag(run_parser)

    return parser


def _dispatch_cli_payload(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], str]:
    """Dispatch one CLI command and apply standard payload sanitation."""
    with _runtime_context(data_dir_override=args.data_dir) as runtime:
        payload, mode = _dispatch_command(runtime, args)
        payload = _sanitize_cli_payload(runtime, payload)
    return payload, mode


def _emit_error_response(payload: dict[str, Any], *, json_mode: bool) -> None:
    """Emit one error payload in requested output mode."""
    if json_mode:
        _emit_json(payload)
        return
    _write_line(
        f"{payload['code']}: {payload['message']}",
        stream=sys.stderr,
    )


def _emit_human_mode_payload(mode: str, payload: dict[str, Any]) -> None:
    """Emit successful payload in human mode by dispatch mode."""
    emitters: dict[str, Any] = {
        "code": _emit_human_code,
        "run": _emit_human_run,
    }
    emitter = emitters.get(mode, _emit_human_code)
    emitter(payload)


def _command_exit_code(mode: str, payload: dict[str, Any]) -> int | None:
    """Return command exit code for run mode, when present."""
    if mode != "run":
        return None
    command_exit_code = payload.get("command_exit_code")
    if not isinstance(command_exit_code, int):
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            command_exit_code = metadata.get("command_exit_code")
    if isinstance(command_exit_code, int):
        return command_exit_code
    return None


def _strip_run_model_noise_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove run-only transport fields not useful for model reasoning."""
    keys_to_drop = (
        "command_exit_code",
        "payload_total_bytes",
        "capture_kind",
        "expires_at",
        "status",
        "tags",
    )
    sanitized = dict(payload)
    for key in keys_to_drop:
        sanitized.pop(key, None)
    return sanitized


def serve(argv: list[str] | None = None) -> int:
    """Run artifact CLI mode and return an exit code."""
    args = _build_parser().parse_args(argv)
    try:
        payload, mode = _dispatch_cli_payload(args)
    except ValueError as exc:
        _write_line(str(exc), stream=sys.stderr)
        return 1
    except Exception as exc:
        _write_line(f"sift-gateway failed: {exc}", stream=sys.stderr)
        return 1

    if _is_error_response(payload):
        _emit_error_response(payload, json_mode=args.json)
        return 1

    command_exit_code = _command_exit_code(mode, payload)
    if args.json:
        emit_payload = payload
        if mode == "run":
            emit_payload = _strip_run_model_noise_fields(payload)
        _emit_json(emit_payload)
    else:
        _emit_human_mode_payload(mode, payload)

    if command_exit_code is not None:
        return command_exit_code
    return 0


def cli() -> None:
    """Entrypoint used for artifact-mode execution."""
    raise SystemExit(serve())


__all__ = ["cli", "serve"]
