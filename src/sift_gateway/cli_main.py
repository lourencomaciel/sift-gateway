"""CLI entrypoint for protocol-agnostic artifact retrieval commands."""

from __future__ import annotations

import argparse
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
from typing import Any, cast

from sift_gateway import __version__
from sift_gateway.cli.output import (
    command_exit_code as _command_exit_code,
)
from sift_gateway.cli.output import (
    emit_error_response as _emit_error_response,
)
from sift_gateway.cli.output import (
    emit_human_mode_payload as _emit_human_mode_payload,
)
from sift_gateway.cli.output import (
    emit_json as _emit_json,
)
from sift_gateway.cli.output import (
    strip_run_model_noise_fields as _strip_run_model_noise_fields,
)
from sift_gateway.cli.output import (
    write_line as _write_line,
)
from sift_gateway.cli.parse import (
    environment_fingerprint as _environment_fingerprint,
)
from sift_gateway.cli.parse import (
    extract_cli_flag_args as _extract_cli_flag_args,
)
from sift_gateway.cli.parse import (
    load_code_source as _load_code_source,
)
from sift_gateway.cli.parse import (
    normalize_command_argv as _normalize_command_argv,
)
from sift_gateway.cli.parse import (
    normalize_tags as _normalize_tags,
)
from sift_gateway.cli.parse import (
    parse_json_or_text_payload as _parse_json_or_text_payload,
)
from sift_gateway.cli.parse import (
    parse_params_json as _parse_params_json,
)
from sift_gateway.cli.parse import (
    parse_ttl_seconds as _parse_ttl_seconds,
)
from sift_gateway.cli.parse import (
    resolve_code_target_arguments as _resolve_code_target_arguments,
)
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
    schema_primary_root_path,
)

_CLI_SESSION_ID = "cli"
_CLI_PREFIX = "cli"
_CLI_UPSTREAM_INSTANCE_ID = "cli_local"

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

        representative_sample, _sample_root_path = _resolve_run_sample_ref(
            runtime,
            payload=execution.payload,
        )
        schemas: list[dict[str, Any]] = []
        if representative_sample is None:
            schemas = _resolve_run_schema_ref(
                runtime,
                artifact_id=artifact_id,
            )
            representative_sample, _sample_root_path = _resolve_run_sample_ref(
                runtime,
                payload=execution.payload,
                root_path=schema_primary_root_path(schemas),
            )
        usage_root_path = schema_primary_root_path(schemas)
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


def _sanitize_cli_payload(
    runtime: Any, payload: dict[str, Any]
) -> dict[str, Any]:
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
