"""Output helpers for ``sift_gateway.cli_main``."""

from __future__ import annotations

import json
import sys
from typing import Any

from sift_gateway.tools.usage_hint import (
    build_code_query_usage,
    render_code_query_usage_hint,
)


def write_line(text: str, *, stream: Any | None = None) -> None:
    """Write one line to the given stream."""
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


def emit_json(payload: dict[str, Any]) -> None:
    """Emit payload as machine-readable JSON."""
    write_line(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def emit_human_code(payload: dict[str, Any]) -> None:
    """Emit compact code-query summary."""
    artifact_id = payload.get("artifact_id")
    if isinstance(artifact_id, str):
        write_line(f"artifact: {artifact_id}")
    write_line(f"mode:     {payload.get('response_mode')}")
    if payload.get("response_mode") == "schema_ref":
        schemas = payload.get("schemas")
        if isinstance(schemas, list):
            write_line(f"schema_roots: {len(schemas)}")
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        stats = metadata.get("stats")
        if isinstance(stats, dict):
            output_records = stats.get("output_records")
            if isinstance(output_records, int):
                write_line(f"records:  {output_records}")
            bytes_out = stats.get("bytes_out")
            if isinstance(bytes_out, int):
                write_line(f"bytes:    {bytes_out}")
    write_line(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def run_payload_metadata(payload: dict[str, Any]) -> dict[str, Any]:
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


def emit_human_run_metadata(meta: dict[str, Any]) -> int | None:
    """Emit run metadata lines and return command exit code when available."""
    records = meta.get("records")
    if isinstance(records, int):
        write_line(f"records:  {records}")
    else:
        write_line("records:  unknown")

    write_line(f"bytes:    {meta.get('payload_total_bytes')}")
    capture_kind = meta.get("capture_kind")
    if isinstance(capture_kind, str):
        write_line(f"capture:  {capture_kind}")
    expires_at = meta.get("expires_at")
    if isinstance(expires_at, str) and expires_at:
        write_line(f"expires:  {expires_at}")
    tags = meta.get("tags")
    if isinstance(tags, list) and tags:
        write_line(f"tags:     {', '.join(str(tag) for tag in tags)}")
    command_exit_code = meta.get("command_exit_code")
    if isinstance(command_exit_code, int):
        write_line(f"exit:     {command_exit_code}")
        return command_exit_code
    return None


def emit_human_run_continuation(
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
                write_line(f"next:     {command_line}")

    if payload.get("response_mode") == "schema_ref":
        schemas = payload.get("schemas")
        if isinstance(schemas, list):
            write_line(f"schema_roots: {len(schemas)}")

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
        write_line(f"hint:     {usage_hint}")


def emit_human_run(payload: dict[str, Any]) -> None:
    """Emit compact run-capture summary."""
    raw_artifact_id = payload.get("artifact_id")
    artifact_id = raw_artifact_id if isinstance(raw_artifact_id, str) else None
    if artifact_id is not None:
        write_line(f"artifact: {artifact_id}")
    write_line(f"mode:     {payload.get('response_mode')}")
    command_exit = emit_human_run_metadata(run_payload_metadata(payload))
    emit_human_run_continuation(
        payload,
        artifact_id=artifact_id,
        command_exit_code=command_exit,
    )


def emit_error_response(payload: dict[str, Any], *, json_mode: bool) -> None:
    """Emit one error payload in requested output mode."""
    if json_mode:
        emit_json(payload)
        return
    write_line(
        f"{payload['code']}: {payload['message']}",
        stream=sys.stderr,
    )


def emit_human_mode_payload(mode: str, payload: dict[str, Any]) -> None:
    """Emit successful payload in human mode by dispatch mode."""
    emitters: dict[str, Any] = {
        "code": emit_human_code,
        "run": emit_human_run,
    }
    emitter = emitters.get(mode, emit_human_code)
    emitter(payload)


def command_exit_code(mode: str, payload: dict[str, Any]) -> int | None:
    """Return command exit code for run mode, when present."""
    if mode != "run":
        return None
    command_exit = payload.get("command_exit_code")
    if not isinstance(command_exit, int):
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            command_exit = metadata.get("command_exit_code")
    if isinstance(command_exit, int):
        return command_exit
    return None


def strip_run_model_noise_fields(payload: dict[str, Any]) -> dict[str, Any]:
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

