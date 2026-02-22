"""Sift Gateway runtime wrapper for benchmark artifact operations."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
import time
from typing import Any

import sift_gateway
from sift_gateway.config import load_gateway_config
from sift_gateway.constants import CAPTURE_KIND_CLI_COMMAND
from sift_gateway.core.artifact_capture import execute_artifact_capture
from sift_gateway.core.artifact_code import execute_artifact_code
from sift_gateway.core.artifact_describe import execute_artifact_describe
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)
from sift_gateway.mcp.server import GatewayServer

_MIGRATIONS_DIR = (
    Path(sift_gateway.__file__).resolve().parent / "db" / "migrations_sqlite"
)
_SESSION_ID = "benchmark_tier1"
_GATEWAY_CONTEXT: dict[str, str] = {"session_id": _SESSION_ID}


def _is_error_response(payload: dict[str, Any]) -> bool:
    # artifact_describe / artifact_code errors include a type marker.
    if payload.get("type") == "gateway_error":
        return True
    # artifact_capture errors use {code, message} without type.
    # Distinguish from success payloads by the absence of artifact_id.
    return (
        isinstance(payload.get("code"), str)
        and isinstance(payload.get("message"), str)
        and "artifact_id" not in payload
    )


@contextmanager
def create_runtime(
    *,
    data_dir: str | None = None,
) -> Generator[GatewayArtifactQueryRuntime, None, None]:
    """Create a temporary Sift runtime for benchmark operations."""
    config = load_gateway_config(data_dir_override=data_dir)
    # Default max_root_discovery_k (3) drops array roots for datasets
    # with more than 3 parallel arrays (e.g. weather has 4 hourly
    # arrays).  Raise the limit so all roots are discoverable.
    config.max_root_discovery_k = 20
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.resources_dir.mkdir(parents=True, exist_ok=True)
    config.blobs_bin_dir.mkdir(parents=True, exist_ok=True)
    config.blobs_payload_dir.mkdir(parents=True, exist_ok=True)
    config.tmp_dir.mkdir(parents=True, exist_ok=True)
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    backend = SqliteBackend(
        db_path=config.sqlite_path,
        busy_timeout_ms=config.sqlite_busy_timeout_ms,
    )
    try:
        with backend.connection() as connection:
            apply_migrations(connection, _MIGRATIONS_DIR)
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


def capture_payload(
    runtime: GatewayArtifactQueryRuntime,
    *,
    payload: Any,
    dataset_name: str,
    question_id: str,
) -> dict[str, Any]:
    """Capture a JSON payload as a Sift artifact.

    Returns the capture result dict with artifact_id.
    """
    request_key = f"bench:tier1:{dataset_name}:{question_id}:{time.time_ns()}"
    result = execute_artifact_capture(
        runtime,
        arguments={
            "_gateway_context": _GATEWAY_CONTEXT,
            "capture_kind": CAPTURE_KIND_CLI_COMMAND,
            "capture_origin": {
                "command_argv": ["benchmark-tier1"],
                "cwd": str(Path.cwd()),
                "dataset": dataset_name,
            },
            "capture_key": request_key,
            "prefix": "bench",
            "tool_name": dataset_name,
            "upstream_instance_id": "bench_tier1",
            "request_key": request_key,
            "request_args_hash": request_key,
            "request_args_prefix": "bench",
            "payload": payload,
            "status": "ok",
            "no_cache": True,
        },
    )
    if _is_error_response(result):
        msg = f"capture failed: {result.get('code')}: {result.get('message')}"
        raise RuntimeError(msg)
    return result


def describe_artifact(
    runtime: GatewayArtifactQueryRuntime,
    *,
    artifact_id: str,
) -> dict[str, Any]:
    """Describe an artifact to get schema information."""
    result = execute_artifact_describe(
        runtime,
        arguments={
            "_gateway_context": _GATEWAY_CONTEXT,
            "artifact_id": artifact_id,
            "scope": "single",
        },
    )
    if _is_error_response(result):
        msg = f"describe failed: {result.get('code')}: {result.get('message')}"
        raise RuntimeError(msg)
    return result


def extract_root_paths(describe_result: dict[str, Any]) -> list[str]:
    """Return all mapped root_paths from a describe result.

    Falls back to ``["$"]`` when no roots are found.
    """
    roots = describe_result.get("roots")
    if isinstance(roots, list):
        paths = [
            root.get("root_path")
            for root in roots
            if isinstance(root, dict)
            and isinstance(root.get("root_path"), str)
            and root.get("root_path")
        ]
        if paths:
            return paths
    schemas = describe_result.get("schemas")
    if isinstance(schemas, list):
        paths = [
            schema.get("root_path")
            for schema in schemas
            if isinstance(schema, dict)
            and isinstance(schema.get("root_path"), str)
            and schema.get("root_path")
        ]
        if paths:
            return paths
    return ["$"]


def execute_code(
    runtime: GatewayArtifactQueryRuntime,
    *,
    artifact_id: str,
    root_path: str,
    code: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute code against an artifact."""
    result = execute_artifact_code(
        runtime,
        {
            "_gateway_context": _GATEWAY_CONTEXT,
            "artifact_id": artifact_id,
            "root_path": root_path,
            "scope": "single",
            "code": code,
            "params": params or {},
        },
    )
    if _is_error_response(result):
        msg = (
            f"code execution failed: {result.get('code')}: "
            f"{result.get('message')}"
        )
        raise RuntimeError(msg)
    return result
