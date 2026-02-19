#!/usr/bin/env python3
"""Benchmark large-payload capture and retrieval paths.

Runs protocol-agnostic core services through a local runtime:
- capture (execute_artifact_capture)
- select query (execute_artifact_select)
- get query (execute_artifact_get)

The benchmark uses synthetic JSON payloads and reports median/p95 timings.
"""

from __future__ import annotations

import argparse
from collections.abc import Generator
from contextlib import contextmanager
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from typing import Any

from sift_gateway.config import load_gateway_config
from sift_gateway.constants import (
    CAPTURE_KIND_CLI_COMMAND,
)
from sift_gateway.core.artifact_capture import execute_artifact_capture
from sift_gateway.core.artifact_get import execute_artifact_get
from sift_gateway.core.artifact_select import execute_artifact_select
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)
from sift_gateway.mcp.server import GatewayServer
from sift_gateway.obs.logging import configure_logging

_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "sift_gateway"
    / "db"
    / "migrations_sqlite"
)
_SESSION_ID = "benchmark_cli_agnostic"
_GATEWAY_CONTEXT = {"session_id": _SESSION_ID}


def _build_payload(rows: int) -> list[dict[str, Any]]:
    return [
        {
            "id": i,
            "state": "open" if i % 3 else "closed",
            "title": f"Synthetic issue {i}",
            "user": {"login": f"user_{i % 97}"},
            "labels": (
                [{"name": f"label_{i % 5}"}, {"name": f"team_{i % 7}"}]
                if i % 2 == 0
                else []
            ),
            "created_at": f"2026-01-{(i % 28) + 1:02d}T12:00:00Z",
            "draft": i % 11 == 0,
        }
        for i in range(rows)
    ]


def _is_error_response(payload: dict[str, Any]) -> bool:
    return isinstance(payload.get("code"), str) and isinstance(
        payload.get("message"), str
    )


def _raise_if_error(payload: dict[str, Any], *, operation: str) -> None:
    if _is_error_response(payload):
        code = payload.get("code")
        message = payload.get("message")
        raise RuntimeError(f"{operation} failed: {code}: {message}")


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = max(0.0, min(1.0, pct / 100.0))
    idx = round(rank * (len(values) - 1))
    return sorted(values)[idx]


def _summary(values: list[float]) -> dict[str, float]:
    return {
        "min_ms": min(values) if values else 0.0,
        "p50_ms": statistics.median(values) if values else 0.0,
        "p95_ms": _percentile(values, 95.0),
        "max_ms": max(values) if values else 0.0,
    }


@contextmanager
def _runtime(
    *,
    data_dir: str | None,
) -> Generator[GatewayArtifactQueryRuntime, None, None]:
    config = load_gateway_config(data_dir_override=data_dir)
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


def _parse_rows(raw_rows: str) -> list[int]:
    values: list[int] = []
    for token in raw_rows.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        value = int(stripped)
        if value <= 0:
            msg = f"row counts must be positive: {value}"
            raise ValueError(msg)
        values.append(value)
    if not values:
        raise ValueError("at least one row count is required")
    return values


def _run_case(
    runtime: GatewayArtifactQueryRuntime,
    *,
    rows: int,
    repeats: int,
    query_limit: int,
) -> dict[str, Any]:
    capture_times_ms: list[float] = []
    select_times_ms: list[float] = []
    get_times_ms: list[float] = []
    payload_bytes: list[int] = []
    query_item_counts: list[int] = []

    for attempt in range(repeats):
        payload = _build_payload(rows)
        request_key = f"bench:{rows}:{attempt}:{time.time_ns()}"

        start = time.perf_counter()
        capture = execute_artifact_capture(
            runtime,
            arguments={
                "_gateway_context": _GATEWAY_CONTEXT,
                "capture_kind": CAPTURE_KIND_CLI_COMMAND,
                "capture_origin": {
                    "command_argv": ["benchmark"],
                    "cwd": str(Path.cwd()),
                    "case_rows": rows,
                    "attempt": attempt,
                },
                "capture_key": request_key,
                "prefix": "bench",
                "tool_name": "synthetic",
                "upstream_instance_id": "bench_local",
                "request_key": request_key,
                "request_args_hash": request_key,
                "request_args_prefix": "bench",
                "payload": payload,
                "status": "ok",
                "no_cache": True,
            },
        )
        capture_times_ms.append((time.perf_counter() - start) * 1000.0)
        _raise_if_error(capture, operation="capture")
        artifact_id = capture.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id:
            raise RuntimeError("capture missing artifact_id")

        payload_total_bytes = capture.get("payload_total_bytes")
        if isinstance(payload_total_bytes, int):
            payload_bytes.append(payload_total_bytes)

        start = time.perf_counter()
        select_result = execute_artifact_select(
            runtime,
            arguments={
                "_gateway_context": _GATEWAY_CONTEXT,
                "artifact_id": artifact_id,
                "scope": "single",
                "root_path": "$",
                "select_paths": ["id", "state", "user.login"],
                "limit": query_limit,
            },
        )
        select_times_ms.append((time.perf_counter() - start) * 1000.0)
        _raise_if_error(select_result, operation="select")
        items = select_result.get("items")
        if isinstance(items, list):
            query_item_counts.append(len(items))

        start = time.perf_counter()
        get_result = execute_artifact_get(
            runtime,
            arguments={
                "_gateway_context": _GATEWAY_CONTEXT,
                "artifact_id": artifact_id,
                "scope": "single",
                "target": "mapped",
                "jsonpath": "$",
                "limit": query_limit,
            },
        )
        get_times_ms.append((time.perf_counter() - start) * 1000.0)
        _raise_if_error(get_result, operation="get")

    return {
        "rows": rows,
        "repeats": repeats,
        "query_limit": query_limit,
        "payload_total_bytes_p50": statistics.median(payload_bytes)
        if payload_bytes
        else 0,
        "select_items_p50": statistics.median(query_item_counts)
        if query_item_counts
        else 0,
        "capture": _summary(capture_times_ms),
        "select": _summary(select_times_ms),
        "get": _summary(get_times_ms),
    }


def _print_table(results: list[dict[str, Any]]) -> None:
    header = (
        "rows | repeats | payload_bytes_p50 | capture_p50_ms | "
        "select_p50_ms | get_p50_ms"
    )
    _write_line(header)
    _write_line("-" * len(header))
    for result in results:
        _write_line(
            f"{result['rows']} | "
            f"{result['repeats']} | "
            f"{int(result['payload_total_bytes_p50'])} | "
            f"{result['capture']['p50_ms']:.2f} | "
            f"{result['select']['p50_ms']:.2f} | "
            f"{result['get']['p50_ms']:.2f}"
        )


def _write_line(text: str) -> None:
    sys.stdout.write(text)
    sys.stdout.write("\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark large-payload capture/query/get flows",
    )
    parser.add_argument(
        "--rows",
        default="1000,5000,20000",
        help="Comma-separated synthetic row counts",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Iterations per row-count case",
    )
    parser.add_argument(
        "--query-limit",
        type=int,
        default=50,
        help="Select/get limit per request",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Optional persistent data dir (default: temp dir per run)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    return parser


def main() -> int:
    """Run benchmark cases and emit table or JSON output."""
    configure_logging(json_output=True, level="INFO")
    args = _build_parser().parse_args()
    if args.repeats <= 0:
        raise SystemExit("--repeats must be > 0")
    if args.query_limit <= 0:
        raise SystemExit("--query-limit must be > 0")

    try:
        row_counts = _parse_rows(args.rows)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if args.data_dir is not None:
        with _runtime(data_dir=args.data_dir) as runtime:
            results = [
                _run_case(
                    runtime,
                    rows=rows,
                    repeats=args.repeats,
                    query_limit=args.query_limit,
                )
                for rows in row_counts
            ]
    else:
        with tempfile.TemporaryDirectory(prefix="sift-bench-") as tmp, _runtime(
            data_dir=tmp
        ) as runtime:
            results = [
                _run_case(
                    runtime,
                    rows=rows,
                    repeats=args.repeats,
                    query_limit=args.query_limit,
                )
                for rows in row_counts
            ]

    payload = {
        "benchmark": "large_payload_capture_query",
        "row_counts": row_counts,
        "repeats": args.repeats,
        "query_limit": args.query_limit,
        "results": results,
    }

    if args.json:
        _write_line(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        )
    else:
        _print_table(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
