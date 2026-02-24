"""Sift Gateway MCP runtime wrapper for benchmark operations.

Connects to the gateway through its FastMCP app (in-process) with
a mock upstream MCP server (subprocess via stdio) that serves the
12 benchmark datasets.  All benchmark operations flow through the
real MCP stack:

- Mirrored tool calls: gateway discovers mock upstream tools and
  proxies calls, persisting artifacts and computing schemas.
- Code queries: the ``artifact`` tool executes user-generated
  Python against persisted artifacts.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
import contextlib
from contextlib import contextmanager
from pathlib import Path
import sys
import threading
from typing import Any

from fastmcp import Client

import sift_gateway
from sift_gateway.config import load_gateway_config
from sift_gateway.config.settings import UpstreamConfig
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.rows import rows_to_dicts
from sift_gateway.core.schema_payload import build_schema_payload
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations
from sift_gateway.mcp.server import bootstrap_server
from sift_gateway.tools.artifact_describe import FETCH_SCHEMA_ROOTS_SQL
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL

_MIGRATIONS_DIR = (
    Path(sift_gateway.__file__).resolve().parent / "db" / "migrations_sqlite"
)
_SESSION_ID = "benchmark_tier1"
_GATEWAY_CONTEXT: dict[str, str] = {"session_id": _SESSION_ID}

_MOCK_UPSTREAM_SCRIPT = str(
    Path(__file__).resolve().parent / "mock_upstream.py"
)

_SCHEMA_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "schema_version",
    "schema_hash",
    "mode",
    "completeness",
    "observed_records",
    "dataset_hash",
    "traversal_contract_version",
    "map_budget_fingerprint",
]

_SCHEMA_FIELD_COLUMNS = [
    "field_path",
    "types",
    "nullable",
    "required",
    "observed_count",
    "example_value",
    "distinct_values",
    "cardinality",
]


class CodeExecutionError(RuntimeError):
    """Raised when user-generated code fails during artifact execution.

    Distinct from generic ``RuntimeError`` so callers can retry with
    error context instead of treating it as an infrastructure failure.
    """


class _MCPRuntime:
    """Wrapper around async FastMCP Client for sync benchmark code.

    Manages a dedicated event loop running on a background thread.
    The Client is connected on this loop so all async objects (the
    transport, internal tasks, etc.) live on the same loop.
    """

    def __init__(self, backend: SqliteBackend) -> None:
        self._backend = backend
        self._client: Client | None = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
        )
        self._thread.start()

    def connect(self, app: Any) -> None:
        """Connect an in-process FastMCP Client on the background loop."""
        client = Client(app)
        future = asyncio.run_coroutine_threadsafe(
            client.__aenter__(),
            self._loop,
        )
        future.result(timeout=60)
        self._client = client

    def list_tools(self) -> list[dict[str, Any]]:
        """List available MCP tools with name, description, schema."""
        if self._client is None:
            msg = "client not connected"
            raise RuntimeError(msg)
        future = asyncio.run_coroutine_threadsafe(
            self._client.list_tools(),
            self._loop,
        )
        result = future.result(timeout=30)
        tools: list[dict[str, Any]] = []
        for tool in result:
            entry: dict[str, Any] = {"name": tool.name}
            if tool.description:
                entry["description"] = tool.description
            if tool.inputSchema:
                entry["input_schema"] = dict(tool.inputSchema)
            tools.append(entry)
        return tools

    def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Call an MCP tool synchronously via the background loop."""
        if self._client is None:
            msg = "client not connected"
            raise RuntimeError(msg)
        future = asyncio.run_coroutine_threadsafe(
            self._client.call_tool(
                name,
                arguments,
                raise_on_error=False,
            ),
            self._loop,
        )
        result = future.result(timeout=120)
        # RuntimeTool returns ToolResult(structured_content=dict)
        # FastMCP Client exposes this as result.structured_content
        if result.structured_content is not None:
            return dict(result.structured_content)
        # Fallback: extract text from content blocks
        texts = []
        for block in result.content or []:
            text = getattr(block, "text", None)
            if isinstance(text, str):
                texts.append(text)
        return {"text": "\n".join(texts)}

    def fetch_schemas(self, artifact_id: str) -> list[dict[str, Any]]:
        """Fetch persisted schemas from the DB for an artifact.

        Used as a fallback when the MCP mirrored-tool response
        returns a representative sample instead of inline schemas.
        """
        with self._backend.connection() as conn:
            schema_roots = rows_to_dicts(
                conn.execute(
                    FETCH_SCHEMA_ROOTS_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchall(),
                _SCHEMA_ROOT_COLUMNS,
            )
            schemas: list[dict[str, Any]] = []
            for schema_root in schema_roots:
                root_key = schema_root.get("root_key")
                if not isinstance(root_key, str):
                    continue
                field_rows = rows_to_dicts(
                    conn.execute(
                        FETCH_SCHEMA_FIELDS_SQL,
                        (WORKSPACE_ID, artifact_id, root_key),
                    ).fetchall(),
                    _SCHEMA_FIELD_COLUMNS,
                )
                schemas.append(
                    build_schema_payload(
                        schema_root=schema_root,
                        field_rows=field_rows,
                        include_null_example_value=True,
                    )
                )
        return schemas

    def _cancel_pending(self) -> None:
        """Cancel remaining tasks on the background loop."""
        for task in asyncio.all_tasks(self._loop):
            task.cancel()

    def close(self) -> None:
        """Shut down the client, event loop, and background thread."""
        if self._client is not None:
            future = asyncio.run_coroutine_threadsafe(
                self._client.__aexit__(None, None, None),
                self._loop,
            )
            with contextlib.suppress(Exception):
                future.result(timeout=10)
        self._loop.call_soon_threadsafe(self._cancel_pending)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        self._loop.close()
        self._backend.close()


def _is_error_response(payload: dict[str, Any]) -> bool:
    """Detect whether a gateway response dict represents an error.

    Two error formats exist:

    1. **Typed errors** — ``{"type": "gateway_error", ...}``.
    2. **Untyped errors** — ``{"code": ..., "message": ...}``
       without an ``artifact_id``.

    The untyped heuristic is intentionally conservative: a response
    that has ``code`` + ``message`` *and* an ``artifact_id`` is
    treated as success.  This works for the benchmark because
    gateway tool results always include ``artifact_id`` on success.
    """
    if payload.get("type") == "gateway_error":
        return True
    return (
        isinstance(payload.get("code"), str)
        and isinstance(payload.get("message"), str)
        and "artifact_id" not in payload
    )


@contextmanager
def create_runtime(
    *,
    data_dir: str | None = None,
    bench_data_dir: str | None = None,
) -> Generator[_MCPRuntime, None, None]:
    """Create an MCP-based Sift runtime for benchmark operations.

    Boots the gateway with a mock upstream MCP server that serves
    the benchmark datasets via stdio transport.

    Args:
        data_dir: Sift data directory for config/DB/state.
        bench_data_dir: Directory containing the benchmark
            dataset JSON files (passed to mock upstream via env).
    """
    config = load_gateway_config(data_dir_override=data_dir)

    # Raise root discovery limit so all roots are found in
    # datasets with many parallel arrays (e.g. weather).
    config.max_root_discovery_k = 20

    # Force schema_ref mode for all responses so the benchmark
    # always exercises the schema -> code query path.
    config.passthrough_max_bytes = 0

    # Resolve bench data dir for the mock upstream.
    if bench_data_dir is None:
        bench_data_dir = str(Path(__file__).resolve().parent / "data")

    # Configure the mock upstream.
    upstream = UpstreamConfig(
        prefix="bench",
        transport="stdio",
        command=sys.executable,
        args=[_MOCK_UPSTREAM_SCRIPT],
        env={"BENCHMARK_DATA_DIR": bench_data_dir},
        inherit_parent_env=True,
        passthrough_allowed=False,
    )
    config.upstreams = [upstream]

    # Ensure directories exist.
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
    runtime: _MCPRuntime | None = None
    try:
        with backend.connection() as connection:
            apply_migrations(connection, _MIGRATIONS_DIR)

        # Bootstrap the server (connects to mock upstream,
        # discovers tools, builds mirrored tool registry).
        # Uses a temporary event loop because bootstrap_server is
        # async but its connections are scoped and fully cleaned up
        # before it returns.  The persistent client loop lives in
        # _MCPRuntime below.
        setup_loop = asyncio.new_event_loop()
        server = setup_loop.run_until_complete(
            bootstrap_server(config, db_pool=backend)
        )
        setup_loop.close()

        # Build the FastMCP app and connect an in-process client.
        app = server.build_fastmcp_app()

        runtime = _MCPRuntime(backend=backend)
        runtime.connect(app)
        yield runtime
    finally:
        if runtime is not None:
            runtime.close()
        else:
            backend.close()


def call_mirrored_tool(
    runtime: _MCPRuntime,
    *,
    dataset_name: str,
) -> dict[str, Any]:
    """Call a mirrored dataset tool through the gateway.

    The gateway captures the upstream response as an artifact,
    computes its schema, and returns a ``schema_ref`` response
    with ``artifact_id`` and ``schemas``.

    Returns:
        Dict with ``artifact_id``, ``schemas`` (or representative
        sample), ``response_mode``, and metadata.
    """
    tool_name = f"bench_get_{dataset_name}"
    result = runtime.call_tool(
        tool_name,
        {"_gateway_context": _GATEWAY_CONTEXT},
    )
    if _is_error_response(result):
        code = result.get("code", "UNKNOWN")
        message = result.get("message", "unknown error")
        msg = f"mirrored tool call failed: {code}: {message}"
        raise RuntimeError(msg)
    if "artifact_id" not in result:
        msg = f"mirrored tool response missing artifact_id: {result}"
        raise RuntimeError(msg)
    return result


def mcp_response_to_describe_format(
    mcp_result: dict[str, Any],
    runtime: _MCPRuntime,
) -> dict[str, Any]:
    """Convert an MCP mirrored-tool response to describe format.

    Transforms the gateway's ``schema_ref`` response into the
    dict shape that ``format_schema_for_prompt()`` expects:
    ``{"roots": [...], "schemas": [...]}``.

    When the response uses ``representative_sample`` mode (no
    inline schemas), falls back to loading persisted schemas
    directly from the gateway's SQLite database.

    Args:
        mcp_result: The dict returned by ``call_mirrored_tool``.
        runtime: The MCP runtime (used for DB fallback).

    Returns:
        A dict compatible with ``format_schema_for_prompt()``.
    """
    schemas = mcp_result.get("schemas", [])

    if not schemas:
        # Representative-sample response — fetch schemas from DB.
        artifact_id = mcp_result.get("artifact_id", "")
        schemas = runtime.fetch_schemas(artifact_id)
        if not schemas:
            msg = (
                f"no schemas found for artifact {artifact_id} "
                f"(neither inline nor in DB)"
            )
            raise RuntimeError(msg)

    # Build roots from schema entries.  Default root_path to "$"
    # when missing — safe for the benchmark since the gateway always
    # populates root_path, but avoids a KeyError if the schema
    # structure changes.
    roots: list[dict[str, Any]] = []
    for schema in schemas:
        root_entry: dict[str, Any] = {
            "root_path": schema.get("root_path", "$"),
        }
        # Carry over count info.  Inline MCP responses nest it
        # under "determinism"; DB-fetched schemas use "coverage".
        observed = None
        for section_key in ("determinism", "coverage"):
            section = schema.get(section_key, {})
            if isinstance(section, dict):
                val = section.get("observed_records")
                if val is not None:
                    observed = val
                    break
        if observed is not None:
            root_entry["count_estimate"] = observed
        root_shape = schema.get("root_shape")
        if root_shape is not None:
            root_entry["root_shape"] = root_shape
        roots.append(root_entry)

    return {"roots": roots, "schemas": schemas}


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
    runtime: _MCPRuntime,
    *,
    artifact_id: str,
    root_path: str,
    code: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute code against an artifact via the MCP artifact tool."""
    result = runtime.call_tool(
        "artifact",
        {
            "_gateway_context": _GATEWAY_CONTEXT,
            "action": "query",
            "query_kind": "code",
            "artifact_id": artifact_id,
            "root_path": root_path,
            "scope": "single",
            "code": code,
            "params": params or {},
        },
    )
    if _is_error_response(result):
        code_val = result.get("code", "UNKNOWN")
        message = result.get("message", "unknown error")
        msg = f"code execution failed: {code_val}: {message}"
        raise CodeExecutionError(msg)
    return result
