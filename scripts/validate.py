#!/usr/bin/env python3
"""Done-means-done runtime validation script.

Exercises the full artifact lifecycle against a fresh DATA_DIR + fresh DB
schema, exits non-zero on any invariant violation.

Requires a live Postgres instance.  Reads DSN from
``SIFT_MCP_TEST_POSTGRES_DSN`` (defaults to docker-compose test DB).

Usage::

    PYTHONPATH=src python scripts/validate.py
    SIFT_MCP_TEST_POSTGRES_DSN="postgresql://..." PYTHONPATH=src python scripts/validate.py
"""

from __future__ import annotations

import asyncio
import re
import shutil
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Imports from the gateway package
# ---------------------------------------------------------------------------

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.db.conn import create_pool
from sift_mcp.db.migrate import apply_migrations
from sift_mcp.jobs.hard_delete import run_hard_delete_batch
from sift_mcp.jobs.soft_delete import run_soft_delete_unreferenced
from sift_mcp.mcp import server as _server_module
from sift_mcp.mcp.server import GatewayServer
from sift_mcp.mcp.upstream import UpstreamInstance, UpstreamToolSchema

_DEFAULT_DSN = "postgresql://sift:sift@localhost:5432/sift_test"

# Safety: only allow DROP SCHEMA on databases whose name has "test" as a
# word-boundary segment (e.g. mcp_test, test_db, testing — but NOT contest,
# latest, or protest_prod).
# Override with SIFT_MCP_VALIDATE_DESTRUCTIVE=1 for non-standard names.
_TEST_DB_PATTERN = re.compile(r"(?:^|[_\-])test(?:[_\-]|$|ing)", re.IGNORECASE)


def _assert_test_database(dsn: str) -> None:
    """Abort if the DSN doesn't point to a test database."""
    import os
    if os.getenv("SIFT_MCP_VALIDATE_DESTRUCTIVE") == "1":
        return
    # Extract DB name: last path segment of the DSN
    match = re.search(r"/([^/?]+)(?:\?|$)", dsn.split("@")[-1])
    db_name = match.group(1) if match else ""
    if not _TEST_DB_PATTERN.search(db_name):
        print(
            f"ABORT: DSN database '{db_name}' does not look like a test database.\n"
            "The validation script runs DROP SCHEMA public CASCADE and will\n"
            "destroy all data.  Either:\n"
            "  - Use a DSN whose database name contains 'test' as a word "
            "segment (e.g. mcp_test, test_db), or\n"
            "  - Set SIFT_MCP_VALIDATE_DESTRUCTIVE=1 to override.",
            file=sys.stderr,
        )
        sys.exit(2)

# ---------------------------------------------------------------------------
# Upstream stub (same dispatch table pattern as e2e integration tests)
# ---------------------------------------------------------------------------

_SMALL_JSON = {
    "users": [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "viewer"},
        {"id": 3, "name": "Charlie", "role": "editor"},
    ],
    "total": 3,
}

_LARGE_JSON = {
    "events": [
        {"id": i, "type": "click", "ts": f"2025-01-{i:02d}T00:00:00Z"}
        for i in range(1, 51)
    ],
}

_OVERSIZE_JSON = {
    "records": [
        {"id": i, "value": f"oversize_item_{i}", "data": "x" * 20}
        for i in range(10)
    ],
    "oversize_marker": True,
}

_UPSTREAM_DISPATCH: dict[str, tuple[str, dict[str, Any]]] = {
    "get_users": (
        "Return user list",
        {
            "content": [{"type": "text", "text": "3 users found"}],
            "structuredContent": _SMALL_JSON,
            "isError": False,
            "meta": {},
        },
    ),
    "get_events": (
        "Return event stream",
        {
            "content": [],
            "structuredContent": _LARGE_JSON,
            "isError": False,
            "meta": {},
        },
    ),
    "failing_tool": (
        "Always fails",
        {
            "content": [{"type": "text", "text": "upstream connection refused"}],
            "structuredContent": None,
            "isError": True,
            "meta": {"exception_type": "ConnectionError"},
        },
    ),
    "get_oversize_data": (
        "Return data for oversize test",
        {
            "content": [],
            "structuredContent": _OVERSIZE_JSON,
            "isError": False,
            "meta": {},
        },
    ),
}


async def _stub_upstream(
    _instance: Any,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    entry = _UPSTREAM_DISPATCH.get(tool_name)
    if entry is not None:
        return entry[1]
    return {
        "content": [{"type": "text", "text": f"unknown tool {tool_name}"}],
        "structuredContent": arguments,
        "isError": False,
        "meta": {},
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _migrations_dir() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "src"
        / "sift_mcp"
        / "db"
        / "migrations"
    )


def _build_upstream() -> UpstreamInstance:
    tools = [
        UpstreamToolSchema(
            name=name,
            description=desc_and_resp[0],
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": True,
            },
            schema_hash=f"schema_{name}",
        )
        for name, desc_and_resp in _UPSTREAM_DISPATCH.items()
    ]
    return UpstreamInstance(
        config=UpstreamConfig(prefix="test", transport="stdio", command="/bin/echo"),
        instance_id="upstream_validate",
        tools=tools,
    )


def _call_mirrored(
    server: GatewayServer,
    tool_qualified_name: str,
    session_id: str,
    extra_args: dict[str, Any] | None = None,
    *,
    cache_mode: str = "fresh",
) -> dict[str, Any]:
    mirrored = server.mirrored_tools[tool_qualified_name]
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id, "cache_mode": cache_mode},
    }
    if extra_args:
        args.update(extra_args)
    return asyncio.run(server.handle_mirrored_tool(mirrored, args))


def _search(
    server: GatewayServer,
    session_id: str,
    *,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id},
        "filters": filters or {},
    }
    if limit is not None:
        args["limit"] = limit
    if cursor is not None:
        args["cursor"] = cursor
    return asyncio.run(server.handle_artifact_search(args))


def _get(
    server: GatewayServer,
    session_id: str,
    artifact_id: str,
    *,
    target: str = "envelope",
) -> dict[str, Any]:
    return asyncio.run(
        server.handle_artifact_get(
            {
                "_gateway_context": {"session_id": session_id},
                "artifact_id": artifact_id,
                "target": target,
            }
        )
    )


def _describe(
    server: GatewayServer,
    session_id: str,
    artifact_id: str,
) -> dict[str, Any]:
    return asyncio.run(
        server.handle_artifact_describe(
            {
                "_gateway_context": {"session_id": session_id},
                "artifact_id": artifact_id,
            }
        )
    )


def _select(
    server: GatewayServer,
    session_id: str,
    artifact_id: str,
    root_path: str,
    *,
    select_paths: list[str] | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id},
        "artifact_id": artifact_id,
        "root_path": root_path,
    }
    if select_paths is not None:
        args["select_paths"] = select_paths
    if limit is not None:
        args["limit"] = limit
    if cursor is not None:
        args["cursor"] = cursor
    return asyncio.run(server.handle_artifact_select(args))


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------


def _setup(dsn: str, data_dir: Path):
    """Create config, pool, apply migrations, build server."""
    config = GatewayConfig(
        data_dir=data_dir,
        postgres_dsn=dsn,
        mapping_mode="sync",
        max_full_map_bytes=2000,
        inline_envelope_max_json_bytes=100_000,
        inline_envelope_max_total_bytes=200_000,
    )
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)

    # Fresh schema — safety-checked in main() before we get here
    with pool.connection() as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.commit()

    with pool.connection() as conn:
        apply_migrations(conn, _migrations_dir())

    upstream = _build_upstream()
    server = GatewayServer(config=config, db_pool=pool, upstreams=[upstream])

    # Replace call_upstream_tool at module level (monkeypatch equivalent)
    _server_module.call_upstream_tool = _stub_upstream  # type: ignore[assignment]

    return server, config, pool


def _setup_oversize(dsn: str, data_dir: Path):
    """Like _setup but with minimal_for_large JSONB mode + low threshold."""
    oversize_dir = data_dir / "oversize"
    config = GatewayConfig(
        data_dir=oversize_dir,
        postgres_dsn=dsn,
        mapping_mode="sync",
        max_full_map_bytes=2000,
        inline_envelope_max_json_bytes=100_000,
        inline_envelope_max_total_bytes=200_000,
        envelope_jsonb_mode="minimal_for_large",
        envelope_jsonb_minimize_threshold_bytes=100,
    )
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)

    # Fresh schema (separate call since main _setup already did this once)
    with pool.connection() as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.commit()

    with pool.connection() as conn:
        apply_migrations(conn, _migrations_dir())

    upstream = _build_upstream()
    server = GatewayServer(config=config, db_pool=pool, upstreams=[upstream])
    _server_module.call_upstream_tool = _stub_upstream  # type: ignore[assignment]

    return server, config, pool


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_full_pipeline(server: GatewayServer) -> None:
    """Small JSON → full mapping → search → get → describe → select."""
    sid = f"sess_{uuid.uuid4().hex}"

    # Mirrored call
    resp = _call_mirrored(server, "test.get_users", sid)
    assert resp["type"] == "gateway_tool_result", f"unexpected type: {resp.get('type')}"
    aid = resp["artifact_id"]
    assert aid.startswith("art_"), f"bad artifact_id prefix: {aid}"

    # Search finds it
    search = _search(server, sid)
    ids = [item["artifact_id"] for item in search["items"]]
    assert aid in ids, "artifact not found in search"

    # Get envelope
    envelope = _get(server, sid, aid, target="envelope")
    assert envelope["artifact_id"] == aid

    # Describe — full mapping
    desc = _describe(server, sid, aid)
    mapping = desc["mapping"]
    assert mapping["map_kind"] == "full", f"expected full, got {mapping['map_kind']}"
    assert mapping["map_status"] == "ready", f"expected ready, got {mapping['map_status']}"
    roots = desc.get("roots", [])
    root_paths = [r["root_path"] for r in roots]
    assert "$.users" in root_paths, f"$.users not in roots: {root_paths}"
    users_root = next(r for r in roots if r["root_path"] == "$.users")
    assert users_root["count_estimate"] == 3, f"count_estimate={users_root['count_estimate']}"

    # Select
    sel = _select(server, sid, aid, "$.users", select_paths=["name", "role"])
    items = sel.get("items", [])
    assert len(items) == 3, f"expected 3 items, got {len(items)}"
    names = {item["projection"]["$.name"] for item in items}
    assert names == {"Alice", "Bob", "Charlie"}, f"names={names}"


def check_cache_fresh(server: GatewayServer) -> None:
    """cache_mode='fresh' always creates a new artifact."""
    sid = f"sess_{uuid.uuid4().hex}"
    unique = f"fresh_{uuid.uuid4().hex}"

    resp1 = _call_mirrored(server, "test.get_users", sid, extra_args={"k": unique})
    resp2 = _call_mirrored(server, "test.get_users", sid, extra_args={"k": unique})
    assert resp1["artifact_id"] != resp2["artifact_id"], "fresh mode reused artifact"


def check_cache_reuse(server: GatewayServer) -> None:
    """cache_mode='allow' reuses by request_key."""
    sid = f"sess_{uuid.uuid4().hex}"
    unique = f"reuse_{uuid.uuid4().hex}"

    resp1 = _call_mirrored(
        server, "test.get_users", sid,
        extra_args={"k": unique}, cache_mode="allow",
    )
    aid1 = resp1["artifact_id"]

    resp2 = _call_mirrored(
        server, "test.get_users", sid,
        extra_args={"k": unique}, cache_mode="allow",
    )
    assert resp2["meta"]["cache"]["reused"] is True, "cache not reused"
    assert resp2["artifact_id"] == aid1, "reused different artifact_id"


def check_error_envelope(server: GatewayServer) -> None:
    """Error upstream → artifact persisted with error status."""
    sid = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.failing_tool", sid)
    assert resp["type"] == "gateway_tool_result", f"unexpected type: {resp.get('type')}"
    aid = resp["artifact_id"]

    search = _search(server, sid, filters={"status": "error"})
    assert any(
        item["artifact_id"] == aid for item in search["items"]
    ), "error artifact not found"

    envelope = _get(server, sid, aid, target="envelope")
    assert envelope["artifact_id"] == aid


def check_partial_mapping(server: GatewayServer) -> None:
    """Large JSON → partial mapping → sampled-only select + cursor."""
    sid = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.get_events", sid)
    assert resp["type"] == "gateway_tool_result"
    aid = resp["artifact_id"]

    desc = _describe(server, sid, aid)
    mapping = desc["mapping"]
    assert mapping["map_kind"] == "partial", f"expected partial, got {mapping['map_kind']}"
    assert mapping["map_status"] == "ready"

    roots = desc.get("roots", [])
    assert len(roots) >= 1, "no roots in partial mapping"

    # Select should indicate sampled_only
    events_root = next((r for r in roots if "events" in r["root_path"]), roots[0])
    sel = _select(
        server, sid, aid, events_root["root_path"],
        select_paths=["id", "type"], limit=3,
    )
    assert sel.get("sampled_only") is True, "expected sampled_only=True"

    # Cursor continuation
    if sel.get("truncated"):
        cursor = sel.get("cursor")
        assert cursor is not None, "truncated but no cursor"
        page2 = _select(
            server, sid, aid, events_root["root_path"],
            select_paths=["id", "type"], limit=3, cursor=cursor,
        )
        assert len(page2.get("items", [])) > 0, "cursor page returned no items"


def check_oversize_offload(dsn: str, data_dir: Path) -> None:
    """Oversize JSON → minimal JSONB + canonical-bytes reconstruction."""
    server, _config, pool = _setup_oversize(dsn, data_dir)
    try:
        sid = f"sess_{uuid.uuid4().hex}"

        resp = _call_mirrored(server, "test.get_oversize_data", sid)
        assert resp["type"] == "gateway_tool_result"
        aid = resp["artifact_id"]

        # Verify JSONB is minimal (content_summary, not full content)
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT pb.envelope FROM artifacts a "
                "JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id "
                "AND pb.payload_hash_full = a.payload_hash_full "
                "WHERE a.artifact_id = %s",
                (aid,),
            ).fetchone()
            assert row is not None, "payload_blobs row not found"
            envelope_jsonb = row[0]
            assert "content_summary" in envelope_jsonb, (
                "expected minimal JSONB with content_summary"
            )
            assert "content" not in envelope_jsonb, (
                "full content should not be in minimal JSONB"
            )

        # artifact.get reconstructs from canonical bytes
        get_resp = _get(server, sid, aid)
        assert get_resp["artifact_id"] == aid

        # artifact.select still works via canonical bytes reconstruction
        sel = _select(
            server, sid, aid, "$.records", select_paths=["id", "value"],
        )
        items = sel.get("items", [])
        assert len(items) == 10, f"expected 10 items, got {len(items)}"
    finally:
        pool.close()


def check_delete_lifecycle(server: GatewayServer, pool) -> None:
    """Soft delete → hard delete → verify artifact + payload removed."""
    sid = f"sess_{uuid.uuid4().hex}"

    # Use get_oversize_data — no other check on the main server creates
    # artifacts with this payload, so its payload_hash_full is unique and
    # hard delete will orphan the payload_blobs row.
    resp = _call_mirrored(server, "test.get_oversize_data", sid)
    aid = resp["artifact_id"]

    # Capture payload_hash so we can verify payload cleanup
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT payload_hash_full FROM artifacts "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, aid),
        ).fetchone()
        assert row is not None
        payload_hash = row[0]

    # Verify accessible
    get_resp = _get(server, sid, aid)
    assert get_resp["artifact_id"] == aid

    # Push last_referenced_at into the past
    with pool.connection() as conn:
        conn.execute(
            "UPDATE artifacts SET last_referenced_at = '2020-06-15T12:00:00Z' "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, aid),
        )
        conn.commit()

    # Soft delete
    with pool.connection() as conn:
        result = run_soft_delete_unreferenced(
            conn, threshold_timestamp="2020-06-15T13:00:00Z",
        )
        assert aid in result.artifact_ids, "artifact not soft-deleted"

    # Verify GONE
    get_resp = _get(server, sid, aid)
    assert get_resp.get("code") == "GONE", f"expected GONE, got {get_resp.get('code')}"

    # Hard delete with FS blob removal enabled
    with pool.connection() as conn:
        hard_result = run_hard_delete_batch(
            conn, grace_period_timestamp="2099-01-01T00:00:00Z",
            remove_fs_blobs=True,
        )
        assert hard_result.artifacts_deleted >= 1, "no artifacts hard-deleted"
        assert hard_result.payloads_deleted >= 1, "payload not cleaned up"

    # Verify artifact row removed
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM artifacts WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, aid),
        ).fetchone()
        assert row is None, "artifact still in DB after hard delete"

    # Verify payload_blobs row removed
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM payload_blobs WHERE workspace_id = %s AND payload_hash_full = %s",
            (WORKSPACE_ID, payload_hash),
        ).fetchone()
        assert row is None, "payload_blobs still in DB after hard delete"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_CHECKS = [
    ("full_pipeline", check_full_pipeline),
    ("cache_fresh", check_cache_fresh),
    ("cache_reuse", check_cache_reuse),
    ("error_envelope", check_error_envelope),
    ("partial_mapping", check_partial_mapping),
    # check_delete_lifecycle and check_oversize_offload handled separately
]


def main() -> int:
    import os

    dsn = os.getenv("SIFT_MCP_TEST_POSTGRES_DSN", _DEFAULT_DSN)
    _assert_test_database(dsn)
    data_dir = Path(tempfile.mkdtemp(prefix="mcp_validate_"))

    print(f"validate: dsn={dsn}")
    print(f"validate: data_dir={data_dir}")

    try:
        server, config, pool = _setup(dsn, data_dir)
    except Exception:
        print("SETUP FAILED")
        traceback.print_exc()
        shutil.rmtree(data_dir, ignore_errors=True)
        return 1

    passed = 0
    failed = 0

    for name, fn in _CHECKS:
        try:
            fn(server)
            print(f"  PASS  {name}")
            passed += 1
        except Exception:
            print(f"  FAIL  {name}")
            traceback.print_exc()
            failed += 1

    # delete lifecycle needs pool arg
    try:
        check_delete_lifecycle(server, pool)
        print("  PASS  delete_lifecycle")
        passed += 1
    except Exception:
        print("  FAIL  delete_lifecycle")
        traceback.print_exc()
        failed += 1

    pool.close()

    # oversize offload uses its own server with minimal_for_large config
    try:
        check_oversize_offload(dsn, data_dir)
        print("  PASS  oversize_offload")
        passed += 1
    except Exception:
        print("  FAIL  oversize_offload")
        traceback.print_exc()
        failed += 1

    shutil.rmtree(data_dir, ignore_errors=True)

    total = passed + failed
    print(f"\nvalidate: {passed}/{total} checks passed")
    if failed:
        print(f"validate: {failed} FAILED")
        return 1
    print("validate: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
