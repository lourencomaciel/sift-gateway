"""End-to-end integration tests for the full MCP gateway artifact lifecycle.

Requires a live Postgres instance.  Set ``SIDEPOUCH_MCP_TEST_POSTGRES_DSN`` to
enable these tests; they are auto-skipped when the env var is absent.

The upstream MCP server is stubbed at the ``call_upstream_tool`` function level
(the same pattern used by ``test_postgres_runtime.py``).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any
import uuid

import pytest

from sidepouch_mcp.config.settings import GatewayConfig, UpstreamConfig
from sidepouch_mcp.constants import WORKSPACE_ID
from sidepouch_mcp.db.conn import create_pool
from sidepouch_mcp.db.migrate import apply_migrations
from sidepouch_mcp.mcp.server import GatewayServer
from sidepouch_mcp.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)

_POSTGRES_DSN_ENV = "SIDEPOUCH_MCP_TEST_POSTGRES_DSN"


# ---------------------------------------------------------------------------
# Upstream stub — routes by tool_name
# ---------------------------------------------------------------------------

_SMALL_JSON = {
    "users": [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "viewer"},
        {"id": 3, "name": "Charlie", "role": "editor"},
    ],
    "total": 3,
}

_MANY_USERS = {
    "users": [
        {"id": i, "name": f"user_{i}", "role": "member"} for i in range(1, 21)
    ],
}

_LARGE_JSON = {
    "events": [
        {"id": i, "type": "click", "ts": f"2025-01-{i:02d}T00:00:00Z"}
        for i in range(1, 51)
    ],
}

_BINARY_HASH = "binhash_e2e_" + "a" * 52
_BINARY_BLOB_ID = "bin_" + _BINARY_HASH[:32]


# Upstream stub dispatch table — single source of truth for tool names,
# descriptions, and responses.  _build_upstream() derives UpstreamToolSchema
# entries from this dict, so adding a tool here automatically registers it.

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
    "get_many_users": (
        "Return many users",
        {
            "content": [],
            "structuredContent": _MANY_USERS,
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
    "get_report": (
        "Return text report",
        {
            "content": [
                {
                    "type": "text",
                    "text": "Monthly report summary: all systems operational.",
                }
            ],
            "structuredContent": None,
            "isError": False,
            "meta": {},
        },
    ),
    "failing_tool": (
        "Always fails",
        {
            "content": [
                {"type": "text", "text": "upstream connection refused"}
            ],
            "structuredContent": None,
            "isError": True,
            "meta": {"exception_type": "ConnectionError"},
        },
    ),
    "get_binary": (
        "Return binary payload",
        {
            "content": [
                {
                    "type": "binary_ref",
                    "blob_id": _BINARY_BLOB_ID,
                    "binary_hash": _BINARY_HASH,
                    "mime": "application/octet-stream",
                    "byte_count": 4096,
                }
            ],
            "structuredContent": None,
            "isError": False,
            "meta": {},
        },
    ),
    "get_oversize_data": (
        "Return data for oversize test",
        {
            "content": [],
            "structuredContent": {
                "records": [
                    {"id": i, "value": f"oversize_item_{i}", "data": "x" * 20}
                    for i in range(10)
                ],
                "oversize_marker": True,
            },
            "isError": False,
            "meta": {},
        },
    ),
}


async def _stub_upstream(
    _instance: Any,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None = None,  # noqa: ARG001
) -> dict[str, Any]:
    """Fake upstream that returns controlled payloads by tool name."""
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
        Path(__file__).resolve().parents[2]
        / "src"
        / "sidepouch_mcp"
        / "db"
        / "migrations"
    )


def _e2e_config(tmp_path: Path) -> GatewayConfig:
    dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {_POSTGRES_DSN_ENV} to run integration tests")
    return GatewayConfig(
        data_dir=tmp_path,
        postgres_dsn=dsn,
        mapping_mode="sync",
        max_full_map_bytes=2000,
        passthrough_max_bytes=0,
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
        config=UpstreamConfig(
            prefix="test", transport="stdio", command="/bin/echo"
        ),
        instance_id="upstream_e2e_test",
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
        "_gateway_context": {
            "session_id": session_id,
            "cache_mode": cache_mode,
        },
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
    jsonpath: str | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id},
        "artifact_id": artifact_id,
        "target": target,
    }
    if jsonpath is not None:
        args["jsonpath"] = jsonpath
    return asyncio.run(server.handle_artifact_get(args))


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
    where: Any = None,
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
    if where is not None:
        args["where"] = where
    if limit is not None:
        args["limit"] = limit
    if cursor is not None:
        args["cursor"] = cursor
    return asyncio.run(server.handle_artifact_select(args))


def _chain_pages(
    server: GatewayServer,
    session_id: str,
    parent_artifact_id: str,
    *,
    cursor: str | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id},
        "parent_artifact_id": parent_artifact_id,
    }
    if cursor is not None:
        args["cursor"] = cursor
    return asyncio.run(server.handle_artifact_chain_pages(args))


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def e2e_env(tmp_path, monkeypatch):
    """Provision config, DB pool, migrations, server, and upstream stub."""
    config = _e2e_config(tmp_path)

    # Ensure derived dirs exist (secrets_path needs state_dir)
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream()
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sidepouch_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Test 1: Full pipeline — small JSON, full mapping
# ---------------------------------------------------------------------------


def test_e2e_small_json_full_pipeline(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # 1. Mirrored call
    response = _call_mirrored(server, "test.get_users", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]
    assert artifact_id.startswith("art_")

    # 2. Search finds it
    search = _search(server, session_id)
    ids = [item["artifact_id"] for item in search["items"]]
    assert artifact_id in ids

    # 3. Get envelope
    envelope = _get(server, session_id, artifact_id, target="envelope")
    assert envelope["artifact_id"] == artifact_id
    assert envelope["target"] == "envelope"

    # 4. Describe — full mapping should have run (sync mode)
    desc = _describe(server, session_id, artifact_id)
    mapping = desc["mapping"]
    assert mapping["map_kind"] == "full"
    assert mapping["map_status"] == "ready"
    roots = desc.get("roots", [])
    assert len(roots) >= 1
    root_paths = [r["root_path"] for r in roots]
    assert "$.users" in root_paths

    users_root = next(r for r in roots if r["root_path"] == "$.users")
    assert users_root["count_estimate"] == 3

    # 5. Select — project name and role from users
    sel = _select(
        server,
        session_id,
        artifact_id,
        "$.users",
        select_paths=["name", "role"],
    )
    items = sel.get("items", [])
    assert len(items) == 3
    names = {item["projection"]["$.name"] for item in items}
    assert names == {"Alice", "Bob", "Charlie"}
    roles = {item["projection"]["$.role"] for item in items}
    assert roles == {"admin", "viewer", "editor"}


# ---------------------------------------------------------------------------
# Test 2: Large JSON — partial mapping
# ---------------------------------------------------------------------------


def test_e2e_large_json_partial_mapping(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_events", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    desc = _describe(server, session_id, artifact_id)
    mapping = desc["mapping"]
    assert mapping["map_kind"] == "partial"
    assert mapping["map_status"] == "ready"
    roots = desc.get("roots", [])
    assert len(roots) >= 1


# ---------------------------------------------------------------------------
# Test 3: Text content — no JSON to map
# ---------------------------------------------------------------------------


def test_e2e_text_content_artifact(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_report", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    # Search finds it
    search = _search(server, session_id)
    assert any(item["artifact_id"] == artifact_id for item in search["items"])

    # Get envelope — should have text content
    envelope = _get(server, session_id, artifact_id, target="envelope")
    assert envelope["artifact_id"] == artifact_id

    # Describe — mapping should fail (no JSON part)
    desc = _describe(server, session_id, artifact_id)
    assert desc["mapping"]["map_status"] == "failed"


# ---------------------------------------------------------------------------
# Test 4: Error response
# ---------------------------------------------------------------------------


def test_e2e_error_response_artifact(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.failing_tool", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    # Search with status filter
    search = _search(server, session_id, filters={"status": "error"})
    assert any(item["artifact_id"] == artifact_id for item in search["items"])

    # Get envelope — should have error info
    envelope = _get(server, session_id, artifact_id, target="envelope")
    assert envelope["artifact_id"] == artifact_id


# ---------------------------------------------------------------------------
# Test 5: Binary payload
# ---------------------------------------------------------------------------


def test_e2e_binary_payload_artifact(e2e_env):
    server, _config, pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Pre-insert binary_blobs row to satisfy FK constraint
    with pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO binary_blobs (workspace_id, binary_hash, blob_id, byte_count, mime, fs_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                WORKSPACE_ID,
                _BINARY_HASH,
                _BINARY_BLOB_ID,
                4096,
                "application/octet-stream",
                "/tmp/fake",
            ),
        )
        conn.commit()

    response = _call_mirrored(server, "test.get_binary", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    # Verify payload_binary_refs row exists
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT binary_hash FROM payload_binary_refs
            WHERE workspace_id = %s AND binary_hash = %s
            """,
            (WORKSPACE_ID, _BINARY_HASH),
        ).fetchone()
        assert row is not None

    # Get envelope
    envelope = _get(server, session_id, artifact_id, target="envelope")
    assert envelope["artifact_id"] == artifact_id


# ---------------------------------------------------------------------------
# Test 6: Search cursor pagination
# ---------------------------------------------------------------------------


def test_e2e_search_cursor_pagination(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create 5 artifacts with different args (unique request keys)
    created_ids = []
    for i in range(5):
        resp = _call_mirrored(
            server,
            "test.get_users",
            session_id,
            extra_args={"_unique": f"run_{i}"},
        )
        assert resp["type"] == "gateway_tool_result"
        created_ids.append(resp["artifact_id"])

    # Paginate with limit=2
    collected: list[str] = []
    cursor = None
    pages = 0
    while True:
        result = _search(server, session_id, limit=2, cursor=cursor)
        collected.extend(item["artifact_id"] for item in result["items"])
        pages += 1
        if not result.get("truncated", False):
            break
        cursor = result.get("cursor")
        assert cursor is not None, "truncated=True but no cursor returned"
        assert pages <= 5, "too many pages — possible infinite loop"

    assert len(collected) == 5
    assert set(collected) == set(created_ids)


# ---------------------------------------------------------------------------
# Test 7: Select cursor pagination
# ---------------------------------------------------------------------------


def test_e2e_select_cursor_pagination(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create artifact with 20 users
    response = _call_mirrored(server, "test.get_many_users", session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    # Verify mapping ran
    desc = _describe(server, session_id, artifact_id)
    assert desc["mapping"]["map_status"] == "ready"

    # Find the users root
    roots = desc.get("roots", [])
    user_root = next((r for r in roots if r["root_path"] == "$.users"), None)
    assert user_root is not None

    # Paginate with limit=5
    collected: list[dict] = []
    cursor = None
    pages = 0
    while True:
        result = _select(
            server,
            session_id,
            artifact_id,
            "$.users",
            select_paths=["id", "name"],
            limit=5,
            cursor=cursor,
        )
        collected.extend(result.get("items", []))
        pages += 1
        if not result.get("truncated", False):
            break
        cursor = result.get("cursor")
        assert cursor is not None
        assert pages <= 10, "too many pages"

    # Should have gotten all 20 users
    assert len(collected) == 20
    # Check deterministic ascending order
    ids = [item["projection"]["$.id"] for item in collected]
    assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# Test 8: Chain pages
# ---------------------------------------------------------------------------


def test_e2e_chain_pages(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create parent artifact
    parent_resp = _call_mirrored(server, "test.get_users", session_id)
    assert parent_resp["type"] == "gateway_tool_result"
    parent_id = parent_resp["artifact_id"]

    # Create 3 child artifacts with chain_seq
    child_ids = []
    for seq in range(3):
        child_resp = _call_mirrored(
            server,
            "test.get_users",
            session_id,
            extra_args={
                "_gateway_parent_artifact_id": parent_id,
                "_gateway_chain_seq": seq,
                "_unique": f"child_{seq}",
            },
        )
        assert child_resp["type"] == "gateway_tool_result"
        child_ids.append(child_resp["artifact_id"])

    # Query chain pages
    pages = _chain_pages(server, session_id, parent_id)
    page_ids = [item["artifact_id"] for item in pages.get("items", [])]
    assert len(page_ids) == 3
    assert page_ids == child_ids  # same order as chain_seq


# ---------------------------------------------------------------------------
# Test 9: Cache reuse
# ---------------------------------------------------------------------------


def test_e2e_cache_reuse(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Use a unique key so prior test runs don't interfere
    unique = f"cache_test_{uuid.uuid4().hex}"

    # First call — fresh creation (allow mode creates the artifact)
    resp1 = _call_mirrored(
        server,
        "test.get_users",
        session_id,
        extra_args={"message": unique},
        cache_mode="allow",
    )
    assert resp1["type"] == "gateway_tool_result"
    artifact_id_1 = resp1["artifact_id"]

    # Second call — identical args, should reuse
    resp2 = _call_mirrored(
        server,
        "test.get_users",
        session_id,
        extra_args={"message": unique},
        cache_mode="allow",
    )
    assert resp2["type"] == "gateway_tool_result"
    assert resp2["meta"]["cache"]["reused"] is True
    assert resp2["artifact_id"] == artifact_id_1


# ---------------------------------------------------------------------------
# Test 10: Session isolation
# ---------------------------------------------------------------------------


def test_e2e_session_isolation(e2e_env):
    server, _config, _pool = e2e_env
    session_a = f"sess_{uuid.uuid4().hex}"
    session_b = f"sess_{uuid.uuid4().hex}"

    # Create artifact in session_a
    resp = _call_mirrored(server, "test.get_users", session_a)
    assert resp["type"] == "gateway_tool_result"
    artifact_id = resp["artifact_id"]

    # Session A can find it
    search_a = _search(server, session_a)
    assert any(item["artifact_id"] == artifact_id for item in search_a["items"])

    # Session B cannot find it via search
    search_b = _search(server, session_b)
    assert not any(
        item["artifact_id"] == artifact_id for item in search_b["items"]
    )


# ---------------------------------------------------------------------------
# Additional helpers
# ---------------------------------------------------------------------------


def _find(
    server: GatewayServer,
    session_id: str,
    artifact_id: str,
    *,
    root_path: str | None = None,
    where: Any = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "_gateway_context": {"session_id": session_id},
        "artifact_id": artifact_id,
    }
    if root_path is not None:
        args["root_path"] = root_path
    if where is not None:
        args["where"] = where
    if limit is not None:
        args["limit"] = limit
    if cursor is not None:
        args["cursor"] = cursor
    return asyncio.run(server.handle_artifact_find(args))


def _status(server: GatewayServer) -> dict[str, Any]:
    return asyncio.run(server.handle_status({}))


# ---------------------------------------------------------------------------
# Additional fixture: oversize envelope (minimal JSONB)
# ---------------------------------------------------------------------------


@pytest.fixture()
def e2e_env_oversize(tmp_path, monkeypatch):
    """Environment with minimal_for_large JSONB mode and low threshold."""
    dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {_POSTGRES_DSN_ENV} to run integration tests")
    config = GatewayConfig(
        data_dir=tmp_path,
        postgres_dsn=dsn,
        mapping_mode="sync",
        max_full_map_bytes=2000,
        passthrough_max_bytes=0,
        envelope_jsonb_mode="minimal_for_large",
        envelope_jsonb_minimize_threshold_bytes=100,
    )
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)
    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())
        upstream = _build_upstream()
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sidepouch_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Test 11: Soft delete → hard delete lifecycle
# ---------------------------------------------------------------------------


def test_e2e_soft_delete_hard_delete_lifecycle(e2e_env):
    from sidepouch_mcp.jobs.hard_delete import run_hard_delete_batch
    from sidepouch_mcp.jobs.soft_delete import (
        run_soft_delete_unreferenced,
    )

    server, _config, pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create artifact
    resp = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = resp["artifact_id"]

    # Verify accessible
    get_resp = _get(server, session_id, artifact_id)
    assert get_resp["artifact_id"] == artifact_id

    # Set last_referenced_at to an isolated past timestamp so soft delete
    # picks up only this artifact (not others sharing the DB).
    with pool.connection() as conn:
        conn.execute(
            "UPDATE artifacts SET last_referenced_at = '2020-06-15T12:00:00Z' "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, artifact_id),
        )
        conn.commit()

    # Soft delete with a tight threshold: only artifacts referenced before
    # 2020-06-15T13:00:00Z are eligible — just our test artifact.
    with pool.connection() as conn:
        result = run_soft_delete_unreferenced(
            conn,
            threshold_timestamp="2020-06-15T13:00:00Z",
        )
        assert artifact_id in result.artifact_ids

    # Verify GONE on get
    get_resp = _get(server, session_id, artifact_id)
    assert get_resp.get("code") == "GONE"

    # Hard delete with a tight grace period: soft-deleted artifacts whose
    # deleted_at is before "now" (we use a timestamp far enough in the
    # future that our just-soft-deleted artifact qualifies, but we scope
    # by checking the specific artifact_id in the result).
    with pool.connection() as conn:
        hard_result = run_hard_delete_batch(
            conn,
            grace_period_timestamp="2099-01-01T00:00:00Z",
            remove_fs_blobs=False,
        )
        assert hard_result.artifacts_deleted >= 1

    # Verify fully removed from DB
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM artifacts WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, artifact_id),
        ).fetchone()
        assert row is None


# ---------------------------------------------------------------------------
# Test 12: Select with WHERE filtering
# ---------------------------------------------------------------------------


def test_e2e_select_where_filtering(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = resp["artifact_id"]

    # Select only admins
    sel = _select(
        server,
        session_id,
        artifact_id,
        "$.users",
        select_paths=["name", "role"],
        where="role = 'admin'",
    )
    items = sel.get("items", [])
    assert len(items) == 1
    assert items[0]["projection"]["$.name"] == "Alice"
    assert items[0]["projection"]["$.role"] == "admin"

    # Select viewers and editors
    sel2 = _select(
        server,
        session_id,
        artifact_id,
        "$.users",
        select_paths=["name"],
        where="role != 'admin'",
    )
    items2 = sel2.get("items", [])
    assert len(items2) == 2
    names = {item["projection"]["$.name"] for item in items2}
    assert names == {"Bob", "Charlie"}


# ---------------------------------------------------------------------------
# Test 13: Artifact.find (sampled records)
# ---------------------------------------------------------------------------


def test_e2e_artifact_find(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create artifact with partial mapping (events > max_full_map_bytes)
    resp = _call_mirrored(server, "test.get_events", session_id)
    artifact_id = resp["artifact_id"]

    desc = _describe(server, session_id, artifact_id)
    assert desc["mapping"]["map_kind"] == "partial"

    # Find samples
    find_resp = _find(server, session_id, artifact_id)
    items = find_resp.get("items", [])
    assert len(items) > 0
    assert find_resp.get("sampled_only") is True

    # Each item should have root_path and sample_index
    for item in items:
        assert "root_path" in item
        assert "sample_index" in item
        assert "record" in item


# ---------------------------------------------------------------------------
# Test 14: Artifact.get with JSONPath filtering
# ---------------------------------------------------------------------------


def test_e2e_get_with_jsonpath(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = resp["artifact_id"]

    # Get envelope status via jsonpath
    result = _get(server, session_id, artifact_id, jsonpath="$.status")
    items = result.get("items", [])
    assert len(items) == 1
    assert items[0] == "ok"

    # Get content array length
    result2 = _get(server, session_id, artifact_id, jsonpath="$.content")
    items2 = result2.get("items", [])
    assert len(items2) == 1
    assert isinstance(items2[0], list)
    assert len(items2[0]) >= 1  # at least one content part


# ---------------------------------------------------------------------------
# Test 15: Select on partial mapping (sampled records)
# ---------------------------------------------------------------------------


def test_e2e_select_partial_mapping_samples(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    # Create artifact with partial mapping
    resp = _call_mirrored(server, "test.get_events", session_id)
    artifact_id = resp["artifact_id"]

    desc = _describe(server, session_id, artifact_id)
    assert desc["mapping"]["map_kind"] == "partial"
    roots = desc.get("roots", [])
    assert len(roots) >= 1
    root_path = roots[0]["root_path"]

    # Select from partial mapping
    sel = _select(
        server,
        session_id,
        artifact_id,
        root_path,
        select_paths=["id", "type"],
    )
    assert sel.get("sampled_only") is True
    items = sel.get("items", [])
    assert len(items) > 0

    # Check sample_indices_used is sorted ascending
    sample_indices = sel.get("sample_indices_used", [])
    assert sample_indices == sorted(sample_indices)

    # Each item should have _locator with sample_index
    for item in items:
        locator = item.get("_locator", {})
        assert "sample_index" in locator


# ---------------------------------------------------------------------------
# Test 16: Oversize envelope — reconstruction from canonical bytes
# ---------------------------------------------------------------------------


def test_e2e_oversize_envelope_reconstruction(e2e_env_oversize):
    server, _config, pool = e2e_env_oversize
    session_id = f"sess_{uuid.uuid4().hex}"

    # Use a unique tool so the payload_blob hash hasn't been seen before
    # (other fixtures use full JSONB mode, and ON CONFLICT DO NOTHING skips re-inserts)
    resp = _call_mirrored(server, "test.get_oversize_data", session_id)
    artifact_id = resp["artifact_id"]

    # Verify JSONB is minimal (content_summary instead of content)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT pb.envelope FROM artifacts a "
            "JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id "
            "AND pb.payload_hash_full = a.payload_hash_full "
            "WHERE a.artifact_id = %s",
            (artifact_id,),
        ).fetchone()
        assert row is not None
        envelope_jsonb = row[0]
        assert isinstance(envelope_jsonb, dict)
        assert "content_summary" in envelope_jsonb
        assert "content" not in envelope_jsonb

    # artifact.get should still work — reconstructs from canonical bytes
    get_resp = _get(server, session_id, artifact_id)
    assert get_resp["artifact_id"] == artifact_id
    items = get_resp.get("items", [])
    assert len(items) >= 1

    # artifact.select should still work — reconstructs from canonical bytes
    sel = _select(
        server,
        session_id,
        artifact_id,
        "$.records",
        select_paths=["id", "value"],
    )
    items = sel.get("items", [])
    assert len(items) == 10
    ids = {item["projection"]["$.id"] for item in items}
    assert ids == set(range(10))


# ---------------------------------------------------------------------------
# Test 17: Migration idempotency
# ---------------------------------------------------------------------------


def test_e2e_migration_idempotency(e2e_env):
    _server, _config, pool = e2e_env

    # Migrations already applied by fixture — running again should be a no-op
    with pool.connection() as conn:
        newly_applied = apply_migrations(conn, _migrations_dir())
        assert newly_applied == []


# ---------------------------------------------------------------------------
# Test 18: Generation-safe mapping race
# ---------------------------------------------------------------------------


def test_e2e_generation_safe_mapping_race(e2e_env):
    from sidepouch_mcp.mapping.runner import MappingResult
    from sidepouch_mcp.mapping.worker import (
        WorkerContext,
        persist_mapping_result,
    )

    server, _config, pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = resp["artifact_id"]

    # Read current generation (mapping already completed in sync mode)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT generation, map_status FROM artifacts "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, artifact_id),
        ).fetchone()
        original_gen = row[0]
        assert row[1] == "ready"

        # Simulate: set status back to stale, bump generation (concurrent modification)
        conn.execute(
            "UPDATE artifacts SET map_status = 'stale', generation = generation + 1 "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, artifact_id),
        )
        conn.commit()

    # Try to persist mapping with OLD generation — should be rejected
    stale_ctx = WorkerContext(
        artifact_id=artifact_id,
        generation=original_gen,
        map_status="stale",
    )
    fake_result = MappingResult(
        map_kind="full",
        map_status="ready",
        mapped_part_index=0,
        roots=[],
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error=None,
    )

    with pool.connection() as conn:
        persisted = persist_mapping_result(
            conn, worker_ctx=stale_ctx, result=fake_result
        )
        assert persisted is False

    # Verify artifact still has stale status (not overwritten)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT map_status, generation FROM artifacts "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, artifact_id),
        ).fetchone()
        assert row[0] == "stale"
        assert row[1] == original_gen + 1


# ---------------------------------------------------------------------------
# Test 19: Artifact.status (health check)
# ---------------------------------------------------------------------------


def test_e2e_artifact_status(e2e_env):
    server, _config, _pool = e2e_env

    result = _status(server)
    assert result["type"] == "gateway_status"
    assert result["db"]["ok"] is True
    assert result["fs"]["ok"] is True
    assert "versions" in result
    assert result["mapping_mode"] == "sync"

    # Verify upstream is listed
    upstreams = result.get("upstreams", [])
    assert any(u["prefix"] == "test" for u in upstreams)

    # Verify budgets are present
    budgets = result.get("budgets", {})
    assert budgets.get("max_full_map_bytes") == 2000


# ---------------------------------------------------------------------------
# Test 20: Multi-session artifact sharing via cache
# ---------------------------------------------------------------------------


def test_e2e_multi_session_cache_sharing(e2e_env):
    server, _config, pool = e2e_env
    session_a = f"sess_{uuid.uuid4().hex}"
    session_b = f"sess_{uuid.uuid4().hex}"
    unique = f"shared_{uuid.uuid4().hex}"

    # Create artifact in session A
    resp_a = _call_mirrored(
        server,
        "test.get_users",
        session_a,
        extra_args={"message": unique},
        cache_mode="allow",
    )
    artifact_id = resp_a["artifact_id"]

    # Session B calls with same args — gets cache reuse
    resp_b = _call_mirrored(
        server,
        "test.get_users",
        session_b,
        extra_args={"message": unique},
        cache_mode="allow",
    )
    assert resp_b["artifact_id"] == artifact_id
    assert resp_b["meta"]["cache"]["reused"] is True

    # Session A can access the artifact
    get_a = _get(server, session_a, artifact_id)
    assert get_a["artifact_id"] == artifact_id

    # Cache reuse does NOT create an artifact_ref for session B, so
    # session B cannot access the artifact via get/search (by design:
    # the reuse path returns early without persisting a session ref).
    get_b = _get(server, session_b, artifact_id)
    assert get_b.get("code") == "NOT_FOUND"

    # Session A finds it in search, session B does not
    search_a = _search(server, session_a)
    assert any(item["artifact_id"] == artifact_id for item in search_a["items"])

    search_b = _search(server, session_b)
    assert not any(
        item["artifact_id"] == artifact_id for item in search_b["items"]
    )

    # Manually insert artifact_ref for session B to verify access works with ref
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO artifact_refs (workspace_id, session_id, artifact_id, "
            "first_seen_at, last_seen_at) VALUES (%s, %s, %s, NOW(), NOW()) "
            "ON CONFLICT DO NOTHING",
            (WORKSPACE_ID, session_b, artifact_id),
        )
        conn.commit()

    # Now session B can access
    get_b2 = _get(server, session_b, artifact_id)
    assert get_b2["artifact_id"] == artifact_id
