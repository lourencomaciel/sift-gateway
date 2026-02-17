"""End-to-end integration tests for the full MCP gateway artifact lifecycle.

Requires a live Postgres instance.  Set ``SIFT_MCP_TEST_POSTGRES_DSN`` to
enable these tests; they are auto-skipped when the env var is absent.

The upstream MCP server is stubbed at the ``call_upstream_tool`` function level
(the same pattern used by ``test_postgres_runtime.py``).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
from typing import Any
import uuid

import pytest

from sift_mcp.config.settings import (
    GatewayConfig,
    PaginationConfig,
    UpstreamConfig,
)
from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.db.conn import create_pool
from sift_mcp.db.migrate import apply_migrations
from sift_mcp.mcp.server import GatewayServer
from sift_mcp.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)

_POSTGRES_DSN_ENV = "SIFT_MCP_TEST_POSTGRES_DSN"


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


def _cursor_page_payload(
    *,
    start_id: int,
    count: int,
    next_cursor: str | None,
) -> dict[str, Any]:
    paging: dict[str, Any] = {
        "cursors": {"after": next_cursor},
        "next": (
            f"https://example.test/users?after={next_cursor}"
            if next_cursor is not None
            else None
        ),
    }
    return {
        "data": [
            {"id": i, "name": f"cursor_user_{i}"}
            for i in range(start_id, start_id + count)
        ],
        "paging": paging,
    }


_CURSOR_PAGES: dict[str | None, dict[str, Any]] = {
    None: _cursor_page_payload(start_id=1, count=4, next_cursor="CUR2"),
    "CUR2": _cursor_page_payload(start_id=5, count=4, next_cursor="CUR3"),
    "CUR3": _cursor_page_payload(start_id=9, count=4, next_cursor=None),
}

_CURSOR_TIMEOUT_PAGES: dict[str | None, dict[str, Any]] = {
    None: _cursor_page_payload(start_id=1, count=4, next_cursor="TMO2"),
    "TMO2": _cursor_page_payload(start_id=5, count=4, next_cursor="TMO3"),
}

_CURSOR_ERROR_PAGES: dict[str | None, dict[str, Any]] = {
    None: _cursor_page_payload(start_id=1, count=4, next_cursor="ERR2"),
}

_CURSOR_NON_JSON_PAGES: dict[str | None, dict[str, Any]] = {
    None: _cursor_page_payload(start_id=1, count=4, next_cursor="TXT2"),
}


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
    "get_cursor_users": (
        "Return cursor-paginated users",
        {
            "content": [],
            "structuredContent": _CURSOR_PAGES[None],
            "isError": False,
            "meta": {},
        },
    ),
    "get_cursor_users_timeout": (
        "Return cursor users but timeout on continuation",
        {
            "content": [],
            "structuredContent": _CURSOR_TIMEOUT_PAGES[None],
            "isError": False,
            "meta": {},
        },
    ),
    "get_cursor_users_error": (
        "Return cursor users but error on continuation",
        {
            "content": [],
            "structuredContent": _CURSOR_ERROR_PAGES[None],
            "isError": False,
            "meta": {},
        },
    ),
    "get_cursor_users_non_json": (
        "Return cursor users but non-json continuation",
        {
            "content": [],
            "structuredContent": _CURSOR_NON_JSON_PAGES[None],
            "isError": False,
            "meta": {},
        },
    ),
    "echo_input": (
        "Echo input arguments",
        {
            "content": [],
            "structuredContent": {},
            "isError": False,
            "meta": {},
        },
    ),
}


async def _stub_upstream(
    _instance: Any,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None = None,
) -> dict[str, Any]:
    """Fake upstream that returns controlled payloads by tool name."""
    if tool_name == "get_cursor_users":
        after = arguments.get("after")
        normalized_after = after if isinstance(after, str) else None
        return {
            "content": [],
            "structuredContent": _CURSOR_PAGES.get(
                normalized_after, _CURSOR_PAGES[None]
            ),
            "isError": False,
            "meta": {},
        }
    if tool_name == "get_cursor_users_timeout":
        after = arguments.get("after")
        normalized_after = after if isinstance(after, str) else None
        if normalized_after == "TMO2":
            await asyncio.sleep(0.2)
        return {
            "content": [],
            "structuredContent": _CURSOR_TIMEOUT_PAGES.get(
                normalized_after, _CURSOR_TIMEOUT_PAGES[None]
            ),
            "isError": False,
            "meta": {},
        }
    if tool_name == "get_cursor_users_error":
        after = arguments.get("after")
        normalized_after = after if isinstance(after, str) else None
        if normalized_after == "ERR2":
            return {
                "content": [{"type": "text", "text": "upstream page failed"}],
                "structuredContent": None,
                "isError": True,
                "meta": {"exception_type": "RuntimeError"},
            }
        return {
            "content": [],
            "structuredContent": _CURSOR_ERROR_PAGES[None],
            "isError": False,
            "meta": {},
        }
    if tool_name == "get_cursor_users_non_json":
        after = arguments.get("after")
        normalized_after = after if isinstance(after, str) else None
        if normalized_after == "TXT2":
            return {
                "content": [
                    {
                        "type": "text",
                        "text": "second page came back as text",
                    }
                ],
                "structuredContent": None,
                "isError": False,
                "meta": {},
            }
        return {
            "content": [],
            "structuredContent": _CURSOR_NON_JSON_PAGES[None],
            "isError": False,
            "meta": {},
        }
    if tool_name == "echo_input":
        return {
            "content": [],
            "structuredContent": {"received": arguments},
            "isError": False,
            "meta": {},
        }
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
        / "sift_mcp"
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
        passthrough_max_bytes=8192,
    )


def _build_upstream(
    *,
    pagination: PaginationConfig | None = None,
    auto_paginate_max_pages: int | None = None,
    auto_paginate_max_records: int | None = None,
    auto_paginate_timeout_seconds: float | None = None,
    passthrough_allowed: bool = False,
) -> UpstreamInstance:
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
            prefix="test",
            transport="stdio",
            command="/bin/echo",
            pagination=pagination,
            auto_paginate_max_pages=auto_paginate_max_pages,
            auto_paginate_max_records=auto_paginate_max_records,
            auto_paginate_timeout_seconds=auto_paginate_timeout_seconds,
            passthrough_allowed=passthrough_allowed,
        ),
        instance_id="upstream_e2e_test",
        tools=tools,
    )


def _cursor_pagination_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="cursor",
        cursor_response_path="$.paging.cursors.after",
        cursor_param_name="after",
        has_more_response_path="$.paging.next",
    )


def _call_mirrored(
    server: GatewayServer,
    tool_qualified_name: str,
    session_id: str,
    extra_args: dict[str, Any] | None = None,
    *,
    allow_reuse: bool = False,
) -> dict[str, Any]:
    mirrored = server.mirrored_tools[tool_qualified_name]
    args: dict[str, Any] = {
        "_gateway_context": {
            "session_id": session_id,
            "allow_reuse": allow_reuse,
        },
    }
    if extra_args:
        args.update(extra_args)
    return asyncio.run(server.handle_mirrored_tool(mirrored, args))


def _artifact_query(
    server: GatewayServer,
    session_id: str,
    *,
    query_kind: str,
    **query_args: Any,
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "action": "query",
        "query_kind": query_kind,
        "_gateway_context": {"session_id": session_id},
    }
    args.update(query_args)
    return asyncio.run(server.handle_artifact(args))


def _artifact_next_page(
    server: GatewayServer,
    session_id: str,
    artifact_id: str,
) -> dict[str, Any]:
    return asyncio.run(
        server.handle_artifact(
            {
                "action": "next_page",
                "_gateway_context": {"session_id": session_id},
                "artifact_id": artifact_id,
            }
        )
    )


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


def _describe_artifact(
    desc: dict[str, Any], artifact_id: str
) -> dict[str, Any]:
    for artifact in desc.get("artifacts", []):
        if (
            isinstance(artifact, dict)
            and artifact.get("artifact_id") == artifact_id
        ):
            return artifact
    raise AssertionError(f"missing artifact summary for {artifact_id}")


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
    scope: str | None = None,
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
    if scope is not None:
        args["scope"] = scope
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


@pytest.fixture
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

        upstream = _build_upstream(passthrough_allowed=False)
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


@pytest.fixture
def e2e_env_paginated(tmp_path, monkeypatch):
    """Provision a paginated upstream with default auto-pagination limits."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            passthrough_allowed=False,
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


@pytest.fixture
def e2e_env_paginated_manual(tmp_path, monkeypatch):
    """Provision a paginated upstream with manual next_page flow."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            auto_paginate_max_pages=1,
            passthrough_allowed=False,
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_kind"] == "full"
    assert artifact["map_status"] == "ready"
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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_kind"] == "partial"
    assert artifact["map_status"] == "ready"
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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_status"] == "failed"


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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_status"] == "ready"

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
        allow_reuse=True,
    )
    assert resp1["type"] == "gateway_tool_result"
    artifact_id_1 = resp1["artifact_id"]

    # Second call — identical args, should reuse
    resp2 = _call_mirrored(
        server,
        "test.get_users",
        session_id,
        extra_args={"message": unique},
        allow_reuse=True,
    )
    assert resp2["type"] == "gateway_tool_result"
    assert resp2["meta"]["cache"]["reused"] is True
    assert resp2["artifact_id"] == artifact_id_1


# ---------------------------------------------------------------------------
# Coverage: Cache fresh mode
# ---------------------------------------------------------------------------


def test_e2e_cache_fresh_when_reuse_disabled(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"
    unique = f"fresh_test_{uuid.uuid4().hex}"

    first = _call_mirrored(
        server,
        "test.get_users",
        session_id,
        extra_args={"message": unique},
        allow_reuse=False,
    )
    second = _call_mirrored(
        server,
        "test.get_users",
        session_id,
        extra_args={"message": unique},
        allow_reuse=False,
    )

    assert first["type"] == "gateway_tool_result"
    assert second["type"] == "gateway_tool_result"
    assert first["artifact_id"] != second["artifact_id"]
    assert first["meta"]["cache"]["reused"] is False
    assert second["meta"]["cache"]["reused"] is False


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


@pytest.fixture
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
        passthrough_max_bytes=8192,
        envelope_jsonb_mode="minimal_for_large",
        envelope_jsonb_minimize_threshold_bytes=100,
    )
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)
    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())
        upstream = _build_upstream(passthrough_allowed=False)
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


@pytest.fixture
def e2e_env_passthrough(tmp_path, monkeypatch):
    """Provision config where passthrough is enabled for upstream calls."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(passthrough_allowed=True)
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


@pytest.fixture
def e2e_env_paginated_failures(tmp_path, monkeypatch):
    """Provision paginated upstream with tight timeout for failure scenarios."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            auto_paginate_timeout_seconds=0.05,
            passthrough_allowed=False,
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Test 11: Soft delete → hard delete lifecycle
# ---------------------------------------------------------------------------


def test_e2e_soft_delete_hard_delete_lifecycle(e2e_env):
    from sift_mcp.jobs.hard_delete import run_hard_delete_batch
    from sift_mcp.jobs.soft_delete import (
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

    # all_related scope excludes deleted artifacts, so lookup becomes not found
    get_resp = _get(server, session_id, artifact_id)
    assert get_resp.get("code") == "NOT_FOUND"

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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_kind"] == "partial"

    # Find samples
    find_resp = _find(server, session_id, artifact_id)
    items = find_resp.get("items", [])
    assert len(items) > 0
    assert find_resp.get("sampled_only") is True

    # Each item should have root_path and sample_index
    for item in items:
        assert "root_path" in item
        assert "sample_index" in item
        assert "record_hash" in item


# ---------------------------------------------------------------------------
# Test 14: Artifact.get with JSONPath filtering
# ---------------------------------------------------------------------------


def test_e2e_get_with_jsonpath(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    resp = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = resp["artifact_id"]

    # Get envelope status via jsonpath
    result = _get(server, session_id, artifact_id, jsonpath="$.total")
    items = result.get("items", [])
    assert len(items) == 1
    assert items[0]["value"] == 3

    # Get user names from structured content
    result2 = _get(server, session_id, artifact_id, jsonpath="$.users[*].name")
    items2 = result2.get("items", [])
    assert len(items2) == 3
    assert {item["value"] for item in items2} == {"Alice", "Bob", "Charlie"}


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
    artifact = _describe_artifact(desc, artifact_id)
    assert artifact["map_kind"] == "partial"
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
        scope="single",
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
    from sift_mcp.mapping.runner import MappingResult
    from sift_mcp.mapping.worker import (
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
    server, _config, _pool = e2e_env
    session_a = f"sess_{uuid.uuid4().hex}"
    session_b = f"sess_{uuid.uuid4().hex}"
    unique = f"shared_{uuid.uuid4().hex}"

    # Create artifact in session A
    resp_a = _call_mirrored(
        server,
        "test.get_users",
        session_a,
        extra_args={"message": unique},
        allow_reuse=True,
    )
    artifact_id = resp_a["artifact_id"]

    # Session B calls with same args — gets cache reuse
    resp_b = _call_mirrored(
        server,
        "test.get_users",
        session_b,
        extra_args={"message": unique},
        allow_reuse=True,
    )
    assert resp_b["artifact_id"] == artifact_id
    assert resp_b["meta"]["cache"]["reused"] is True

    # Session A can access the artifact
    get_a = _get(server, session_a, artifact_id)
    assert get_a["artifact_id"] == artifact_id

    # Cache reuse should attach the artifact ref for session B, making
    # the returned handle immediately retrievable in that session.
    get_b = _get(server, session_b, artifact_id)
    assert get_b["artifact_id"] == artifact_id

    # Both sessions should now see the artifact in search.
    search_a = _search(server, session_a)
    assert any(item["artifact_id"] == artifact_id for item in search_a["items"])

    search_b = _search(server, session_b)
    assert any(item["artifact_id"] == artifact_id for item in search_b["items"])


# ---------------------------------------------------------------------------
# Test 21: Consolidated query flow (action=query)
# ---------------------------------------------------------------------------


def test_e2e_consolidated_query_flow(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_users", session_id)
    artifact_id = response["artifact_id"]

    search = _artifact_query(
        server,
        session_id,
        query_kind="search",
        filters={},
    )
    assert any(item["artifact_id"] == artifact_id for item in search["items"])

    envelope = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        scope="single",
    )
    assert envelope["artifact_id"] == artifact_id
    assert envelope["pagination"]["layer"] == "artifact_retrieval"
    assert envelope["pagination"]["retrieval_status"] == "COMPLETE"

    desc = _artifact_query(
        server,
        session_id,
        query_kind="describe",
        artifact_id=artifact_id,
        scope="single",
    )
    roots = desc.get("roots", [])
    root_paths = [r["root_path"] for r in roots]
    assert "$.users" in root_paths

    sel = _artifact_query(
        server,
        session_id,
        query_kind="select",
        artifact_id=artifact_id,
        root_path="$.users",
        select_paths=["name", "role"],
        scope="single",
    )
    names = {item["projection"]["$.name"] for item in sel["items"]}
    assert names == {"Alice", "Bob", "Charlie"}


# ---------------------------------------------------------------------------
# Test 22: Manual upstream pagination (action=next_page)
# ---------------------------------------------------------------------------


def test_e2e_next_page_multi_step_flow(e2e_env_paginated_manual):
    server, _config, _pool = e2e_env_paginated_manual
    session_id = f"sess_{uuid.uuid4().hex}"

    first_page = _call_mirrored(server, "test.get_cursor_users", session_id)
    first_id = first_page["artifact_id"]

    first_pagination = first_page.get("pagination", {})
    assert first_pagination.get("retrieval_status") == "PARTIAL"
    assert first_pagination.get("partial_reason") == "MORE_PAGES_AVAILABLE"
    assert first_pagination.get("has_next_page") is True
    assert first_pagination.get("next_action", {}).get("arguments") == {
        "action": "next_page",
        "artifact_id": first_id,
    }

    first_get = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=first_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    first_ids = [item["value"] for item in first_get["items"]]
    assert first_ids == [1, 2, 3, 4]

    second_page = _artifact_next_page(server, session_id, first_id)
    second_id = second_page["artifact_id"]
    assert second_id != first_id
    assert second_page["pagination"]["retrieval_status"] == "PARTIAL"
    assert second_page["pagination"]["page_number"] == 1

    second_get = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=second_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    second_ids = [item["value"] for item in second_get["items"]]
    assert second_ids == [5, 6, 7, 8]

    children = _artifact_query(
        server,
        session_id,
        query_kind="search",
        filters={"parent_artifact_id": first_id},
    )
    child = next(
        item for item in children["items"] if item["artifact_id"] == second_id
    )
    assert child["chain_seq"] == 1


# ---------------------------------------------------------------------------
# Test 23: Auto-pagination default merges upstream pages
# ---------------------------------------------------------------------------


def test_e2e_auto_pagination_default_merges_pages(e2e_env_paginated):
    server, _config, _pool = e2e_env_paginated
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_cursor_users", session_id)
    artifact_id = response["artifact_id"]

    pagination = response.get("pagination", {})
    assert pagination.get("retrieval_status") == "COMPLETE"
    assert pagination.get("has_next_page") is False
    assert pagination.get("next_action") is None
    assert pagination.get("page_number") == 2

    merged = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    merged_ids = [item["value"] for item in merged["items"]]
    assert len(merged_ids) == 12
    assert set(merged_ids) == set(range(1, 13))

    desc = _artifact_query(
        server,
        session_id,
        query_kind="describe",
        artifact_id=artifact_id,
        scope="single",
    )
    data_root = next(
        root for root in desc["roots"] if root["root_path"] == "$.data"
    )
    assert data_root["count_estimate"] == 12


# ---------------------------------------------------------------------------
# Test 24: Artifact-ref argument resolution on real mirrored calls
# ---------------------------------------------------------------------------


def test_e2e_artifact_ref_resolution_via_mirrored_call(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    source = _call_mirrored(server, "test.get_users", session_id)
    source_artifact_id = source["artifact_id"]

    echoed = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={
            "payload_ref": source_artifact_id,
            "name_ref": f"{source_artifact_id}:$.users[0].name",
            "keep": "verbatim",
        },
    )
    echoed_artifact_id = echoed["artifact_id"]

    name = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=echoed_artifact_id,
        target="envelope",
        jsonpath="$.received.name_ref",
        scope="single",
    )
    assert [item["value"] for item in name["items"]] == ["Alice"]

    total = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=echoed_artifact_id,
        target="envelope",
        jsonpath="$.received.payload_ref.total",
        scope="single",
    )
    assert [item["value"] for item in total["items"]] == [3]

    keep = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=echoed_artifact_id,
        target="envelope",
        jsonpath="$.received.keep",
        scope="single",
    )
    assert [item["value"] for item in keep["items"]] == ["verbatim"]


# ---------------------------------------------------------------------------
# Test 25: Passthrough response + eventual async persistence
# ---------------------------------------------------------------------------


def test_e2e_passthrough_eventually_persists_artifact(e2e_env_passthrough):
    server, _config, _pool = e2e_env_passthrough
    session_id = f"sess_{uuid.uuid4().hex}"
    nonce = f"pt_{uuid.uuid4().hex}"

    first = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"nonce": nonce},
        allow_reuse=False,
    )
    assert first.get("type") != "gateway_tool_result"
    assert (
        first.get("structuredContent", {}).get("received", {}).get("nonce")
        == nonce
    )

    handle: dict[str, Any] | None = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        probe = _call_mirrored(
            server,
            "test.echo_input",
            session_id,
            extra_args={"nonce": nonce},
            allow_reuse=True,
        )
        if probe.get("type") == "gateway_tool_result":
            handle = probe
            break
        time.sleep(0.05)

    assert handle is not None, (
        "timed out waiting for async passthrough persistence"
    )
    assert handle["meta"]["cache"]["reused"] is True
    artifact_id = handle["artifact_id"]

    search = _artifact_query(
        server,
        session_id,
        query_kind="search",
        filters={},
    )
    assert any(item["artifact_id"] == artifact_id for item in search["items"])

    echoed_nonce = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        jsonpath="$.received.nonce",
        scope="single",
    )
    assert [item["value"] for item in echoed_nonce["items"]] == [nonce]


# ---------------------------------------------------------------------------
# Test 26: Auto-pagination failure modes (timeout/error/non-JSON)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tool_name",
    [
        "test.get_cursor_users_timeout",
        "test.get_cursor_users_error",
        "test.get_cursor_users_non_json",
    ],
)
def test_e2e_auto_pagination_failure_modes(
    e2e_env_paginated_failures,
    tool_name: str,
):
    server, _config, _pool = e2e_env_paginated_failures
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, tool_name, session_id)
    assert response["type"] == "gateway_tool_result"
    artifact_id = response["artifact_id"]

    pagination = response.get("pagination", {})
    assert pagination.get("retrieval_status") == "PARTIAL"
    assert pagination.get("partial_reason") == "MORE_PAGES_AVAILABLE"
    assert pagination.get("has_next_page") is True
    assert pagination.get("page_number") == 0

    merged = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    merged_ids = [item["value"] for item in merged["items"]]
    assert merged_ids == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Test 27: Artifact-ref negative-path integration
# ---------------------------------------------------------------------------


def test_e2e_artifact_ref_missing_returns_not_found(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"
    missing_id = "art_" + "f" * 32

    result = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"payload_ref": missing_id},
    )
    assert result["code"] == "NOT_FOUND"
    assert "could not be resolved" in result["message"]


def test_e2e_artifact_ref_deleted_returns_gone(e2e_env):
    server, _config, pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    source = _call_mirrored(server, "test.get_users", session_id)
    source_artifact_id = source["artifact_id"]

    with pool.connection() as conn:
        conn.execute(
            "UPDATE artifacts SET deleted_at = NOW() "
            "WHERE workspace_id = %s AND artifact_id = %s",
            (WORKSPACE_ID, source_artifact_id),
        )
        conn.commit()

    result = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"payload_ref": source_artifact_id},
    )
    assert result["code"] == "GONE"
    assert "has been deleted" in result["message"]


def test_e2e_artifact_ref_binary_only_returns_invalid_argument(e2e_env):
    server, _config, pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

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

    source = _call_mirrored(server, "test.get_binary", session_id)
    source_artifact_id = source["artifact_id"]

    result = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"payload_ref": source_artifact_id},
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "binary" in result["message"]


def test_e2e_artifact_ref_invalid_jsonpath_returns_invalid_argument(e2e_env):
    server, _config, _pool = e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    source = _call_mirrored(server, "test.get_users", session_id)
    source_artifact_id = source["artifact_id"]

    result = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={
            "payload_ref": f"{source_artifact_id}:$.users[?(@.active)]",
        },
    )
    assert result["code"] == "INVALID_ARGUMENT"
    assert "JSONPath" in result["message"]


# ---------------------------------------------------------------------------
# Test 28: Passthrough failure path (async persistence failure)
# ---------------------------------------------------------------------------


def test_e2e_passthrough_async_persist_failure_is_non_fatal(
    e2e_env_passthrough,
    monkeypatch,
):
    server, _config, _pool = e2e_env_passthrough
    session_id = f"sess_{uuid.uuid4().hex}"
    nonce = f"pt_fail_{uuid.uuid4().hex}"

    def _raise_persist(*args, **kwargs):
        raise RuntimeError("simulated persist failure")

    monkeypatch.setattr(
        "sift_mcp.mcp.handlers.mirrored_tool.persist_artifact",
        _raise_persist,
    )

    result = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"nonce": nonce},
        allow_reuse=False,
    )
    assert result.get("type") != "gateway_tool_result"
    assert (
        result.get("structuredContent", {}).get("received", {}).get("nonce")
        == nonce
    )

    time.sleep(0.15)

    search = _artifact_query(
        server,
        session_id,
        query_kind="search",
        filters={},
    )
    assert search["items"] == []

    probe = _call_mirrored(
        server,
        "test.echo_input",
        session_id,
        extra_args={"nonce": nonce},
        allow_reuse=True,
    )
    assert probe.get("type") != "gateway_tool_result"


# ---------------------------------------------------------------------------
# Test 29: Auto-pagination boundary integration (max_pages/max_records)
# ---------------------------------------------------------------------------


@pytest.fixture
def e2e_env_paginated_max_pages(tmp_path, monkeypatch):
    """Paginated upstream capped to 2 total pages via auto-pagination."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            auto_paginate_max_pages=2,
            auto_paginate_max_records=1000,
            passthrough_allowed=False,
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


@pytest.fixture
def e2e_env_paginated_max_records(tmp_path, monkeypatch):
    """Paginated upstream capped to first page by record budget."""
    config = _e2e_config(tmp_path)

    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    pool = create_pool(config)
    try:
        with pool.connection() as conn:
            apply_migrations(conn, _migrations_dir())

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            auto_paginate_max_pages=10,
            auto_paginate_max_records=3,
            passthrough_allowed=False,
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, pool
    finally:
        pool.close()


def test_e2e_auto_pagination_max_pages_boundary(e2e_env_paginated_max_pages):
    server, _config, _pool = e2e_env_paginated_max_pages
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_cursor_users", session_id)
    artifact_id = response["artifact_id"]

    pagination = response.get("pagination", {})
    assert pagination.get("retrieval_status") == "PARTIAL"
    assert pagination.get("partial_reason") == "MORE_PAGES_AVAILABLE"
    assert pagination.get("has_next_page") is True
    assert pagination.get("page_number") == 1

    data = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    assert [item["value"] for item in data["items"]] == list(range(1, 9))


def test_e2e_auto_pagination_max_records_boundary(
    e2e_env_paginated_max_records,
):
    server, _config, _pool = e2e_env_paginated_max_records
    session_id = f"sess_{uuid.uuid4().hex}"

    response = _call_mirrored(server, "test.get_cursor_users", session_id)
    artifact_id = response["artifact_id"]

    pagination = response.get("pagination", {})
    assert pagination.get("retrieval_status") == "PARTIAL"
    assert pagination.get("partial_reason") == "MORE_PAGES_AVAILABLE"
    assert pagination.get("has_next_page") is True
    assert pagination.get("page_number") == 0

    data = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    assert [item["value"] for item in data["items"]] == [1, 2, 3, 4]
