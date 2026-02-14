"""End-to-end integration tests for SQLite runtime artifact flows."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
import uuid

import pytest

from sift_mcp.config.settings import (
    GatewayConfig,
    PaginationConfig,
    UpstreamConfig,
)
from sift_mcp.db.backend import SqliteBackend
from sift_mcp.db.migrate import apply_migrations
from sift_mcp.mcp.server import GatewayServer
from sift_mcp.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)

_SMALL_JSON = {
    "users": [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "viewer"},
        {"id": 3, "name": "Charlie", "role": "editor"},
    ],
    "total": 3,
}


def _cursor_page(
    *,
    start_id: int,
    count: int,
    next_cursor: str | None,
) -> dict[str, Any]:
    return {
        "data": [{"id": i} for i in range(start_id, start_id + count)],
        "paging": {
            "cursors": {"after": next_cursor},
            "next": (
                f"https://example.test/page?after={next_cursor}"
                if next_cursor is not None
                else None
            ),
        },
    }


_CURSOR_PAGES: dict[str | None, dict[str, Any]] = {
    None: _cursor_page(start_id=1, count=3, next_cursor="CUR2"),
    "CUR2": _cursor_page(start_id=4, count=3, next_cursor=None),
}


_UPSTREAM_DISPATCH: dict[str, tuple[str, dict[str, Any]]] = {
    "get_users": (
        "Return user list",
        {
            "content": [],
            "structuredContent": _SMALL_JSON,
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
}


async def _stub_upstream(
    _instance: Any,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None = None,  # noqa: ARG001
) -> dict[str, Any]:
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
    entry = _UPSTREAM_DISPATCH.get(tool_name)
    if entry is not None:
        return entry[1]
    return {
        "content": [{"type": "text", "text": f"unknown tool {tool_name}"}],
        "structuredContent": arguments,
        "isError": False,
        "meta": {},
    }


def _sqlite_config(tmp_path: Path) -> GatewayConfig:
    return GatewayConfig(
        data_dir=tmp_path,
        db_backend="sqlite",
        mapping_mode="sync",
        max_full_map_bytes=2000,
        passthrough_max_bytes=8192,
    )


def _sqlite_migrations_dir() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "src"
        / "sift_mcp"
        / "db"
        / "migrations_sqlite"
    )


def _cursor_pagination_config() -> PaginationConfig:
    return PaginationConfig(
        strategy="cursor",
        cursor_response_path="$.paging.cursors.after",
        cursor_param_name="after",
        has_more_response_path="$.paging.next",
    )


def _build_upstream(
    *,
    pagination: PaginationConfig | None = None,
    auto_paginate_max_pages: int | None = None,
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
            passthrough_allowed=False,
        ),
        instance_id="upstream_sqlite_e2e",
        tools=tools,
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


@pytest.fixture()
def sqlite_e2e_env(tmp_path, monkeypatch):
    """Provision SQLite backend, migrations, server, and upstream stub."""
    config = _sqlite_config(tmp_path)
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    backend = SqliteBackend(
        db_path=config.sqlite_path,
        busy_timeout_ms=config.sqlite_busy_timeout_ms,
    )
    try:
        with backend.connection() as conn:
            apply_migrations(conn, _sqlite_migrations_dir(), param_marker="?")

        upstream = _build_upstream()
        server = GatewayServer(
            config=config, db_pool=backend, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, backend
    finally:
        backend.close()


@pytest.fixture()
def sqlite_e2e_paginated_env(tmp_path, monkeypatch):
    """Provision SQLite backend with manual next_page pagination flow."""
    config = _sqlite_config(tmp_path)
    for d in [config.state_dir, config.blobs_bin_dir, config.tmp_dir]:
        d.mkdir(parents=True, exist_ok=True)

    backend = SqliteBackend(
        db_path=config.sqlite_path,
        busy_timeout_ms=config.sqlite_busy_timeout_ms,
    )
    try:
        with backend.connection() as conn:
            apply_migrations(conn, _sqlite_migrations_dir(), param_marker="?")

        upstream = _build_upstream(
            pagination=_cursor_pagination_config(),
            auto_paginate_max_pages=1,
        )
        server = GatewayServer(
            config=config, db_pool=backend, upstreams=[upstream]
        )
        monkeypatch.setattr(
            "sift_mcp.mcp.server.call_upstream_tool",
            _stub_upstream,
        )
        yield server, config, backend
    finally:
        backend.close()


def test_sqlite_e2e_full_pipeline(sqlite_e2e_env):
    server, _config, _backend = sqlite_e2e_env
    session_id = f"sess_{uuid.uuid4().hex}"

    mirrored = _call_mirrored(server, "test.get_users", session_id)
    assert mirrored["type"] == "gateway_tool_result"
    artifact_id = mirrored["artifact_id"]

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
    assert envelope["pagination"]["retrieval_status"] == "COMPLETE"

    describe = _artifact_query(
        server,
        session_id,
        query_kind="describe",
        artifact_id=artifact_id,
        scope="single",
    )
    root_paths = [root["root_path"] for root in describe["roots"]]
    assert "$.users" in root_paths

    selected = _artifact_query(
        server,
        session_id,
        query_kind="select",
        artifact_id=artifact_id,
        root_path="$.users",
        select_paths=["name", "role"],
        scope="single",
    )
    names = {item["projection"]["$.name"] for item in selected["items"]}
    assert names == {"Alice", "Bob", "Charlie"}


def test_sqlite_e2e_next_page_flow(sqlite_e2e_paginated_env):
    server, _config, _backend = sqlite_e2e_paginated_env
    session_id = f"sess_{uuid.uuid4().hex}"

    first = _call_mirrored(server, "test.get_cursor_users", session_id)
    first_id = first["artifact_id"]
    assert first["pagination"]["retrieval_status"] == "PARTIAL"
    assert first["pagination"]["has_next_page"] is True

    second = _artifact_next_page(server, session_id, first_id)
    second_id = second["artifact_id"]
    assert second["pagination"]["retrieval_status"] == "COMPLETE"
    assert second["pagination"]["has_next_page"] is False

    ids = _artifact_query(
        server,
        session_id,
        query_kind="get",
        artifact_id=second_id,
        target="envelope",
        jsonpath="$.data[*].id",
        scope="single",
    )
    assert [item["value"] for item in ids["items"]] == [4, 5, 6]


def test_sqlite_e2e_cache_reuse_across_sessions(sqlite_e2e_env):
    server, _config, _backend = sqlite_e2e_env
    session_a = f"sess_{uuid.uuid4().hex}"
    session_b = f"sess_{uuid.uuid4().hex}"
    marker = f"sqlite_shared_{uuid.uuid4().hex}"

    first = _call_mirrored(
        server,
        "test.get_users",
        session_a,
        extra_args={"marker": marker},
        allow_reuse=True,
    )
    artifact_id = first["artifact_id"]

    second = _call_mirrored(
        server,
        "test.get_users",
        session_b,
        extra_args={"marker": marker},
        allow_reuse=True,
    )
    assert second["artifact_id"] == artifact_id
    assert second["meta"]["cache"]["reused"] is True

    get_b = _artifact_query(
        server,
        session_b,
        query_kind="get",
        artifact_id=artifact_id,
        target="envelope",
        scope="single",
    )
    assert get_b["artifact_id"] == artifact_id
