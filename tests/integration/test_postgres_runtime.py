from __future__ import annotations

import asyncio
import os
from pathlib import Path
import uuid

import pytest

from mcp_artifact_gateway.artifacts.create import (
    CreateArtifactInput,
    persist_artifact,
)
from mcp_artifact_gateway.config.settings import GatewayConfig, UpstreamConfig
from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.db.conn import create_pool
from mcp_artifact_gateway.db.migrate import apply_migrations
from mcp_artifact_gateway.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
)
from mcp_artifact_gateway.jobs.hard_delete import run_hard_delete_batch
from mcp_artifact_gateway.mcp.server import GatewayServer
from mcp_artifact_gateway.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)

_POSTGRES_DSN_ENV = "MCP_GATEWAY_TEST_POSTGRES_DSN"


def _integration_config(tmp_path: Path) -> GatewayConfig:
    dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {_POSTGRES_DSN_ENV} to run integration tests")
    return GatewayConfig(
        data_dir=tmp_path,
        postgres_dsn=dsn,
        mapping_mode="sync",
    )


def _migrations_dir() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "src"
        / "mcp_artifact_gateway"
        / "db"
        / "migrations"
    )


def _make_envelope(data: dict | None = None) -> Envelope:
    """Build a simple JSON envelope for testing."""
    return Envelope(
        upstream_instance_id="upstream_int_1",
        upstream_prefix="demo",
        tool="echo",
        status="ok",
        content=[JsonContentPart(value=data or {"id": 1, "status": "ok"})],
        meta={"warnings": []},
    )


def _persist_one(
    connection,
    config: GatewayConfig,
    session_id: str,
    *,
    envelope: Envelope | None = None,
    binary_hashes: list[str] | None = None,
):
    """Persist a single artifact and return the handle."""
    return persist_artifact(
        connection=connection,
        config=config,
        input_data=CreateArtifactInput(
            session_id=session_id,
            upstream_instance_id="upstream_int_1",
            prefix="demo",
            tool_name="echo",
            request_key=f"rk_{uuid.uuid4().hex}",
            request_args_hash=f"args_{uuid.uuid4().hex}",
            request_args_prefix="args_prefix",
            upstream_tool_schema_hash="schema_demo_echo",
            envelope=envelope or _make_envelope(),
        ),
        binary_hashes=binary_hashes,
    )


# ---------------------------------------------------------------------------
# Batch 1 tests (existing)
# ---------------------------------------------------------------------------
def test_runtime_handlers_work_with_real_postgres(tmp_path: Path) -> None:
    config = _integration_config(tmp_path)
    pool = create_pool(config)

    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_id = f"sess_int_{uuid.uuid4().hex}"
        artifact_id: str
        with pool.connection() as connection:
            handle = _persist_one(connection, config, session_id)
            artifact_id = handle.artifact_id

        server = GatewayServer(config=config, db_pool=pool)
        search = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        assert any(
            item["artifact_id"] == artifact_id for item in search["items"]
        )

        envelope_result = asyncio.run(
            server.handle_artifact_get(
                {
                    "_gateway_context": {"session_id": session_id},
                    "artifact_id": artifact_id,
                    "target": "envelope",
                }
            )
        )
        assert envelope_result["artifact_id"] == artifact_id
        assert envelope_result["target"] == "envelope"
    finally:
        pool.close()


def test_mirrored_tool_flow_persists_artifact_with_real_postgres(
    tmp_path: Path, monkeypatch
) -> None:
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        upstream = UpstreamInstance(
            config=UpstreamConfig(
                prefix="demo",
                transport="stdio",
                command="/usr/bin/printf",
            ),
            instance_id="upstream_int_2",
            tools=[
                UpstreamToolSchema(
                    name="echo",
                    description="echo",
                    input_schema={
                        "type": "object",
                        "properties": {"message": {"type": "string"}},
                        "required": ["message"],
                    },
                    schema_hash="schema_demo_echo",
                )
            ],
        )
        server = GatewayServer(
            config=config, db_pool=pool, upstreams=[upstream]
        )
        mirrored = server.mirrored_tools["demo.echo"]

        async def _fake_call(_upstream, _tool, args):
            return {
                "content": [
                    {"type": "text", "text": str(args.get("message", ""))}
                ],
                "structuredContent": {"message": args.get("message")},
                "isError": False,
                "meta": {},
            }

        monkeypatch.setattr(
            "mcp_artifact_gateway.mcp.server.call_upstream_tool", _fake_call
        )

        session_id = f"sess_int_{uuid.uuid4().hex}"
        response = asyncio.run(
            server.handle_mirrored_tool(
                mirrored,
                {
                    "_gateway_context": {
                        "session_id": session_id,
                        "cache_mode": "fresh",
                    },
                    "message": "hello integration",
                },
            )
        )
        assert response["type"] == "gateway_tool_result"
        artifact_id = response["artifact_id"]
        assert isinstance(artifact_id, str) and artifact_id.startswith("art_")

        search = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        assert any(
            item["artifact_id"] == artifact_id for item in search["items"]
        )
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Batch 2: G45 session visibility and cleanup correctness
# ---------------------------------------------------------------------------


def test_search_only_returns_artifacts_visible_to_own_session(
    tmp_path: Path,
) -> None:
    """artifact.search must only return artifacts in artifact_refs for that session.

    Artifacts created under session_a must NOT appear when session_b
    calls artifact.search (and vice versa), proving session isolation.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_a = f"sess_a_{uuid.uuid4().hex}"
        session_b = f"sess_b_{uuid.uuid4().hex}"

        with pool.connection() as connection:
            handle_a = _persist_one(
                connection,
                config,
                session_a,
                envelope=_make_envelope({"owner": "a"}),
            )
        with pool.connection() as connection:
            handle_b = _persist_one(
                connection,
                config,
                session_b,
                envelope=_make_envelope({"owner": "b"}),
            )

        server = GatewayServer(config=config, db_pool=pool)

        # Search as session_a
        search_a = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_a}, "filters": {}}
            )
        )
        ids_a = {item["artifact_id"] for item in search_a["items"]}

        # Search as session_b
        search_b = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_b}, "filters": {}}
            )
        )
        ids_b = {item["artifact_id"] for item in search_b["items"]}

        # session_a sees its own artifact, not session_b's
        assert handle_a.artifact_id in ids_a
        assert handle_b.artifact_id not in ids_a

        # session_b sees its own artifact, not session_a's
        assert handle_b.artifact_id in ids_b
        assert handle_a.artifact_id not in ids_b
    finally:
        pool.close()


def test_artifact_visible_immediately_after_creation(tmp_path: Path) -> None:
    """A new artifact must appear in artifact.search immediately after persist.

    persist_artifact commits must have no deferred visibility window.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_id = f"sess_imm_{uuid.uuid4().hex}"
        server = GatewayServer(config=config, db_pool=pool)

        # Before creation: search must return zero items for this session
        search_before = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        assert len(search_before["items"]) == 0

        # Create artifact
        with pool.connection() as connection:
            handle = _persist_one(connection, config, session_id)

        # Immediately after: search returns the artifact
        search_after = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        found_ids = {item["artifact_id"] for item in search_after["items"]}
        assert handle.artifact_id in found_ids

        # Also verify artifact.get succeeds immediately
        get_result = asyncio.run(
            server.handle_artifact_get(
                {
                    "_gateway_context": {"session_id": session_id},
                    "artifact_id": handle.artifact_id,
                    "target": "envelope",
                }
            )
        )
        assert get_result["artifact_id"] == handle.artifact_id
    finally:
        pool.close()


def test_artifact_get_not_found_for_wrong_session(tmp_path: Path) -> None:
    """Verify artifact.get returns NOT_FOUND for another session's artifact."""
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_owner = f"sess_own_{uuid.uuid4().hex}"
        session_other = f"sess_oth_{uuid.uuid4().hex}"

        with pool.connection() as connection:
            handle = _persist_one(connection, config, session_owner)

        server = GatewayServer(config=config, db_pool=pool)

        # Owner can get it
        get_owner = asyncio.run(
            server.handle_artifact_get(
                {
                    "_gateway_context": {"session_id": session_owner},
                    "artifact_id": handle.artifact_id,
                    "target": "envelope",
                }
            )
        )
        assert get_owner["artifact_id"] == handle.artifact_id

        # Other session cannot
        get_other = asyncio.run(
            server.handle_artifact_get(
                {
                    "_gateway_context": {"session_id": session_other},
                    "artifact_id": handle.artifact_id,
                    "target": "envelope",
                }
            )
        )
        assert get_other["code"] == "NOT_FOUND"
    finally:
        pool.close()


def test_payload_binary_refs_recorded_on_persist(tmp_path: Path) -> None:
    """Verify binary_hashes create payload_binary_refs rows on persist.

    This prevents the hard-delete job from orphaning binary blobs
    that are still referenced.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_id = f"sess_bin_{uuid.uuid4().hex}"
        bin_hash_1 = f"binhash_{uuid.uuid4().hex}"
        bin_hash_2 = f"binhash_{uuid.uuid4().hex}"

        # Insert fake binary_blobs rows so the FK is satisfied
        with pool.connection() as connection:
            for bh in (bin_hash_1, bin_hash_2):
                connection.execute(
                    """
                    INSERT INTO binary_blobs
                        (workspace_id, binary_hash, blob_id, byte_count, mime, fs_path)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        WORKSPACE_ID,
                        bh,
                        f"blob_{bh[:16]}",
                        1024,
                        "application/octet-stream",
                        f"/tmp/{bh}",
                    ),
                )
            connection.commit()

        envelope = Envelope(
            upstream_instance_id="upstream_int_1",
            upstream_prefix="demo",
            tool="echo",
            status="ok",
            content=[
                BinaryRefContentPart(
                    blob_id=f"blob_{bin_hash_1[:16]}",
                    binary_hash=bin_hash_1,
                    mime="application/octet-stream",
                    byte_count=1024,
                ),
                BinaryRefContentPart(
                    blob_id=f"blob_{bin_hash_2[:16]}",
                    binary_hash=bin_hash_2,
                    mime="application/octet-stream",
                    byte_count=1024,
                ),
            ],
            meta={"warnings": []},
        )

        with pool.connection() as connection:
            handle = persist_artifact(
                connection=connection,
                config=config,
                input_data=CreateArtifactInput(
                    session_id=session_id,
                    upstream_instance_id="upstream_int_1",
                    prefix="demo",
                    tool_name="echo",
                    request_key=f"rk_{uuid.uuid4().hex}",
                    request_args_hash=f"args_{uuid.uuid4().hex}",
                    request_args_prefix="args_prefix",
                    upstream_tool_schema_hash="schema_demo_echo",
                    envelope=envelope,
                ),
                binary_hashes=[bin_hash_1, bin_hash_2],
            )

        # Verify payload_binary_refs rows exist
        with pool.connection() as connection:
            rows = connection.execute(
                """
                SELECT binary_hash FROM payload_binary_refs
                WHERE workspace_id = %s AND payload_hash_full = %s
                ORDER BY binary_hash
                """,
                (WORKSPACE_ID, handle.payload_hash_full),
            ).fetchall()
            recorded_hashes = {row[0] for row in rows}

        assert bin_hash_1 in recorded_hashes
        assert bin_hash_2 in recorded_hashes
    finally:
        pool.close()


def test_hard_delete_removes_payload_only_when_unreferenced(
    tmp_path: Path,
) -> None:
    """Shared payloads survive hard-delete until all references are gone.

    When two artifacts share the same payload_hash_full and one is
    hard-deleted, the payload must NOT be removed because the other
    artifact still references it.  Only after both are gone should
    the payload be deleted.

    This validates the FIND_UNREFERENCED_PAYLOADS_SQL subquery in the
    hard-delete job.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_1 = f"sess_hd1_{uuid.uuid4().hex}"
        session_2 = f"sess_hd2_{uuid.uuid4().hex}"

        # Use the SAME envelope so both artifacts share the same payload_hash_full
        shared_envelope = _make_envelope(
            {"shared": True, "key": "deterministic_payload"}
        )

        with pool.connection() as connection:
            handle_1 = _persist_one(
                connection, config, session_1, envelope=shared_envelope
            )
        with pool.connection() as connection:
            handle_2 = _persist_one(
                connection, config, session_2, envelope=shared_envelope
            )

        payload_hash = handle_1.payload_hash_full
        assert payload_hash == handle_2.payload_hash_full, (
            "Both artifacts must share the same payload hash"
        )

        # Soft-delete artifact 1 by setting deleted_at directly
        with pool.connection() as connection:
            connection.execute(
                """
                UPDATE artifacts
                SET deleted_at = NOW() - INTERVAL '1 day',
                    expires_at = NOW() - INTERVAL '2 days'
                WHERE workspace_id = %s AND artifact_id = %s
                """,
                (WORKSPACE_ID, handle_1.artifact_id),
            )
            connection.commit()

        # Run hard delete -- should remove artifact 1 row
        with pool.connection() as connection:
            result = run_hard_delete_batch(
                connection,
                grace_period_timestamp="9999-12-31T23:59:59Z",
                batch_size=100,
                remove_fs_blobs=False,
            )
        assert result.artifacts_deleted >= 1

        # Payload must still exist because artifact 2 still references it
        with pool.connection() as connection:
            payload_row = connection.execute(
                "SELECT 1 FROM payload_blobs WHERE workspace_id = %s AND payload_hash_full = %s",
                (WORKSPACE_ID, payload_hash),
            ).fetchone()
        assert payload_row is not None, (
            "Payload must survive when another artifact still references it"
        )

        # Now soft-delete artifact 2 as well
        with pool.connection() as connection:
            connection.execute(
                """
                UPDATE artifacts
                SET deleted_at = NOW() - INTERVAL '1 day',
                    expires_at = NOW() - INTERVAL '2 days'
                WHERE workspace_id = %s AND artifact_id = %s
                """,
                (WORKSPACE_ID, handle_2.artifact_id),
            )
            connection.commit()

        # Run hard delete again -- should remove artifact 2 and then the payload
        with pool.connection() as connection:
            result2 = run_hard_delete_batch(
                connection,
                grace_period_timestamp="9999-12-31T23:59:59Z",
                batch_size=100,
                remove_fs_blobs=False,
            )
        assert result2.artifacts_deleted >= 1

        # NOW the payload should be gone because no artifacts reference it
        with pool.connection() as connection:
            payload_row = connection.execute(
                "SELECT 1 FROM payload_blobs WHERE workspace_id = %s AND payload_hash_full = %s",
                (WORKSPACE_ID, payload_hash),
            ).fetchone()
        assert payload_row is None, (
            "Payload must be removed once all referencing artifacts are hard-deleted"
        )
    finally:
        pool.close()


def test_hard_delete_respects_binary_ref_protection(tmp_path: Path) -> None:
    """Binary blobs with active refs survive hard-delete.

    Binary blobs referenced via payload_binary_refs must not be
    deleted during hard delete while any payload still references them.

    The FK constraint payload_binary_refs -> binary_blobs uses ON DELETE
    RESTRICT, so the hard-delete job only removes binary_blobs that have
    no remaining payload_binary_refs rows.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_id = f"sess_bref_{uuid.uuid4().hex}"
        bin_hash = f"binhash_{uuid.uuid4().hex}"

        # Insert a binary_blob
        with pool.connection() as connection:
            connection.execute(
                """
                INSERT INTO binary_blobs
                    (workspace_id, binary_hash, blob_id, byte_count, mime, fs_path)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    WORKSPACE_ID,
                    bin_hash,
                    f"blob_{bin_hash[:16]}",
                    2048,
                    "application/octet-stream",
                    f"/tmp/{bin_hash}",
                ),
            )
            connection.commit()

        envelope_with_binary = Envelope(
            upstream_instance_id="upstream_int_1",
            upstream_prefix="demo",
            tool="echo",
            status="ok",
            content=[
                BinaryRefContentPart(
                    blob_id=f"blob_{bin_hash[:16]}",
                    binary_hash=bin_hash,
                    mime="application/octet-stream",
                    byte_count=2048,
                ),
            ],
            meta={"warnings": []},
        )

        # Create two artifacts sharing the same binary ref
        with pool.connection() as connection:
            handle_1 = persist_artifact(
                connection=connection,
                config=config,
                input_data=CreateArtifactInput(
                    session_id=session_id,
                    upstream_instance_id="upstream_int_1",
                    prefix="demo",
                    tool_name="echo",
                    request_key=f"rk_{uuid.uuid4().hex}",
                    request_args_hash=f"args_{uuid.uuid4().hex}",
                    request_args_prefix="args_prefix",
                    upstream_tool_schema_hash="schema_demo_echo",
                    envelope=envelope_with_binary,
                ),
                binary_hashes=[bin_hash],
            )

        session_id_2 = f"sess_bref2_{uuid.uuid4().hex}"
        with pool.connection() as connection:
            handle_2 = persist_artifact(
                connection=connection,
                config=config,
                input_data=CreateArtifactInput(
                    session_id=session_id_2,
                    upstream_instance_id="upstream_int_1",
                    prefix="demo",
                    tool_name="echo",
                    request_key=f"rk_{uuid.uuid4().hex}",
                    request_args_hash=f"args_{uuid.uuid4().hex}",
                    request_args_prefix="args_prefix",
                    upstream_tool_schema_hash="schema_demo_echo",
                    envelope=envelope_with_binary,
                ),
                binary_hashes=[bin_hash],
            )

        # Soft-delete + hard-delete artifact_1
        with pool.connection() as connection:
            connection.execute(
                """
                UPDATE artifacts
                SET deleted_at = NOW() - INTERVAL '1 day',
                    expires_at = NOW() - INTERVAL '2 days'
                WHERE workspace_id = %s AND artifact_id = %s
                """,
                (WORKSPACE_ID, handle_1.artifact_id),
            )
            connection.commit()

        with pool.connection() as connection:
            run_hard_delete_batch(
                connection,
                grace_period_timestamp="9999-12-31T23:59:59Z",
                batch_size=100,
                remove_fs_blobs=False,
            )

        # binary_blob must still exist because artifact_2's payload still references it
        with pool.connection() as connection:
            blob_row = connection.execute(
                "SELECT 1 FROM binary_blobs WHERE workspace_id = %s AND binary_hash = %s",
                (WORKSPACE_ID, bin_hash),
            ).fetchone()
        assert blob_row is not None, (
            "Binary blob must survive when a payload still references it"
        )

        # Now soft-delete + hard-delete artifact_2
        with pool.connection() as connection:
            connection.execute(
                """
                UPDATE artifacts
                SET deleted_at = NOW() - INTERVAL '1 day',
                    expires_at = NOW() - INTERVAL '2 days'
                WHERE workspace_id = %s AND artifact_id = %s
                """,
                (WORKSPACE_ID, handle_2.artifact_id),
            )
            connection.commit()

        with pool.connection() as connection:
            run_hard_delete_batch(
                connection,
                grace_period_timestamp="9999-12-31T23:59:59Z",
                batch_size=100,
                remove_fs_blobs=False,
            )

        # Now binary_blob should be gone (no more payload_binary_refs point to it)
        with pool.connection() as connection:
            blob_row = connection.execute(
                "SELECT 1 FROM binary_blobs WHERE workspace_id = %s AND binary_hash = %s",
                (WORKSPACE_ID, bin_hash),
            ).fetchone()
        assert blob_row is None, (
            "Binary blob must be removed after all referencing payloads are hard-deleted"
        )
    finally:
        pool.close()


def test_soft_delete_does_not_remove_from_search_until_expired(
    tmp_path: Path,
) -> None:
    """Artifact remains in search until soft-delete sets deleted_at.

    Only after soft-delete sets deleted_at does the default search
    filter (include_deleted=false) exclude it.
    """
    config = _integration_config(tmp_path)
    pool = create_pool(config)
    try:
        with pool.connection() as connection:
            apply_migrations(connection, _migrations_dir())

        session_id = f"sess_sd_{uuid.uuid4().hex}"

        with pool.connection() as connection:
            handle = _persist_one(connection, config, session_id)

        server = GatewayServer(config=config, db_pool=pool)

        # Artifact visible before soft delete
        search1 = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        assert any(
            item["artifact_id"] == handle.artifact_id
            for item in search1["items"]
        )

        # Manually set deleted_at (simulating soft-delete)
        with pool.connection() as connection:
            connection.execute(
                """
                UPDATE artifacts SET deleted_at = NOW()
                WHERE workspace_id = %s AND artifact_id = %s
                """,
                (WORKSPACE_ID, handle.artifact_id),
            )
            connection.commit()

        # Default search (include_deleted=false) should exclude it
        search2 = asyncio.run(
            server.handle_artifact_search(
                {"_gateway_context": {"session_id": session_id}, "filters": {}}
            )
        )
        assert not any(
            item["artifact_id"] == handle.artifact_id
            for item in search2["items"]
        )

        # Search with include_deleted=true should still find it
        search3 = asyncio.run(
            server.handle_artifact_search(
                {
                    "_gateway_context": {"session_id": session_id},
                    "filters": {"include_deleted": True},
                }
            )
        )
        assert any(
            item["artifact_id"] == handle.artifact_id
            for item in search3["items"]
        )
    finally:
        pool.close()
