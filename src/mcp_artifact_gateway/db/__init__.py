"""Database layer for MCP Artifact Gateway.

Re-exports the connection helpers, migration functions, and all repository
functions so callers can use a single import path::

    from mcp_artifact_gateway.db import create_pool, get_conn, upsert_session
"""

from mcp_artifact_gateway.db.conn import (
    create_pool,
    execute,
    fetchall,
    fetchone,
    get_conn,
    transaction,
)
from mcp_artifact_gateway.db.migrate import check_migrations, run_migrations
from mcp_artifact_gateway.db.repos import (
    delete_binary_blob,
    delete_payload_blob,
    find_by_dedupe_hash,
    find_latest_by_request_key,
    find_soft_delete_candidates,
    find_unreferenced_binaries,
    find_unreferenced_payloads,
    get_artifact,
    get_artifact_roots,
    get_artifact_samples,
    get_payload_blob,
    hard_delete_artifact,
    insert_artifact,
    replace_artifact_samples,
    search_by_session,
    soft_delete,
    touch_last_referenced,
    update_artifact_ref_last_seen,
    update_mapping_status,
    update_session_last_seen,
    upsert_artifact_ref,
    upsert_artifact_root,
    upsert_binary_blob,
    upsert_payload_binary_ref,
    upsert_payload_blob,
    upsert_payload_hash_alias,
    upsert_session,
)

__all__ = [
    # conn
    "create_pool",
    "get_conn",
    "transaction",
    "fetchone",
    "fetchall",
    "execute",
    # migrate
    "run_migrations",
    "check_migrations",
    # sessions
    "upsert_session",
    "update_session_last_seen",
    "upsert_artifact_ref",
    "update_artifact_ref_last_seen",
    # payloads
    "upsert_payload_blob",
    "get_payload_blob",
    "upsert_binary_blob",
    "upsert_payload_binary_ref",
    "upsert_payload_hash_alias",
    "find_by_dedupe_hash",
    # artifacts
    "insert_artifact",
    "get_artifact",
    "find_latest_by_request_key",
    "touch_last_referenced",
    "soft_delete",
    "search_by_session",
    # mapping
    "update_mapping_status",
    "upsert_artifact_root",
    "replace_artifact_samples",
    "get_artifact_roots",
    "get_artifact_samples",
    # prune
    "find_soft_delete_candidates",
    "hard_delete_artifact",
    "find_unreferenced_payloads",
    "delete_payload_blob",
    "find_unreferenced_binaries",
    "delete_binary_blob",
]
