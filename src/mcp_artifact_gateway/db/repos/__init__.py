"""Database repository modules for MCP Artifact Gateway."""

from mcp_artifact_gateway.db.repos.sessions_repo import (
    update_artifact_ref_last_seen,
    update_session_last_seen,
    upsert_artifact_ref,
    upsert_session,
)
from mcp_artifact_gateway.db.repos.payloads_repo import (
    find_by_dedupe_hash,
    get_payload_blob,
    upsert_binary_blob,
    upsert_payload_binary_ref,
    upsert_payload_blob,
    upsert_payload_hash_alias,
)
from mcp_artifact_gateway.db.repos.artifacts_repo import (
    find_latest_by_request_key,
    get_artifact,
    insert_artifact,
    search_by_session,
    soft_delete,
    touch_last_referenced,
)
from mcp_artifact_gateway.db.repos.mapping_repo import (
    get_artifact_roots,
    get_artifact_samples,
    replace_artifact_samples,
    update_mapping_status,
    upsert_artifact_root,
)
from mcp_artifact_gateway.db.repos.prune_repo import (
    delete_binary_blob,
    delete_payload_blob,
    find_soft_delete_candidates,
    find_unreferenced_binaries,
    find_unreferenced_payloads,
    hard_delete_artifact,
)

__all__ = [
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
