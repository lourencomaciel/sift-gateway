-- 002_indexes.sql: Additional indexes (SQLite version, same as Postgres).
CREATE INDEX IF NOT EXISTS idx_binary_blobs_created_at
    ON binary_blobs (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_binary_blobs_byte_count
    ON binary_blobs (workspace_id, byte_count);
CREATE INDEX IF NOT EXISTS idx_payload_blobs_created_at
    ON payload_blobs (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payload_blobs_total_bytes
    ON payload_blobs (workspace_id, payload_total_bytes);
CREATE INDEX IF NOT EXISTS idx_payload_hash_aliases_dedupe_created
    ON payload_hash_aliases (workspace_id, payload_hash_dedupe, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payload_binary_refs_binary_hash
    ON payload_binary_refs (workspace_id, binary_hash);
CREATE INDEX IF NOT EXISTS idx_payload_binary_refs_created_at
    ON payload_binary_refs (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_artifacts_deleted_at
    ON artifacts (workspace_id, deleted_at)
    WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artifacts_last_referenced_at
    ON artifacts (workspace_id, last_referenced_at DESC);
CREATE INDEX IF NOT EXISTS idx_artifacts_upstream_tool
    ON artifacts (workspace_id, upstream_instance_id, source_tool);
CREATE INDEX IF NOT EXISTS idx_artifact_roots_root_path
    ON artifact_roots (workspace_id, root_path);
CREATE INDEX IF NOT EXISTS idx_artifacts_request_args_hash
    ON artifacts (workspace_id, request_args_hash)
    WHERE request_args_hash IS NOT NULL;
