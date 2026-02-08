-- 002_indexes.sql: Additional indexes for production query patterns.
-- Indexes here supplement those in 001_init.sql (which already covers:
--   sessions: idx_sessions_last_seen
--   binary_blobs: PK(workspace_id, binary_hash), UQ(workspace_id, blob_id)
--   payload_blobs: PK(workspace_id, payload_hash_full)
--   artifacts: ux_artifacts_parent_chain, idx_artifacts_request_key_created_seq,
--              idx_artifacts_created_seq_desc, idx_artifacts_expires_active,
--              idx_artifacts_session_id, idx_artifacts_payload_hash
--   artifact_refs: idx_artifact_refs_last_seen
--   artifact_samples: idx_artifact_samples_root_path
-- )

-- ---------------------------------------------------------------------------
-- binary_blobs indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_binary_blobs_created_at
    ON binary_blobs (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_binary_blobs_byte_count
    ON binary_blobs (workspace_id, byte_count);

-- ---------------------------------------------------------------------------
-- payload_blobs indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_payload_blobs_created_at
    ON payload_blobs (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_payload_blobs_total_bytes
    ON payload_blobs (workspace_id, payload_total_bytes);

-- ---------------------------------------------------------------------------
-- payload_hash_aliases indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_payload_hash_aliases_dedupe_created
    ON payload_hash_aliases (workspace_id, payload_hash_dedupe, created_at DESC);

-- ---------------------------------------------------------------------------
-- payload_binary_refs indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_payload_binary_refs_binary_hash
    ON payload_binary_refs (workspace_id, binary_hash);

CREATE INDEX IF NOT EXISTS idx_payload_binary_refs_created_at
    ON payload_binary_refs (workspace_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- artifacts: additional indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_artifacts_deleted_at
    ON artifacts (workspace_id, deleted_at)
    WHERE deleted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_last_referenced_at
    ON artifacts (workspace_id, last_referenced_at DESC);

CREATE INDEX IF NOT EXISTS idx_artifacts_upstream_tool
    ON artifacts (workspace_id, upstream_instance_id, source_tool);

-- ---------------------------------------------------------------------------
-- artifact_roots indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_artifact_roots_root_path
    ON artifact_roots (workspace_id, root_path);

-- ---------------------------------------------------------------------------
-- artifacts: reuse lookup index on request_args_hash
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_artifacts_request_args_hash
    ON artifacts (workspace_id, request_args_hash)
    WHERE request_args_hash IS NOT NULL;
