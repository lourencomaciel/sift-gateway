-- SQLite schema: equivalent to Postgres 001_init.sql
-- Requires: PRAGMA foreign_keys = ON (set at connection time)

CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_name TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sessions (
    workspace_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_last_seen
    ON sessions (workspace_id, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS binary_blobs (
    workspace_id TEXT NOT NULL,
    binary_hash TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    mime TEXT NOT NULL,
    fs_path TEXT NOT NULL,
    probe_head_hash TEXT NULL,
    probe_tail_hash TEXT NULL,
    probe_bytes INTEGER NOT NULL DEFAULT 0 CHECK (probe_bytes >= 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, binary_hash),
    UNIQUE (workspace_id, blob_id)
);

CREATE TABLE IF NOT EXISTS payload_blobs (
    workspace_id TEXT NOT NULL,
    payload_hash_full TEXT NOT NULL,
    envelope JSON NULL,
    envelope_canonical_encoding TEXT NOT NULL CHECK (
        envelope_canonical_encoding IN ('zstd', 'gzip', 'none')
    ),
    envelope_canonical_bytes BLOB NOT NULL,
    envelope_canonical_bytes_len INTEGER NOT NULL CHECK (envelope_canonical_bytes_len >= 0),
    canonicalizer_version TEXT NOT NULL,
    payload_json_bytes INTEGER NOT NULL CHECK (payload_json_bytes >= 0),
    payload_binary_bytes_total INTEGER NOT NULL CHECK (payload_binary_bytes_total >= 0),
    payload_total_bytes INTEGER NOT NULL CHECK (payload_total_bytes >= 0),
    contains_binary_refs INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, payload_hash_full)
);

CREATE TABLE IF NOT EXISTS payload_binary_refs (
    workspace_id TEXT NOT NULL,
    payload_hash_full TEXT NOT NULL,
    binary_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, payload_hash_full, binary_hash),
    FOREIGN KEY (workspace_id, payload_hash_full)
        REFERENCES payload_blobs (workspace_id, payload_hash_full)
        ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, binary_hash)
        REFERENCES binary_blobs (workspace_id, binary_hash)
        ON DELETE RESTRICT
);

-- Sequence table to emulate Postgres GENERATED ALWAYS AS IDENTITY
CREATE TABLE IF NOT EXISTS _created_seq_counter (
    id INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS artifacts (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    created_seq INTEGER NOT NULL DEFAULT 0,
    session_id TEXT NOT NULL,
    source_tool TEXT NOT NULL,
    upstream_instance_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    payload_hash_full TEXT NOT NULL,
    canonicalizer_version TEXT NOT NULL,
    payload_json_bytes INTEGER NOT NULL CHECK (payload_json_bytes >= 0),
    payload_binary_bytes_total INTEGER NOT NULL CHECK (payload_binary_bytes_total >= 0),
    payload_total_bytes INTEGER NOT NULL CHECK (payload_total_bytes >= 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NULL,
    deleted_at TEXT NULL,
    last_referenced_at TEXT NOT NULL DEFAULT (datetime('now')),
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    parent_artifact_id TEXT NULL,
    chain_seq INTEGER NULL CHECK (chain_seq IS NULL OR chain_seq >= 0),
    map_kind TEXT NOT NULL DEFAULT 'none' CHECK (map_kind IN ('none', 'full', 'partial')),
    map_status TEXT NOT NULL DEFAULT 'pending' CHECK (
        map_status IN ('pending', 'ready', 'failed', 'stale')
    ),
    mapper_version TEXT NOT NULL,
    map_budget_fingerprint TEXT NULL,
    map_backend_id TEXT NULL,
    prng_version TEXT NULL,
    map_error TEXT NULL,
    upstream_tool_schema_hash TEXT NULL,
    request_args_hash TEXT NULL,
    request_args_prefix TEXT NULL,
    mapped_part_index INTEGER NULL CHECK (mapped_part_index IS NULL OR mapped_part_index >= 0),
    index_status TEXT NOT NULL DEFAULT 'off' CHECK (
        index_status IN ('off', 'pending', 'ready', 'partial', 'failed')
    ),
    error_summary TEXT NULL,
    PRIMARY KEY (workspace_id, artifact_id),
    FOREIGN KEY (workspace_id, session_id)
        REFERENCES sessions (workspace_id, session_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (workspace_id, payload_hash_full)
        REFERENCES payload_blobs (workspace_id, payload_hash_full)
        ON DELETE RESTRICT,
    FOREIGN KEY (workspace_id, parent_artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE SET NULL
);

-- Trigger to auto-generate created_seq from sequence table
CREATE TRIGGER IF NOT EXISTS trg_artifacts_created_seq
AFTER INSERT ON artifacts
FOR EACH ROW
WHEN NEW.created_seq = 0
BEGIN
    INSERT INTO _created_seq_counter (id) VALUES (NULL);
    UPDATE artifacts
        SET created_seq = last_insert_rowid()
        WHERE workspace_id = NEW.workspace_id AND artifact_id = NEW.artifact_id;
END;

CREATE UNIQUE INDEX IF NOT EXISTS ux_artifacts_parent_chain
    ON artifacts (workspace_id, parent_artifact_id, chain_seq)
    WHERE chain_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_request_key_created_seq
    ON artifacts (workspace_id, request_key, created_seq DESC);

CREATE INDEX IF NOT EXISTS idx_artifacts_created_seq_desc
    ON artifacts (workspace_id, created_seq DESC);

CREATE INDEX IF NOT EXISTS idx_artifacts_expires_active
    ON artifacts (workspace_id, expires_at)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_session_id
    ON artifacts (workspace_id, session_id);

CREATE INDEX IF NOT EXISTS idx_artifacts_payload_hash
    ON artifacts (workspace_id, payload_hash_full);

CREATE TABLE IF NOT EXISTS artifact_roots (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_key TEXT NOT NULL,
    root_path TEXT NOT NULL,
    count_estimate INTEGER NULL CHECK (count_estimate IS NULL OR count_estimate >= 0),
    inventory_coverage REAL NULL,
    root_summary JSON NULL,
    root_score REAL NULL,
    root_shape TEXT NULL,
    fields_top JSON NULL,
    examples JSON NULL,
    recipes JSON NULL,
    sample_indices JSON NULL,
    PRIMARY KEY (workspace_id, artifact_id, root_key),
    FOREIGN KEY (workspace_id, artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_samples (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_key TEXT NOT NULL,
    root_path TEXT NOT NULL,
    sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
    record JSON NOT NULL,
    record_bytes INTEGER NOT NULL CHECK (record_bytes >= 0),
    record_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, artifact_id, root_key, sample_index),
    FOREIGN KEY (workspace_id, artifact_id, root_key)
        REFERENCES artifact_roots (workspace_id, artifact_id, root_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifact_samples_root_path
    ON artifact_samples (workspace_id, artifact_id, root_path);
