-- 001_init.sql
-- MCP Artifact Gateway v1.9 — initial schema.
-- All tables use workspace_id TEXT NOT NULL DEFAULT 'local' for single-tenant operation.

-- ===================================================================
-- sessions
-- ===================================================================
CREATE TABLE IF NOT EXISTS sessions (
    workspace_id   text        NOT NULL DEFAULT 'local',
    session_id     text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_last_seen
    ON sessions (workspace_id, last_seen_at DESC);

-- ===================================================================
-- binary_blobs
-- ===================================================================
CREATE TABLE IF NOT EXISTS binary_blobs (
    workspace_id       text        NOT NULL DEFAULT 'local',
    binary_hash        text        NOT NULL,
    blob_id            text        NOT NULL,
    byte_count         bigint      NOT NULL,
    mime               text            NULL,
    fs_path            text        NOT NULL,
    probe_head_hash    text            NULL,
    probe_tail_hash    text            NULL,
    probe_bytes        int             NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, binary_hash),
    UNIQUE (workspace_id, blob_id)
);

CREATE INDEX IF NOT EXISTS idx_binary_blobs_created
    ON binary_blobs (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_binary_blobs_byte_count
    ON binary_blobs (workspace_id, byte_count);

-- ===================================================================
-- payload_blobs
-- ===================================================================
CREATE TABLE IF NOT EXISTS payload_blobs (
    workspace_id                  text        NOT NULL DEFAULT 'local',
    payload_hash_full             text        NOT NULL,
    envelope                      jsonb       NOT NULL,
    envelope_canonical_encoding   text        NOT NULL
        CHECK (envelope_canonical_encoding IN ('zstd', 'gzip', 'none')),
    envelope_canonical_bytes      bytea       NOT NULL,
    envelope_canonical_bytes_len  int         NOT NULL
        CHECK (envelope_canonical_bytes_len >= 0),
    canonicalizer_version         text        NOT NULL,
    payload_json_bytes            int         NOT NULL
        CHECK (payload_json_bytes >= 0),
    payload_binary_bytes_total    bigint      NOT NULL
        CHECK (payload_binary_bytes_total >= 0),
    payload_total_bytes           bigint      NOT NULL
        CHECK (payload_total_bytes >= 0),
    contains_binary_refs          boolean     NOT NULL,
    created_at                    timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, payload_hash_full)
);

CREATE INDEX IF NOT EXISTS idx_payload_blobs_created
    ON payload_blobs (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_payload_blobs_total_bytes
    ON payload_blobs (workspace_id, payload_total_bytes);

-- ===================================================================
-- payload_hash_aliases
-- ===================================================================
CREATE TABLE IF NOT EXISTS payload_hash_aliases (
    workspace_id          text        NOT NULL DEFAULT 'local',
    payload_hash_dedupe   text        NOT NULL,
    payload_hash_full     text        NOT NULL,
    upstream_instance_id  text        NOT NULL,
    tool                  text        NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, payload_hash_dedupe, payload_hash_full),

    CONSTRAINT fk_pha_payload_blob
        FOREIGN KEY (workspace_id, payload_hash_full)
        REFERENCES payload_blobs (workspace_id, payload_hash_full)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pha_dedupe_created
    ON payload_hash_aliases (workspace_id, payload_hash_dedupe, created_at DESC);

-- ===================================================================
-- payload_binary_refs
-- ===================================================================
CREATE TABLE IF NOT EXISTS payload_binary_refs (
    workspace_id       text        NOT NULL DEFAULT 'local',
    payload_hash_full  text        NOT NULL,
    binary_hash        text        NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, payload_hash_full, binary_hash),

    CONSTRAINT fk_pbr_payload_blob
        FOREIGN KEY (workspace_id, payload_hash_full)
        REFERENCES payload_blobs (workspace_id, payload_hash_full)
        ON DELETE CASCADE,

    CONSTRAINT fk_pbr_binary_blob
        FOREIGN KEY (workspace_id, binary_hash)
        REFERENCES binary_blobs (workspace_id, binary_hash)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pbr_binary_hash
    ON payload_binary_refs (workspace_id, binary_hash);

CREATE INDEX IF NOT EXISTS idx_pbr_created
    ON payload_binary_refs (workspace_id, created_at DESC);

-- ===================================================================
-- artifacts
-- ===================================================================
CREATE TABLE IF NOT EXISTS artifacts (
    workspace_id              text        NOT NULL DEFAULT 'local',
    artifact_id               text        NOT NULL,
    created_seq               bigint      GENERATED ALWAYS AS IDENTITY,
    session_id                text        NOT NULL,
    source_tool               text        NOT NULL,
    upstream_instance_id      text        NOT NULL,
    upstream_tool_schema_hash text            NULL,
    request_key               text        NOT NULL,
    request_args_hash         text        NOT NULL,
    request_args_prefix       text        NOT NULL,
    payload_hash_full         text        NOT NULL,
    canonicalizer_version     text        NOT NULL,
    payload_json_bytes        int         NOT NULL
        CHECK (payload_json_bytes >= 0),
    payload_binary_bytes_total bigint     NOT NULL
        CHECK (payload_binary_bytes_total >= 0),
    payload_total_bytes       bigint      NOT NULL
        CHECK (payload_total_bytes >= 0),
    created_at                timestamptz NOT NULL DEFAULT now(),
    expires_at                timestamptz     NULL,
    deleted_at                timestamptz     NULL,
    last_referenced_at        timestamptz NOT NULL DEFAULT now(),
    generation                int         NOT NULL DEFAULT 1
        CHECK (generation >= 1),
    parent_artifact_id        text            NULL,
    chain_seq                 int             NULL,
    map_kind                  text        NOT NULL
        CHECK (map_kind IN ('none', 'full', 'partial')),
    map_status                text        NOT NULL
        CHECK (map_status IN ('pending', 'ready', 'failed', 'stale')),
    mapped_part_index         int             NULL,
    mapper_version            text        NOT NULL,
    map_budget_fingerprint    text            NULL,
    map_backend_id            text            NULL,
    prng_version              text            NULL,
    map_error                 jsonb           NULL,
    index_status              text        NOT NULL
        CHECK (index_status IN ('off', 'pending', 'ready', 'partial', 'failed')),
    error_summary             text            NULL,

    PRIMARY KEY (workspace_id, artifact_id),

    CONSTRAINT fk_art_session
        FOREIGN KEY (workspace_id, session_id)
        REFERENCES sessions (workspace_id, session_id)
        ON DELETE RESTRICT,

    CONSTRAINT fk_art_payload
        FOREIGN KEY (workspace_id, payload_hash_full)
        REFERENCES payload_blobs (workspace_id, payload_hash_full)
        ON DELETE RESTRICT,

    CONSTRAINT fk_art_parent
        FOREIGN KEY (workspace_id, parent_artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE SET NULL
);

-- Partial unique index for chain ordering within a parent.
CREATE UNIQUE INDEX IF NOT EXISTS uq_art_parent_chain_seq
    ON artifacts (workspace_id, parent_artifact_id, chain_seq)
    WHERE chain_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_art_session_seq
    ON artifacts (workspace_id, session_id, created_seq DESC);

CREATE INDEX IF NOT EXISTS idx_art_request_key_seq
    ON artifacts (workspace_id, request_key, created_seq DESC);

CREATE INDEX IF NOT EXISTS idx_art_created_seq
    ON artifacts (workspace_id, created_seq DESC);

CREATE INDEX IF NOT EXISTS idx_art_expires
    ON artifacts (workspace_id, expires_at)
    WHERE deleted_at IS NULL AND expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_art_deleted
    ON artifacts (workspace_id, deleted_at)
    WHERE deleted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_art_last_referenced
    ON artifacts (workspace_id, last_referenced_at);

CREATE INDEX IF NOT EXISTS idx_art_parent_seq
    ON artifacts (workspace_id, parent_artifact_id, created_seq DESC);

-- ===================================================================
-- artifact_refs
-- ===================================================================
CREATE TABLE IF NOT EXISTS artifact_refs (
    workspace_id   text        NOT NULL DEFAULT 'local',
    session_id     text        NOT NULL,
    artifact_id    text        NOT NULL,
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, session_id, artifact_id),

    CONSTRAINT fk_aref_session
        FOREIGN KEY (workspace_id, session_id)
        REFERENCES sessions (workspace_id, session_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_aref_artifact
        FOREIGN KEY (workspace_id, artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aref_session_last_seen
    ON artifact_refs (workspace_id, session_id, last_seen_at DESC);

-- ===================================================================
-- artifact_roots
-- ===================================================================
CREATE TABLE IF NOT EXISTS artifact_roots (
    workspace_id          text             NOT NULL DEFAULT 'local',
    artifact_id           text             NOT NULL,
    root_key              text             NOT NULL,
    root_path             text             NOT NULL,
    count_estimate        int                  NULL,
    inventory_coverage    double precision NOT NULL
        CHECK (inventory_coverage >= 0.0 AND inventory_coverage <= 1.0),
    root_summary          text             NOT NULL,
    root_score            double precision NOT NULL,
    root_shape            jsonb            NOT NULL,
    fields_top            jsonb            NOT NULL,
    examples              jsonb            NOT NULL,
    recipes               jsonb            NOT NULL,
    sample_indices        jsonb            NOT NULL,

    PRIMARY KEY (workspace_id, artifact_id, root_key),

    CONSTRAINT fk_aroot_artifact
        FOREIGN KEY (workspace_id, artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aroot_artifact
    ON artifact_roots (workspace_id, artifact_id);

CREATE INDEX IF NOT EXISTS idx_aroot_path
    ON artifact_roots (workspace_id, root_path);

-- ===================================================================
-- artifact_samples
-- ===================================================================
CREATE TABLE IF NOT EXISTS artifact_samples (
    workspace_id   text        NOT NULL DEFAULT 'local',
    artifact_id    text        NOT NULL,
    root_key       text        NOT NULL,
    root_path      text        NOT NULL,
    sample_index   int         NOT NULL,
    record         jsonb       NOT NULL,
    record_bytes   int         NOT NULL
        CHECK (record_bytes >= 0),
    record_hash    text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (workspace_id, artifact_id, root_key, sample_index),

    CONSTRAINT fk_asample_root
        FOREIGN KEY (workspace_id, artifact_id, root_key)
        REFERENCES artifact_roots (workspace_id, artifact_id, root_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_asample_root_key
    ON artifact_samples (workspace_id, artifact_id, root_key);

CREATE INDEX IF NOT EXISTS idx_asample_root_path
    ON artifact_samples (workspace_id, artifact_id, root_path);
