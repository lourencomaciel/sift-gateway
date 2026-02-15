-- 003_schema_v1.sql: SQLite schema persistence for deterministic schemas.

CREATE TABLE IF NOT EXISTS artifact_schema_roots (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_key TEXT NOT NULL,
    root_path TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('exact', 'sampled')),
    completeness TEXT NOT NULL CHECK (
        completeness IN ('complete', 'partial')
    ),
    observed_records INTEGER NOT NULL CHECK (observed_records >= 0),
    dataset_hash TEXT NOT NULL,
    traversal_contract_version TEXT NOT NULL,
    map_budget_fingerprint TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, artifact_id, root_key),
    FOREIGN KEY (workspace_id, artifact_id, root_key)
        REFERENCES artifact_roots (workspace_id, artifact_id, root_key)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_schema_fields (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_key TEXT NOT NULL,
    field_path TEXT NOT NULL,
    types JSON NOT NULL,
    nullable INTEGER NOT NULL,
    required INTEGER NOT NULL,
    observed_count INTEGER NOT NULL CHECK (observed_count >= 0),
    example_value TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, artifact_id, root_key, field_path),
    FOREIGN KEY (workspace_id, artifact_id, root_key)
        REFERENCES artifact_schema_roots (workspace_id, artifact_id, root_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifact_schema_roots_root_path
    ON artifact_schema_roots (workspace_id, artifact_id, root_path);
