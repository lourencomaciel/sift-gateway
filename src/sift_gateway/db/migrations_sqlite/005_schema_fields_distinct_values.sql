-- 005_schema_fields_distinct_values.sql: add sampled enum metadata columns.
--
-- Recreate artifact_schema_fields with distinct_values/cardinality so
-- existing SQLite databases are upgraded consistently.

CREATE TABLE IF NOT EXISTS artifact_schema_fields_v3 (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_key TEXT NOT NULL,
    field_path TEXT NOT NULL,
    types JSON NOT NULL,
    nullable INTEGER NOT NULL,
    required INTEGER NOT NULL,
    observed_count INTEGER NOT NULL CHECK (observed_count >= 0),
    example_value TEXT NULL,
    distinct_values JSON NULL,
    cardinality INTEGER NULL CHECK (cardinality >= 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, artifact_id, root_key, field_path),
    FOREIGN KEY (workspace_id, artifact_id, root_key)
        REFERENCES artifact_schema_roots (workspace_id, artifact_id, root_key)
        ON DELETE CASCADE
);

INSERT INTO artifact_schema_fields_v3 (
    workspace_id,
    artifact_id,
    root_key,
    field_path,
    types,
    nullable,
    required,
    observed_count,
    example_value,
    distinct_values,
    cardinality
)
SELECT
    workspace_id,
    artifact_id,
    root_key,
    field_path,
    types,
    nullable,
    required,
    observed_count,
    example_value,
    NULL,
    NULL
FROM artifact_schema_fields;

DROP TABLE artifact_schema_fields;

ALTER TABLE artifact_schema_fields_v3
    RENAME TO artifact_schema_fields;
