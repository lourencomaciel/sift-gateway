-- 004_schema_fields_example_value.sql: normalize schema fields table shape.
--
-- SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
-- Recreate artifact_schema_fields in-place with the expected v1 shape,
-- preserving existing rows and initializing example_value to NULL.

CREATE TABLE IF NOT EXISTS artifact_schema_fields_v2 (
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

INSERT INTO artifact_schema_fields_v2 (
    workspace_id,
    artifact_id,
    root_key,
    field_path,
    types,
    nullable,
    required,
    observed_count,
    example_value
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
    NULL
FROM artifact_schema_fields;

DROP TABLE artifact_schema_fields;

ALTER TABLE artifact_schema_fields_v2
    RENAME TO artifact_schema_fields;
