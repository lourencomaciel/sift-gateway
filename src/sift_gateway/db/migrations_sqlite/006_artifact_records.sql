-- 006_artifact_records.sql: Materialized records for SQL-based filtering.
-- Full mapping: stores all extracted records per root.
-- Partial mapping: stores sampled records per root.

CREATE TABLE IF NOT EXISTS artifact_records (
    workspace_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    root_path TEXT NOT NULL,
    idx INTEGER NOT NULL CHECK (idx >= 0),
    record JSON NOT NULL,
    PRIMARY KEY (workspace_id, artifact_id, root_path, idx),
    FOREIGN KEY (workspace_id, artifact_id)
        REFERENCES artifacts (workspace_id, artifact_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifact_records_root_path
    ON artifact_records (workspace_id, artifact_id, root_path);

-- Backfill: force remap of existing ready artifacts so the new
-- artifact_records table is populated by the mapping pipeline.
UPDATE artifacts SET map_status = 'stale'
WHERE map_status = 'ready';
