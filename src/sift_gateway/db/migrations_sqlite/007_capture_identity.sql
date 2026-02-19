-- 007_capture_identity.sql: protocol-neutral capture identity columns.
ALTER TABLE artifacts
    ADD COLUMN capture_kind TEXT NULL;

ALTER TABLE artifacts
    ADD COLUMN capture_origin JSON NULL;

ALTER TABLE artifacts
    ADD COLUMN capture_key TEXT NULL;

-- Backfill from MCP-shaped identity fields for existing rows.
UPDATE artifacts
SET capture_kind = CASE
    WHEN kind = 'derived_query' THEN 'derived_query'
    WHEN kind = 'derived_codegen' THEN 'derived_codegen'
    ELSE 'mcp_tool'
END
WHERE capture_kind IS NULL;

UPDATE artifacts
SET capture_key = request_key
WHERE capture_key IS NULL;

UPDATE artifacts
SET capture_origin = json_object(
    'prefix',
    CASE
        WHEN instr(source_tool, '.') > 0
            THEN substr(source_tool, 1, instr(source_tool, '.') - 1)
        ELSE source_tool
    END,
    'tool',
    CASE
        WHEN instr(source_tool, '.') > 0
            THEN substr(source_tool, instr(source_tool, '.') + 1)
        ELSE source_tool
    END,
    'upstream_instance_id',
    upstream_instance_id
)
WHERE capture_origin IS NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_capture_kind_created_seq
    ON artifacts (workspace_id, capture_kind, created_seq DESC)
    WHERE capture_kind IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_capture_key_created_seq
    ON artifacts (workspace_id, capture_key, created_seq DESC)
    WHERE capture_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_artifacts_capture_kind_last_seen
    ON artifacts (workspace_id, capture_kind, last_referenced_at DESC)
    WHERE capture_kind IS NOT NULL;
