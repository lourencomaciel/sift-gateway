-- 005_schema_fields_distinct_values.sql: add sampled enum metadata columns.

ALTER TABLE artifact_schema_fields
    ADD COLUMN IF NOT EXISTS distinct_values JSONB NULL;

ALTER TABLE artifact_schema_fields
    ADD COLUMN IF NOT EXISTS cardinality BIGINT NULL CHECK (cardinality >= 0);
