-- 004_schema_fields_example_value.sql: ensure schema field example column exists.

ALTER TABLE artifact_schema_fields
    ADD COLUMN IF NOT EXISTS example_value TEXT NULL;
