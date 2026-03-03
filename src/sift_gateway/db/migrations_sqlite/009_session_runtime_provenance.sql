-- 009_session_runtime_provenance.sql: record last runtime identity on sessions.
ALTER TABLE sessions
    ADD COLUMN last_runtime_pid INTEGER NULL;

ALTER TABLE sessions
    ADD COLUMN last_runtime_instance_uuid TEXT NULL;
