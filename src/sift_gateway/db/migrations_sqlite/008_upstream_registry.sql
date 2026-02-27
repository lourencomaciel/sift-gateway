-- 008_upstream_registry.sql: canonical upstream registry and admin state.

CREATE TABLE IF NOT EXISTS upstream_registry (
    workspace_id TEXT NOT NULL,
    prefix TEXT NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('stdio', 'http')),
    command TEXT NULL,
    args_json TEXT NOT NULL DEFAULT '[]',
    url TEXT NULL,
    pagination_json TEXT NULL,
    passthrough_allowed INTEGER NOT NULL DEFAULT 1 CHECK (
        passthrough_allowed IN (0, 1)
    ),
    semantic_salt_env_keys_json TEXT NOT NULL DEFAULT '[]',
    semantic_salt_headers_json TEXT NOT NULL DEFAULT '[]',
    inherit_parent_env INTEGER NOT NULL DEFAULT 0 CHECK (
        inherit_parent_env IN (0, 1)
    ),
    external_user_id TEXT NULL,
    secret_ref TEXT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    source_kind TEXT NOT NULL DEFAULT 'manual' CHECK (
        source_kind IN ('manual', 'init_sync', 'snippet_add')
    ),
    source_ref TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, prefix),
    CHECK (
        (transport = 'stdio'
         AND command IS NOT NULL
         AND command <> ''
         AND url IS NULL)
        OR
        (transport = 'http'
         AND url IS NOT NULL
         AND url <> ''
         AND command IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_upstream_registry_enabled
    ON upstream_registry (workspace_id, enabled, prefix);

CREATE TABLE IF NOT EXISTS upstream_runtime_state (
    workspace_id TEXT NOT NULL,
    prefix TEXT NOT NULL,
    last_probe_at TEXT NULL,
    last_probe_ok INTEGER NULL CHECK (last_probe_ok IN (0, 1)),
    last_probe_error_code TEXT NULL,
    last_probe_error_message TEXT NULL,
    last_probe_tool_count INTEGER NULL CHECK (
        last_probe_tool_count IS NULL OR last_probe_tool_count >= 0
    ),
    last_success_at TEXT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, prefix),
    FOREIGN KEY (workspace_id, prefix)
        REFERENCES upstream_registry (workspace_id, prefix)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS upstream_admin_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL,
    prefix TEXT NULL,
    action TEXT NOT NULL,
    details_json TEXT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    error_code TEXT NULL,
    error_message TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_upstream_admin_events_created
    ON upstream_admin_events (workspace_id, created_at DESC, id DESC);
