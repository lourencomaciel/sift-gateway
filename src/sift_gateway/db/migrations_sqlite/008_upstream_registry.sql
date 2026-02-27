-- 008_upstream_registry.sql: canonical upstream registry.

CREATE TABLE IF NOT EXISTS upstream_registry (
    workspace_id TEXT NOT NULL,
    prefix TEXT NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('stdio', 'http')),
    command TEXT NULL,
    args_json TEXT NOT NULL DEFAULT '[]',
    url TEXT NULL,
    pagination_json TEXT NULL,
    auto_paginate_max_pages INTEGER NULL CHECK (
        auto_paginate_max_pages IS NULL OR auto_paginate_max_pages >= 0
    ),
    auto_paginate_max_records INTEGER NULL CHECK (
        auto_paginate_max_records IS NULL OR auto_paginate_max_records >= 0
    ),
    auto_paginate_timeout_seconds REAL NULL CHECK (
        auto_paginate_timeout_seconds IS NULL
        OR auto_paginate_timeout_seconds > 0
    ),
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
