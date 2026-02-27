# Upstream Registration UX and Registry Design (Draft)

This document proposes a simpler upstream registration
experience for Sift while preserving Sift's core guarantees
(artifact persistence, redaction, pagination contract, deterministic lineage).

## 1. Goals

1. Replace JSON-snippet-heavy registration with ergonomic flags.
2. Keep `state/config.json` compatibility for existing installs.
3. Use SQLite as the canonical upstream registry source.
4. Preserve secret handling via `secret_ref` file externalization.
5. Add explicit inspect/probe/admin flows for upstreams.

## 2. Non-Goals

1. No change to artifact/query contracts (`artifact(...)`, `run`, `code`).
2. No change to response-mode logic (`full` vs `schema_ref`).
3. No requirement to expose OAuth in phase 1.

## 3. Proposed CLI Surface

`sift-gateway upstream` becomes a complete admin tree.

## 3.1 Read commands

```bash
# List registered upstreams
sift-gateway upstream list
sift-gateway upstream list --json

# Show one upstream with resolved runtime details
sift-gateway upstream inspect --server github
sift-gateway upstream inspect --server github --json

# Probe tools/list connectivity
sift-gateway upstream test --server github
sift-gateway upstream test --all
```

## 3.2 Write commands

```bash
# Stdio upstream
sift-gateway upstream add \
  --name github \
  --transport stdio \
  --command npx \
  --arg -y \
  --arg @modelcontextprotocol/server-github \
  --external-user-id auto

# HTTP upstream
sift-gateway upstream add \
  --name notion \
  --transport http \
  --url https://mcp.notion.com/mcp

# Set auth/config secret material (externalized to state/upstream_secrets)
sift-gateway upstream auth set \
  --server notion \
  --header "Authorization=Bearer $NOTION_TOKEN"

sift-gateway upstream auth set \
  --server github \
  --env "GITHUB_TOKEN=$GITHUB_TOKEN"

# Remove upstream
sift-gateway upstream remove --server notion

# Disable/enable without deleting definition
sift-gateway upstream disable --server notion
sift-gateway upstream enable --server notion
```

## 3.3 Backward-compatible commands

Keep existing snippet path:

```bash
sift-gateway upstream add '{"github":{"command":"npx","args":["-y","..."]}}'
```

Compatibility behavior:

1. If positional JSON snippet is supplied, execute legacy parser path.
2. If `--name` is supplied, execute new flag-based add path.
3. If both are supplied, return `INVALID_ARGUMENT`.

## 4. SQLite Schema (Canonical Registry)

Add migration `008_upstream_registry.sql`.

```sql
CREATE TABLE IF NOT EXISTS upstream_registry (
    workspace_id TEXT NOT NULL,
    prefix TEXT NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('stdio', 'http')),
    command TEXT NULL,
    args_json TEXT NOT NULL DEFAULT '[]',
    url TEXT NULL,
    pagination_json TEXT NULL,
    passthrough_allowed INTEGER NOT NULL DEFAULT 1 CHECK (passthrough_allowed IN (0, 1)),
    semantic_salt_env_keys_json TEXT NOT NULL DEFAULT '[]',
    semantic_salt_headers_json TEXT NOT NULL DEFAULT '[]',
    inherit_parent_env INTEGER NOT NULL DEFAULT 0 CHECK (inherit_parent_env IN (0, 1)),
    external_user_id TEXT NULL,
    secret_ref TEXT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    source_kind TEXT NOT NULL DEFAULT 'manual' CHECK (source_kind IN ('manual', 'init_sync', 'snippet_add')),
    source_ref TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, prefix),
    CHECK (
        (transport = 'stdio' AND command IS NOT NULL AND command <> '' AND url IS NULL)
        OR
        (transport = 'http' AND url IS NOT NULL AND url <> '' AND command IS NULL)
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
```

Notes:

1. Secret values stay in `state/upstream_secrets/<prefix>.json`.
2. Registry stores only `secret_ref`.
3. `workspace_id` remains `local` for current single-tenant model.

## 5. Config Compatibility Model

Source of truth strategy:

1. Canonical read source becomes `upstream_registry` rows where `enabled = 1`.
2. `state/config.json` remains a compatibility mirror (`mcpServers`).
3. On startup, if registry table is empty and config has `mcpServers`,
   bootstrap-import config into registry (one time).

Write strategy for mutating commands:

1. Validate input.
2. Upsert registry rows in SQLite transaction.
3. Externalize secrets and update `secret_ref` file when needed.
4. Mirror registry snapshot back to `state/config.json`.
5. Record admin event row.

## 6. Runtime Integration

`connect_upstreams(...)` should read configs through a resolver:

1. `load_upstreams_from_registry(...)` when registry exists and non-empty.
2. Fallback to existing `state/config.json` loader when empty.

`gateway.status` should include:

1. Registry source (`registry` or `config_fallback`).
2. Last probe fields from `upstream_runtime_state`.
3. Existing runtime probe fields remain unchanged.

## 7. Rollout Plan

## Phase 1 (UX only, low risk)

1. Add new CLI commands (`list`, `inspect`, `test`, flag-based `add`, `remove`).
2. Keep current config-file persistence path.
3. No SQLite registry writes yet.

## Phase 2 (SQLite registry canonical)

1. Add migration `008_upstream_registry.sql`.
2. Add registry repository and bootstrap import.
3. Switch runtime upstream resolution to registry-first.
4. Keep config mirror writes for backward compatibility.

## Phase 3 (optional enhancements)

1. OAuth helper commands for HTTP upstreams (`upstream login`).
2. Alias/script generator (`upstream script --install`).
3. Interactive add mode (`upstream add --interactive`).

## 8. Acceptance Criteria

1. Registering a stdio upstream requires no JSON snippets.
2. Registering an HTTP upstream requires no file edits.
3. `upstream list/inspect/test` work without restarting the process.
4. Existing `init` and `upstream add <json>` workflows continue to work.
5. Secrets never persist inline in registry rows or config mirror.

## 9. Open Questions

1. Should disabled upstreams remain visible to `gateway.status` by default?
2. Should `upstream test --all` run in parallel or sequentially by default?
3. Do we want automatic rollback to config mirror if registry write fails?
