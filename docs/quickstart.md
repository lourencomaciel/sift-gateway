# Quick Start Guide

Complete walkthrough for installing and configuring Sift MCP.

## Installation

### Using pipx (Recommended)

```bash
pipx install sift-mcp
```

To upgrade:

```bash
pipx upgrade sift-mcp
```

### Using uv

```bash
uv tool install sift-mcp
```

To upgrade:

```bash
uv tool upgrade sift-mcp
```

### Development Setup

If you're contributing to Sift or want to run from source:

```bash
git clone https://github.com/zmaciel/sift-mcp.git
cd sift-mcp
uv sync
```

See [CONTRIBUTING.md](../CONTRIBUTING.md) for full development guide.

## Importing Your MCP Configuration

Sift can import your existing MCP server configuration from Claude Desktop, Cursor, or Claude Code.

### Basic Import (SQLite)

```bash
sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

This command:

1. Copies your `mcpServers` configuration into `.sift-mcp/state/config.json`
2. Sets `db_backend` to `sqlite` (default, no setup required)
3. Externalizes inline `env` and `headers` into per-upstream secret files under `.sift-mcp/state/upstream_secrets/`
4. Creates a backup of your original config at `<source>.backup`
5. Rewrites the source config to point to Sift only
6. Stores `_gateway_sync` metadata for automatic future syncing

### Preview Changes (Dry Run)

To see what changes will be made without applying them:

```bash
sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --dry-run
```

### Revert Changes

If you need to undo the import and restore your original config:

```bash
sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --revert
```

This restores the `.backup` file.

## PostgreSQL Setup (Optional)

For production deployments or when you need concurrent multi-process access, use PostgreSQL instead of SQLite.

### Install with PostgreSQL Support

```bash
# Using pipx
pipx install "sift-mcp[postgres]"

# Using uv
uv tool install "sift-mcp[postgres]"

# From source
uv sync --extra postgres
```

### Start PostgreSQL with Docker

The project includes a `docker-compose.yml` that provisions two databases:

- `sift` — Application runtime database
- `sift_test` — Integration test database (created by `scripts/init-test-db.sql`)

```bash
docker compose up -d
```

If the container existed before the init script mount, recreate it:

```bash
docker compose down -v && docker compose up -d
```

### Import with PostgreSQL Backend

```bash
sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --db-backend postgres
```

When `--postgres-dsn` is not provided, Sift resolves the DSN in this order:

1. `SIFT_MCP_POSTGRES_DSN` environment variable
2. Existing `postgres_dsn` in `.sift-mcp/state/config.json`
3. Auto-provisioned local Docker Postgres DSN

The resolved DSN is written to `.sift-mcp/state/config.json`.

### Custom PostgreSQL DSN

To use an existing PostgreSQL instance:

```bash
sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --db-backend postgres \
  --postgres-dsn postgresql://user:pass@host:5432/sift
```

## Adding MCP Servers After Initial Setup

After running `init`, your source config file contains only the Sift gateway entry. To add a new upstream MCP server:

1. **Edit the source config** and add the new server entry alongside the gateway entry:

```json
{
  "mcpServers": {
    "sift-gateway": {
      "command": "sift-mcp",
      "args": []
    },
    "new-server": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-example"]
    }
  }
}
```

2. **Restart Sift**

On startup, Sift reads the `_gateway_sync` metadata, detects new non-gateway entries in the source file, imports them (including secret externalization), and rewrites the source file back to gateway-only.

**No manual re-init is needed.**

## Manual Configuration

You can also manually configure Sift by creating `.sift-mcp/state/config.json`:

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "_gateway": {
        "secret_ref": "github",
        "semantic_salt_env_keys": ["GITHUB_ORG"],
        "strict_schema_reuse": true,
        "passthrough_allowed": true,
        "dedupe_exclusions": ["$.timestamp"]
      }
    },
    "remote": {
      "url": "https://example.com/mcp",
      "_gateway": {
        "secret_ref": "remote"
      }
    }
  }
}
```

Secrets are stored externally in `.sift-mcp/state/upstream_secrets/<prefix>.json` (with 0600 permissions). Use `_gateway.secret_ref` to reference them instead of placing credentials inline.

See [Configuration Reference](config.md) for all available settings.

### Transport Inference

Sift automatically infers the transport protocol:

- `command` present → `stdio` transport
- `url` present → `http` transport

### Environment Isolation

By default, stdio upstream processes receive only a minimal set of environment variables:

- `PATH`, `HOME`, `LANG`, `LC_ALL`
- `TMPDIR`, `TMP`, `TEMP`
- `USER`, `LOGNAME`, `SHELL`

Explicit `env` from the config or secret file is merged on top.

To pass the full parent process environment to a specific upstream:

```json
{
  "mcpServers": {
    "my-server": {
      "command": "...",
      "_gateway": {
        "inherit_parent_env": true
      }
    }
  }
}
```

## Verifying Setup

Check Sift's health and configuration at any time:

```bash
sift-mcp --check
```

This validates:

- Configuration file syntax
- Database connectivity
- Filesystem permissions
- Upstream server availability

The command exits with status `0` if all checks pass.

## Your First Artifact

After setup:

1. **Restart your MCP client** (Claude Desktop, Cursor, or Claude Code)

2. **Call an upstream tool** that returns a large response (> 8 KB default)

3. **Receive an artifact handle** instead of the full response:

```json
{
  "artifact_id": "art_7f3a...",
  "schemas": [...],
  "usage_hint": "Use artifact(action='query', query_kind='select', ...) to retrieve data",
  "pagination": {...}
}
```

4. **Query the artifact:**

```python
# Get metadata
artifact(action="query", query_kind="describe", artifact_id="art_7f3a...")

# Fetch full response
artifact(action="query", query_kind="get", artifact_id="art_7f3a...")

# Select specific data
artifact(action="query", query_kind="select", artifact_id="art_7f3a...", root_path="$.items", limit=100)
```

See [Recipes & Examples](recipes.md) for more usage patterns.

## Troubleshooting

### Sift isn't starting

- Check `sift-mcp --check` for configuration errors
- Verify Python version: `python --version` (requires >= 3.11)
- Inspect stderr output from your MCP client process for startup/runtime errors

### Upstream tools not working

- Verify upstream servers are configured correctly in your original MCP config
- Check `.sift-mcp/state/upstream_secrets/` for externalized secrets
- Test upstream directly (temporarily remove Sift from config)

### PostgreSQL connection errors

- Verify Docker container is running: `docker ps`
- Check DSN format: `postgresql://user:pass@host:port/database`
- Test with the same DSN Sift is using: `psql "$SIFT_MCP_POSTGRES_DSN"` (or copy `postgres_dsn` from `.sift-mcp/state/config.json`)

### Artifacts not being created

- Check passthrough threshold: default is 8 KB (`passthrough_max_bytes`)
- Verify response size exceeds threshold
- See [Configuration Reference](config.md) to adjust threshold

## Next Steps

- **[Recipes & Examples](recipes.md)** — Learn common usage patterns
- **[Configuration Reference](config.md)** — Customize Sift's behavior
- **[Deployment Guide](deployment.md)** — Run Sift in production
- **[Architecture & Spec](spec_v1_9.md)** — Understand how Sift works internally
