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
git clone https://github.com/lourencomaciel/sift-mcp.git
cd sift-mcp
uv sync
```

See [CONTRIBUTING.md](../CONTRIBUTING.md) for full development guide.

## OpenClaw Users

If your main goal is preventing context overflow in OpenClaw, start with the
dedicated pack:

- [OpenClaw Integration Pack](openclaw/README.md)
- [Installable skill file](openclaw/SKILL.md)

## Importing Your MCP Configuration

Sift can import your existing MCP server configuration from Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, or Zed.

### Basic Import

```bash
sift-mcp init \
  --from claude
```

`--from` accepts either an explicit path or one of these shortcuts:
`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`.

This command:

1. Copies your `mcpServers` configuration into `{data_dir}/state/config.json` (default data dir: `.sift-mcp`)
2. Configures the SQLite database backend (no setup required)
3. Externalizes inline `env` and `headers` into per-upstream secret files under `{data_dir}/state/upstream_secrets/`
4. Creates a backup of your original config at `<source>.backup`
5. Rewrites the source config to point to Sift only
6. Stores `_gateway_sync` metadata for automatic future syncing

### Preview Changes (Dry Run)

To see what changes will be made without applying them:

```bash
sift-mcp init \
  --from claude \
  --dry-run
```

### Revert Changes

If you need to undo the import and restore your original config:

```bash
sift-mcp init \
  --from claude \
  --revert
```

This restores the `.backup` file.

## Code Query Packages (Optional)

Code queries (`query_kind="code"`) can use pandas, NumPy, jmespath, and other
libraries. These are not included in the base install to keep Sift lightweight.

### Install the code bundle

```bash
# Using pipx
pipx install "sift-mcp[code]"

# Using uv
uv tool install "sift-mcp[code]"
```

Backward-compatible alias:

```bash
pipx install "sift-mcp[data-science]"
```

### Install individual packages

You can install any pip package into Sift's environment:

```bash
sift-mcp install pandas scipy matplotlib
```

This installs the package and updates the instance allowlist so the import
is permitted in code queries. To remove:

```bash
sift-mcp uninstall scipy
```

> **Note:** Sift runs in an isolated Python environment. Packages installed
> in your system Python are not available to code queries — use
> `sift-mcp install` instead of `pip install`.

## Adding MCP Servers After Initial Setup

After running `init`, your source config file contains only the Sift gateway entry. To add a new upstream MCP server:

1. **Edit the source config** and add the new server entry alongside the gateway entry:

```json
{
  "mcpServers": {
    "artifact-gateway": {
      "command": "sift-mcp",
      "args": ["--data-dir", "/absolute/path/to/.sift-mcp"]
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

You can also add servers directly to the selected instance:

```bash
sift-mcp upstream add '{"new-server":{"command":"npx","args":["-y","@modelcontextprotocol/server-example"]}}' --from claude
```

Optional targeting overrides:
- `sift-mcp upstream add '<json>' --from claude --data-dir /abs/path/to/data-dir`

## Manual Configuration

You can also manually configure Sift by creating `{data_dir}/state/config.json`:

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "_gateway": {
        "secret_ref": "github",
        "semantic_salt_env_keys": ["GITHUB_ORG"]
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

Secrets are stored externally in the same instance under `state/upstream_secrets/<prefix>.json` (with 0600 permissions). Use `_gateway.secret_ref` to reference them instead of placing credentials inline.

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

1. **Restart your MCP client** (Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, or Zed)

2. **Call an upstream tool** through Sift

3. **Receive either raw response or an artifact handle**:

- small responses may be returned directly
- larger responses (or continuation-required responses) return an artifact
  handle
- every mirrored response is still persisted as an artifact

When a handle is returned:

```json
{
  "artifact_id": "art_7f3a...",
  "schemas": [...],
  "usage_hint": "Use artifact(action='query', query_kind='select', ...) to retrieve data",
  "pagination": {...}
}
```

4. **Query the artifact (handle path):**

If your mirrored call returned raw payload, use `query_kind="search"` to find
the persisted artifact for your session or set
`SIFT_MCP_PASSTHROUGH_MAX_BYTES=0` to force handle responses.

```python
# Get metadata
artifact(action="query", query_kind="describe", artifact_id="art_7f3a...")

# Fetch full response
artifact(action="query", query_kind="get", artifact_id="art_7f3a...")

# Select specific data
artifact(
    action="query",
    query_kind="select",
    artifact_id="art_7f3a...",
    root_path="$.items",
    select_paths=["id", "name"],
    limit=100,
)
```

See [Recipes & Examples](recipes.md) for more usage patterns.

## Troubleshooting

### Sift isn't starting

- Check `sift-mcp --check` for configuration errors
- Verify Python version: `python --version` (requires >= 3.11)
- Inspect stderr output from your MCP client process for startup/runtime errors

### Upstream tools not working

- Verify upstream servers are configured correctly in your original MCP config
- Check `{data_dir}/state/upstream_secrets/` for externalized secrets
- Test upstream directly (temporarily remove Sift from config)

### Artifacts not being created

- Check `sift-mcp --check` for DB/FS health errors
- Verify upstream calls succeed and return content
- See [Configuration Reference](config.md) for artifact and mapping budgets

## Next Steps

- **[Recipes & Examples](recipes.md)** — Learn common usage patterns
- **[Configuration Reference](config.md)** — Customize Sift's behavior
- **[Deployment Guide](deployment.md)** — Run Sift in production
- **[Architecture](architecture.md)** — Understand how Sift works internally
