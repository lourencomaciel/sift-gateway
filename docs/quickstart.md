# Quick Start Guide

Complete walkthrough for installing and configuring Sift Gateway.

## Installation

### Using pipx (recommended)

```bash
pipx install sift-gateway
```

To upgrade:

```bash
pipx upgrade sift-gateway
```

### Using uv

```bash
uv tool install sift-gateway
```

To upgrade:

```bash
uv tool upgrade sift-gateway
```

### Development setup

```bash
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync
```

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the full development workflow.

## OpenClaw users

If your main goal is preventing context overflow in OpenClaw, start with:

- [OpenClaw Integration Pack](openclaw/README.md)
- [Installable skill file](openclaw/SKILL.md)

## Importing your MCP configuration

Sift can import existing MCP server configuration from Claude Desktop, Claude
Code, Cursor, VS Code, Windsurf, or Zed.

### Basic import

```bash
sift-gateway init --from claude
```

`--from` accepts either an explicit path or one of:
`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`.

This command:

1. Copies your MCP config into `{data_dir}/state/config.json`.
2. Sets up SQLite storage.
3. Externalizes inline secrets into `{data_dir}/state/upstream_secrets/`.
4. Creates a backup at `<source>.backup`.
5. Rewrites source config to point to Sift.
6. Stores `_gateway_sync` metadata for ongoing sync.

### Dry run

```bash
sift-gateway init --from claude --dry-run
```

### Revert

```bash
sift-gateway init --from claude --revert
```

## Code query packages (optional)

Code queries (`query_kind="code"`, `sift-gateway code`) can use pandas,
NumPy, jmespath, and other libraries when those packages are installed in
Sift's runtime environment.

### Install bundle

```bash
# pipx
pipx install "sift-gateway[code]"

# uv
uv tool install "sift-gateway[code]"
```

Compatibility alias:

```bash
pipx install "sift-gateway[data-science]"
```

### Install individual packages

```bash
sift-gateway install pandas scipy matplotlib
```

Remove packages:

```bash
sift-gateway uninstall scipy
```

## Verifying setup

```bash
sift-gateway --check
```

This validates config, DB connectivity, filesystem permissions, and upstream
availability.

## Your first artifact (CLI)

Capture data:

```bash
sift-gateway run -- echo '[{"id":1,"name":"a"},{"id":2,"name":"b"}]'
```

Run analysis:

```bash
sift-gateway code <artifact_id> '$' --code "def run(data, schema, params): return len(data)"
```

If pagination is present (`pagination.next.kind=="command"`), continue with:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next-params-applied>
```

## Your first artifact (MCP)

1. Restart your MCP client.
2. Call an upstream mirrored tool through Sift.
3. Receive a response with:
   - `response_mode="full"` with inline `payload`, or
   - `response_mode="schema_ref"` with `artifact_id` and compact schema.
4. If pagination is partial, continue with:

```python
artifact(action="next_page", artifact_id="art_...")
```

Run code query over an artifact:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_...",
    root_path="$.items",
    code="def run(data, schema, params): return {'rows': len(data)}",
)
```

## Adding MCP servers after initial setup

After `init`, source config usually contains only the Sift gateway entry. To add
new upstream servers:

1. Edit source config and add the upstream entry.
2. Restart Sift.

Sift detects the new server, imports it, externalizes secrets, and rewrites
source config back to gateway-only.

You can also add servers directly:

```bash
sift-gateway upstream add '{"new-server":{"command":"npx","args":["-y","@modelcontextprotocol/server-example"]}}' --from claude
```

Optional target override:

```bash
sift-gateway upstream add '<json>' --from claude --data-dir /abs/path/to/data-dir
```

## Manual configuration

You can manually create `{data_dir}/state/config.json`:

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
    }
  }
}
```

Secrets should live under `state/upstream_secrets/<prefix>.json`.

## Troubleshooting

### Sift is not starting

- Run `sift-gateway --check`.
- Verify Python version (`>= 3.11`).
- Inspect stderr from your MCP client process.

### Upstream tools are failing

- Verify upstream config and secret files.
- Test upstream directly (temporarily bypass Sift).

### Artifacts seem missing

- Ensure you are using the expected `--data-dir`.
- Check TTL settings on captures.
- Verify session context in MCP calls (`_gateway_context.session_id`).

## Next steps

- [Recipes & Examples](recipes.md)
- [Configuration Reference](config.md)
- [Deployment Guide](deployment.md)
- [Architecture](architecture.md)
