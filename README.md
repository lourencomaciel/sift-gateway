# MCP Artifact Gateway

Local single-tenant MCP proxy that keeps long MCP responses out of prompt context by storing them as durable artifacts and exposing bounded deterministic retrieval.

## Why this exists

Standard MCP tool calls are great for immediate answers, but long responses can quickly clog model context windows, push out useful conversation state, and make MCPs unusable for complex workflows.

- large tool outputs consume prompt tokens repeatedly on follow-up turns;
- tool outputs are often ephemeral and hard to revisit across sessions;
- large payloads are expensive to resend and reprocess;
- follow-up queries need deterministic pagination and bounded traversal;
- upstream errors are usually not captured as first-class artifacts.

This gateway exists to move bulky MCP output out of context, keep it durable, and make retrieval deterministic without changing upstream tool schemas.

## What the gateway does

1. Connects to configured upstream MCP servers (stdio or HTTP).
2. Mirrors each upstream tool as `{prefix}.{tool}`.
3. Intercepts each call, forwards arguments upstream, and stores the normalized response envelope.
4. Returns the result: small payloads (< `passthrough_max_bytes`, default 8 KB) are returned raw (gateway is transparent); larger payloads return an artifact handle for retrieval via query tools.
5. Exposes retrieval tools over stored artifacts with bounded response budgets and signed cursors.

Design invariants (from v1.9 spec):

- single workspace (`WORKSPACE_ID = "local"`);
- deterministic traversal/mapping/cursor behavior;
- bounded responses (items, bytes, compute, wildcard caps);
- crash-safe storage writes;
- always-store semantics (including upstream errors).

## Built-in gateway tools

- `gateway.status`
- `artifact.search`
- `artifact.get`
- `artifact.select`
- `artifact.describe`
- `artifact.find`
- `artifact.chain_pages`

## Requirements

- Python `>=3.11`
- [`uv`](https://docs.astral.sh/uv/)
- Docker (only needed for PostgreSQL backend)

## Quick start

1. Install dependencies:

```bash
uv sync
```

2. Run the gateway (uses SQLite by default — no external dependencies):

```bash
uv run mcp-gateway
```

That's it. The default SQLite backend stores data at `.mcp_gateway/state/gateway.db` and requires no setup.

### Using PostgreSQL instead

For production deployments or when you need concurrent multi-process access:

1. Install with the Postgres extra:

```bash
uv sync --extra postgres
```

2. Copy environment template and set the backend:

```bash
cp .env.example .env
# Edit .env: set MCP_GATEWAY_DB_BACKEND=postgres
```

3. Start Postgres:

```bash
docker compose up -d
```

`docker compose` provisions:

- `mcp_gateway` (app runtime DB)
- `mcp_test` (integration test DB; created by `scripts/init-test-db.sql` on first container init)

If the container already existed before the init script mount, recreate it:

```bash
docker compose down -v && docker compose up -d
```

4. Run startup checks:

```bash
uv run mcp-gateway --check
```

5. Run the gateway:

```bash
uv run mcp-gateway
```

## Configure upstream MCP servers

The gateway accepts the standard `mcpServers` format used by Claude Desktop, Cursor, and Claude Code.

### Migrate an existing config

```bash
uv run mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

This command:

1. copies source `mcpServers` into `.mcp_gateway/state/config.json`;
2. writes a backup to `<source>.backup`;
3. rewrites the source config to point to the gateway only.

Preview:

```bash
uv run mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json --dry-run
```

Revert:

```bash
uv run mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json --revert
```

### Manual config (`mcpServers`)

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": { "GITHUB_TOKEN": "..." },
      "_gateway": {
        "semantic_salt_env_keys": ["GITHUB_ORG"],
        "strict_schema_reuse": true,
        "passthrough_allowed": true,
        "dedupe_exclusions": ["$.timestamp"]
      }
    },
    "remote": {
      "url": "https://example.com/mcp",
      "headers": { "Authorization": "Bearer ..." }
    }
  }
}
```

Transport inference:

- `command` present -> `stdio`
- `url` present -> `http`

Notes:

- `upstreams` (array format) is still supported for compatibility.
- You cannot mix `mcpServers` and `upstreams` in the same config file.

## Configuration model

Precedence:

1. `MCP_GATEWAY_*` environment variables
2. `DATA_DIR/state/config.json`
3. compiled defaults

Key defaults:

- `MCP_GATEWAY_DB_BACKEND=sqlite`
- `MCP_GATEWAY_POSTGRES_DSN=postgresql://localhost:5432/mcp_gateway`
- `MCP_GATEWAY_DATA_DIR=.mcp_gateway`
- `MCP_GATEWAY_MAPPING_MODE=hybrid`
- `MCP_GATEWAY_ENVELOPE_JSONB_MODE=full`
- `MCP_GATEWAY_ENVELOPE_CANONICAL_ENCODING=zstd`
- `MCP_GATEWAY_MAX_FULL_MAP_BYTES=10000000`
- `MCP_GATEWAY_MAX_ITEMS=1000`
- `MCP_GATEWAY_MAX_BYTES_OUT=5000000`
- `MCP_GATEWAY_CURSOR_TTL_MINUTES=60`
- `MCP_GATEWAY_WHERE_CANONICALIZATION_MODE=raw_string`

For the full key/type/default reference, see:

- `docs/config.md`
- `src/mcp_artifact_gateway/config/settings.py`

## Docs map

- Architecture and invariants: `docs/spec_v1_9.md`
- Configuration reference: `docs/config.md`
- Traversal ordering contract: `docs/traversal_contract.md`
- Cursor format and staleness rules: `docs/cursor_contract.md`

## Development

### Unit tests

```bash
uv run pytest tests/unit -q
```

### Integration tests

```bash
docker compose up -d
uv run pytest tests/integration -v
```

Default test DSN is provided by `tests/integration/conftest.py`:
`postgresql://mcp_gateway:mcp_gateway@localhost:5432/mcp_test`

Override:

```bash
MCP_GATEWAY_TEST_POSTGRES_DSN="postgresql://user:pass@host:5432/db" uv run pytest tests/integration -v
```

### Runtime validation

```bash
PYTHONPATH=src uv run python scripts/validate.py
```

### Lint and type-check

```bash
uv run ruff check src
uv run mypy src
```

## Project layout

```text
src/mcp_artifact_gateway/
  main.py                  # CLI entrypoint
  app.py                   # app composition root
  config/                  # settings, mcpServers parser, init migration
  db/                      # pool, migrations, repositories
  fs/                      # content-addressed blob storage
  mcp/                     # upstream connections, mirroring, server wiring
  artifacts/               # envelope and artifact creation pipeline
  mapping/                 # full + partial mapping
  retrieval/               # bounded deterministic traversal responses
  cursor/                  # signed cursor payload + HMAC verification
  query/                   # JSONPath subset, select paths, where DSL
  tools/                   # gateway and artifact retrieval tool handlers
  jobs/                    # soft delete, hard delete, reconcile tasks
  obs/                     # structured logging + metrics
tests/
  unit/
  integration/
docs/
  spec_v1_9.md
  config.md
  traversal_contract.md
  cursor_contract.md
```
