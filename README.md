# MCP Artifact Gateway

Local single-tenant MCP gateway (Python) that proxies upstream MCP tools, persists every result as a durable artifact envelope, and supports bounded deterministic retrieval.

## Requirements

- Python `>=3.11` (repo currently uses Python `3.13`)
- [`uv`](https://docs.astral.sh/uv/)
- Docker (for local Postgres via `docker compose`)

## Quick Start

1. Sync dependencies:

```bash
uv sync --all-extras
```

2. Start Postgres:

```bash
docker compose up -d
```

This provisions two databases: `mcp_gateway` (app) and `mcp_test` (integration tests).

> **Existing containers:** The init script only runs on first start. If you already
> have the container running without the `mcp_test` database, recreate it:
> `docker compose down -v && docker compose up -d`

3. Create local env file:

```bash
cp .env.example .env
```

## Setting Up MCP Servers

The gateway uses the same `mcpServers` config format as Claude Desktop, Cursor, and Claude Code. The fastest way to get started is to migrate your existing config:

```bash
mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

This will:
1. Copy your MCP servers into the gateway's config (`.mcp_gateway/state/config.json`)
2. Back up your original file to `<file>.backup`
3. Rewrite the original to point only at the gateway

Preview with `--dry-run` first:

```bash
mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json --dry-run
```

To undo:

```bash
mcp-gateway init --from ~/Library/Application\ Support/Claude/claude_desktop_config.json --revert
```

### Manual configuration

You can also edit `.mcp_gateway/state/config.json` directly:

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": { "GITHUB_TOKEN": "..." }
    },
    "remote-api": {
      "url": "https://example.com/mcp",
      "headers": { "Authorization": "Bearer ..." }
    }
  }
}
```

Transport is inferred automatically: `command` means stdio, `url` means HTTP.

Gateway-specific options go under `_gateway` (ignored by other tools):

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "_gateway": {
        "semantic_salt_env_keys": ["GITHUB_ORG"],
        "strict_schema_reuse": false
      }
    }
  }
}
```

The legacy `upstreams` array format is still supported but deprecated.

## Configuration

Environment variables use the `MCP_GATEWAY_` prefix.

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_GATEWAY_POSTGRES_DSN` | `postgresql://localhost:5432/mcp_gateway` | Postgres connection string |
| `MCP_GATEWAY_DATA_DIR` | `.mcp_gateway` | Root data directory |
| `MCP_GATEWAY_ENVELOPE_JSONB_MODE` | `full` | `full \| minimal_for_large \| none` |
| `MCP_GATEWAY_ENVELOPE_CANONICAL_ENCODING` | `zstd` | `zstd \| gzip \| none` |
| `MCP_GATEWAY_MAX_JSON_PART_PARSE_BYTES` | `50000000` | Max JSON part size before byte-backed offload |
| `MCP_GATEWAY_CURSOR_TTL_MINUTES` | `60` | Cursor TTL in minutes |
| `MCP_GATEWAY_WHERE_CANONICALIZATION_MODE` | `raw_string` | `raw_string \| canonical_ast` |

See `.env.example` and `src/mcp_artifact_gateway/config/settings.py` for the full set.

## Development

### Unit tests

Unit tests run without any external dependencies:

```bash
python -m pytest tests/unit/ -q
```

### Integration tests

Integration tests require the Postgres container from docker-compose:

```bash
# Start Postgres (first time creates the mcp_test database automatically)
docker compose up -d

# Run integration tests (DSN defaults to docker-compose setup)
python -m pytest tests/integration/ -v
```

The default DSN (`postgresql://mcp_gateway:mcp_gateway@localhost:5432/mcp_test`) is set
automatically by `tests/integration/conftest.py`. Override it for custom setups:

```bash
MCP_GATEWAY_TEST_POSTGRES_DSN="postgresql://user:pass@host:5432/db" python -m pytest tests/integration/ -v
```

### Runtime validation

A standalone smoke-test script exercises the full artifact lifecycle (create,
search, get, describe, select, cache reuse, error envelopes, partial mapping,
soft/hard delete) against a fresh DB schema:

```bash
PYTHONPATH=src python scripts/validate.py
```

Exits non-zero on any invariant violation.

### Lint and type-check

```bash
uv run ruff check src
uv run mypy src
```

## Project Layout

```text
src/mcp_artifact_gateway/
  main.py                  # CLI entrypoint
  constants.py             # version/identity/constants
  config/settings.py       # typed gateway settings (pydantic-settings)
  config/mcp_servers.py    # standard mcpServers format parser
  config/init.py           # mcp-gateway init migration command
  artifacts/               # artifact creation pipeline
  cache/                   # advisory locks, stampede control
  canon/                   # RFC 8785 canonicalizer
  cursor/                  # HMAC cursors, pagination
  db/                      # psycopg3 pool, migrations, repos
  envelope/                # model, normalization, oversize handling
  fs/                      # content-addressed blob store
  jobs/                    # soft delete, hard delete, reconcile
  mapping/                 # full/partial mapping, runner, worker
  mcp/                     # upstream proxy, mirrored tools, server
  obs/                     # logging, metrics
  query/                   # jsonpath, select_paths, where DSL
  retrieval/               # traversal, response budgets
  storage/                 # payload store (compress, hash, integrity)
  tools/                   # status, search, get, select, describe, find, chain_pages
tests/
  unit/                    # ~850 unit tests (no external deps)
  integration/             # 20 end-to-end tests (requires Postgres)
docker-compose.yml         # local Postgres with test DB init
scripts/
  init-test-db.sql         # creates mcp_test database on first start
  validate.py              # done-means-done runtime validation
```
